package t2api

import (
	"encoding/json"
	"fmt"
	"os"
)

type Config struct {
	Number  string `json:"number"`
	Refresh string `json:"refresh"`
}

type AccountConfig struct {
	ID      string `json:"id"`
	Number  string `json:"number"`
	Refresh string `json:"refresh"`
}

type AccountsFile struct {
	Accounts []AccountConfig `json:"accounts"`
}

type Account struct {
	ID      string
	Number  string
	Refresh string
	Bearer  string
}

const accountsFile = "accounts.json"
const configFile = "creds.json"

func LoadAccounts() ([]*Account, error) {
	data, err := os.ReadFile(accountsFile)
	if err != nil {
		return nil, err
	}

	var af AccountsFile
	if err := json.Unmarshal(data, &af); err != nil {
		return nil, err
	}

	var accounts []*Account
	for _, ac := range af.Accounts {
		accounts = append(accounts, &Account{
			ID:      ac.ID,
			Number:  ac.Number,
			Refresh: ac.Refresh,
		})
	}

	return accounts, nil
}

func SaveAccounts(accounts []*Account) error {
	var af AccountsFile
	for _, a := range accounts {
		af.Accounts = append(af.Accounts, AccountConfig{
			ID:      a.ID,
			Number:  a.Number,
			Refresh: a.Refresh,
		})
	}

	data, err := json.MarshalIndent(af, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(accountsFile, data, 0644)
}

func MultiLogin() ([]*Account, error) {
	accounts, err := LoadAccounts()
	if err != nil {
		return nil, fmt.Errorf("accounts.json не найден: %w", err)
	}

	for _, acc := range accounts {
		Check()

		if acc.Refresh != "" {
			newRefresh, access, err := GetTokens(acc.Refresh)
			if err == nil {
				acc.Bearer = "Bearer " + access
				if newRefresh != "" && newRefresh != acc.Refresh {
					acc.Refresh = newRefresh
				}
				continue
			}
			fmt.Printf("[Login] Токен для %s истёк, нужен SMS\n", acc.Number)
		}

		smsCode := RequestSms(acc.Number)
		newRefresh, access := RequestBearer(acc.Number, smsCode)
		acc.Bearer = "Bearer " + access
		acc.Refresh = newRefresh
	}

	if err := SaveAccounts(accounts); err != nil {
		fmt.Printf("[Login] Не удалось сохранить: %v\n", err)
	}

	return accounts, nil
}

func loadConfig() Config {
	var cfg Config
	data, err := os.ReadFile(configFile)
	if err == nil {
		json.Unmarshal(data, &cfg)
	}
	return cfg
}

func saveConfig(cfg Config) {
	data, _ := json.MarshalIndent(cfg, "", "  ")
	os.WriteFile(configFile, data, 0644)
}

func Login() (string, string) {
	var access string
	Check()

	cfg := loadConfig()

	if cfg.Number != "" {
		TryAnotherNumber(&cfg)
	}

	if cfg.Number == "" {
		cfg.Number = GetNumber(&cfg)
	}

	var err error
	if cfg.Refresh != "" {
		var newRefresh string
		newRefresh, access, err = GetTokens(cfg.Refresh)
		if err != nil {
			os.Exit(5)
		}

		if newRefresh != "" && newRefresh != cfg.Refresh {
			cfg.Refresh = newRefresh
			saveConfig(cfg)
		}
	}

	if cfg.Refresh == "" {
		smsCode := RequestSms(cfg.Number)

		newRefresh, acc := RequestBearer(cfg.Number, smsCode)
		access = acc
		cfg.Refresh = newRefresh
		saveConfig(cfg)
	}

	bearer := "Bearer " + access
	return bearer, cfg.Number
}

func TryAnotherNumber(cfg *Config) {
	var resetNumber string
	fmt.Print("Ввести другой номер телефона? (y/N): ")
	fmt.Scanln(&resetNumber)

	if yes_reset[resetNumber] {
		cfg.Number = ""
		cfg.Refresh = ""
		saveConfig(*cfg)
	}
}

func GetNumber(cfg *Config) string {
	var rememberNumber string
	var number string

	cfg.Refresh = ""
	saveConfig(*cfg)
	for number == "" || len(number) != 10 {
		fmt.Print("Введите номер телефона: +7")
		fmt.Scanln(&number)
		if number == "" || len(number) != 10 {
			fmt.Println("Неверно введен номер телефона!")
		}
	}
	fmt.Print("Запомнить номер телефона? (Y/n): ")
	fmt.Scanln(&rememberNumber)

	if yes[rememberNumber] {
		cfg.Number = number
		saveConfig(*cfg)
	}
	return number
}

func Check() {
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		emptyConfig := Config{Number: "", Refresh: ""}
		saveConfig(emptyConfig)
	}
}
