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

const configFile = "creds.json"

func loadConfig() Config {
	var cfg Config
	data, err := os.ReadFile(configFile)
	if err == nil {
		json.Unmarshal(data, &cfg)
	}
	return cfg
}

// saveConfig сохраняет данные структуры в JSON-файл
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