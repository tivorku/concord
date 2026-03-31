package t2api

import (
	"encoding/json"
	"fmt"
	"os"
)

const accountsFile = "accounts.json"

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
		if acc.Refresh != "" {
			newRefresh, access, err := GetTokens(acc.Refresh)
			if err == nil {
				acc.Bearer = "Bearer " + access
				if newRefresh != "" && newRefresh != acc.Refresh {
					acc.Refresh = newRefresh
				}
				continue
			}
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
