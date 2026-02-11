package t2api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type LotInfo struct {
	ID    string
	IsBot bool
}

type T2ErrorResponse struct {
	Meta struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	} `json:"meta"`
}

// GetTop4IDs делает анонимный запрос и детектирует ботов по флагу "my"
func GetTop4IDs(volume, cost int) ([]LotInfo, error) {
	url := fmt.Sprintf("https://api.t2.ru/api/exchange/lots?trafficType=data&volume=%d&cost=%d&limit=4", volume, cost)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Tele2-User-Agent", "mytele2-app/6.19.0")
	req.Header.Set("User-Agent", "okhttp/4.12.0")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var res struct {
		Data []struct {
			ID string `json:"id"`
			My bool   `json:"my"`
		} `json:"data"`
	}
	json.NewDecoder(resp.Body).Decode(&res)

	var results []LotInfo
	for _, lot := range res.Data {
		results = append(results, LotInfo{ID: lot.ID, IsBot: lot.My})
	}
	return results, nil
}

// Rocket выполняет поднятие лота. Поддерживает прокси-клиент.
func Rocket(client *http.Client, bearer, number, lotID string) error {
	url := fmt.Sprintf("https://api.t2.ru/api/subscribers/7%s/exchange/lots/premium", number)
	jsonData := []byte(fmt.Sprintf(`{"lotId":"%s"}`, lotID))
	
	req, _ := http.NewRequest("PUT", url, bytes.NewBuffer(jsonData))
	req.Header.Set("Authorization", bearer)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Tele2-User-Agent", "mytele2-app/6.19.0")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	}
	return fmt.Errorf("ошибка Т2: %d", resp.StatusCode)
}