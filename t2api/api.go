package t2api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type LotInfo struct {
	ID    string
	IsBot bool
}

type T2LotsResponse struct {
	Data []struct {
		ID     string `json:"id"`
		Status string `json:"status"`
		Volume struct {
			Value float64 `json:"value"`
			UOM   string  `json:"uom"`
		} `json:"volume"`
		Cost struct {
			Amount float64 `json:"amount"`
		} `json:"cost"`
		Seller struct {
			Name   *string  `json:"name"`
			Emojis []string `json:"emojis"`
		} `json:"seller"`
		CreationDate string `json:"creationDate"`
	} `json:"data"`
}

func ShowAndSelectLot(bearer, number string) (string, int, int, error) {
	url := fmt.Sprintf("https://%s/api/subscribers/7%s/exchange/lots/created", T2Host, number)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", bearer)
	req.Header.Set("Tele2-User-Agent", AppVersion)
	req.Header.Set("X-API-Version", "2")
	req.Header.Set("User-Agent", OkHttpVersion)

	resp, err := SharedClient.Do(req)
	if err != nil {
		return "", 0, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", 0, 0, fmt.Errorf("Т2 вернул ошибку: %d", resp.StatusCode)
	}

	var res T2LotsResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return "", 0, 0, fmt.Errorf("Ошибка парсинга JSON: %v", err)
	}

	fmt.Printf("[DEBUG] Всего лотов получено: %d\n", len(res.Data))

	type activeLot struct {
		id     string
		vol    int
		amount int
	}
	var selectable []activeLot
	count := 0

	for i := len(res.Data) - 1; i >= 0; i-- {
		lot := res.Data[i]

		if lot.Status != "active" {
			continue
		}

		t, err := time.Parse(time.RFC3339, lot.CreationDate)
		if err != nil {
			t, _ = time.Parse("2006-01-02T15:04:05Z", lot.CreationDate)
		}

		if time.Since(t).Hours() > 30*24 {
			continue
		}

		count++
		name := "Аноним"
		if lot.Seller.Name != nil {
			name = *lot.Seller.Name
		}

		v := int(lot.Volume.Value)
		c := int(lot.Cost.Amount)

		fmt.Printf("%d. %-10s | %d %s | %d руб\n", count, name, v, lot.Volume.UOM, c)
		selectable = append(selectable, activeLot{lot.ID, v, c})
	}

	if len(selectable) == 0 {
		return "", 0, 0, fmt.Errorf("Активные лоты не найдены. Создайте лот на Маркете вручную.")
	}

	fmt.Print("\nВыберите номер лота: ")
	var choice int
	_, err = fmt.Scanf("%d\n", &choice)
	if err != nil {
		return "", 0, 0, fmt.Errorf("Ошибка ввода: %v", err)
	}

	if choice < 1 || choice > len(selectable) {
		return "", 0, 0, fmt.Errorf("Неверный номер.")
	}

	target := selectable[choice-1]
	fmt.Printf("[MDN] Выбран лот: %s (%d ГБ)\n", target.id, target.vol)
	return target.id, target.vol, target.amount, nil
}

func GetTop4IDs(volume, cost int) ([]LotInfo, error) {
	url := fmt.Sprintf("https://%s/api/exchange/lots?trafficType=data&volume=%d&cost=%d&limit=4", T2Host, volume, cost)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	// устанавливаем заголовки ТОЧНО как в рабочем скрипте
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Tele2-User-Agent", AppVersion)
	req.Header.Set("User-Agent", OkHttpVersion)

	resp, err := SharedClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Ошибка запроса: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Ошибка чтения тела: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		snippet := string(body)
		if len(snippet) > 200 {
			snippet = snippet[:200]
		}
		return nil, fmt.Errorf("Сервер вернул %d. Тело: %s", resp.StatusCode, snippet)
	}

	var res struct {
		Data []struct {
			ID string `json:"id"`
			My bool   `json:"my"`
		} `json:"data"`
	}

	if err := json.Unmarshal(body, &res); err != nil {
		return nil, fmt.Errorf("Ошибка декодирования JSON: %v. Тело: %s", err, string(body[:50]))
	}

	if len(res.Data) == 0 {
		return nil, fmt.Errorf("Лоты не найдены.")
	}

	var results []LotInfo
	for _, lot := range res.Data {
		results = append(results, LotInfo{ID: lot.ID, IsBot: lot.My})
	}
	return results, nil
}

func Rocket(client *http.Client, bearer, number, lotID string) error {
	url := fmt.Sprintf("https://%s/api/subscribers/7%s/exchange/lots/premium", T2Host, number)
	jsonData := []byte(fmt.Sprintf(`{"lotId":"%s"}`, lotID))
	req, _ := http.NewRequest("PUT", url, bytes.NewBuffer(jsonData))
	req.Header.Set("Authorization", bearer)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Tele2-User-Agent", AppVersion)
	req.Header.Set("User-Agent", OkHttpVersion)
	req.Header.Set("X-API-Version", "2")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	}
	return fmt.Errorf("Ошибка Т2: %v", resp.Status)
}
