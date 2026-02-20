package t2api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	//"strings"
	//"net/http/httputil" // Добавь 
	"time"
	"io"
	//"net"
	//"crypto/tls"
	
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
var emojiMap = map[string]string{
	"devil":  "😈",
	"cool":   "😎",
	"cat":    "😺",
	"zipped": "🤐",
	"scream": "😱",
	"rich":   "🤑",
	"tongue": "😛",
	"bomb":   "💣",
}
func ShowAndSelectLot(bearer, number string) (string, int, int, error) {
	url := fmt.Sprintf("https://yar.t2.ru/api/subscribers/7%s/exchange/lots/created", number)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", bearer)
	req.Header.Set("Tele2-User-Agent", "mytele2-app/6.19.0")
	req.Header.Set("X-API-Version", "2")
	req.Header.Set("User-Agent", "okhttp/4.12.0")

	resp, err := sharedClient.Do(req)
	if err != nil {
		return "", 0, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", 0, 0, fmt.Errorf("Т2 вернул ошибку: %d", resp.StatusCode)
	}

	var res T2LotsResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return "", 0, 0, fmt.Errorf("ошибка парсинга JSON: %v", err)
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

		// Лог для отладки каждого лота
		// fmt.Printf("[DEBUG] Лот %s: статус=%s, дата=%s\n", lot.ID, lot.Status, lot.CreationDate)

		if lot.Status != "active" {
			continue
		}

		// Проверка даты (Т2 иногда шлет даты в странных форматах)
		t, err := time.Parse(time.RFC3339, lot.CreationDate)
		if err != nil {
			// Если RFC3339 не подошел, пробуем упрощенный формат
			t, _ = time.Parse("2006-01-02T15:04:05Z", lot.CreationDate)
		}

		if !t.IsZero() && time.Since(t).Hours() > 30*24 {
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
		return "", 0, 0, fmt.Errorf("активные лоты не найдены. Создайте лот на Маркете вручную")
	}

	// На Android fmt.Scanln может "проглатывать" ввод, если в буфере остался перевод строки.
	// Используем более надежный способ чтения для Termux:
	fmt.Print("\nВыберите номер лота: ")
	var choice int
	_, err = fmt.Scanf("%d\n", &choice) 
	if err != nil {
		return "", 0, 0, fmt.Errorf("ошибка ввода: %v", err)
	}

	if choice < 1 || choice > len(selectable) {
		return "", 0, 0, fmt.Errorf("неверный номер")
	}

	target := selectable[choice-1]
	fmt.Printf("[MDN] Выбран лот: %s (%d ГБ)\n", target.id, target.vol)
	return target.id, target.vol, target.amount, nil
}


// GetTop4IDs — итоговая функция
func GetTop4IDs(volume, cost int) ([]LotInfo, error) {
	url := fmt.Sprintf("https://yar.t2.ru/api/exchange/lots?trafficType=data&volume=%d&cost=%d&limit=4", volume, cost)
	
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	// Устанавливаем заголовки ТОЧНО как в рабочем скрипте
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Tele2-User-Agent", "mytele2-app/6.19.0")
	req.Header.Set("User-Agent", "okhttp/4.12.0")

	resp, err := sharedClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ошибка запроса: %v", err)
	}
	defer resp.Body.Close()

	// Читаем тело ответа для анализа
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("ошибка чтения тела: %v", err)
	}

	// Если сервер всё равно вернул не 200, выводим что именно не так
	if resp.StatusCode != http.StatusOK {
		snippet := string(body)
		if len(snippet) > 200 {
			snippet = snippet[:200]
		}
		return nil, fmt.Errorf("сервер вернул %d. Тело: %s", resp.StatusCode, snippet)
	}

	// Декодируем JSON
	var res struct {
		Data []struct {
			ID string `json:"id"`
			My bool   `json:"my"`
		} `json:"data"`
	}

	if err := json.Unmarshal(body, &res); err != nil {
		return nil, fmt.Errorf("ошибка декодирования JSON: %v. Тело: %s", err, string(body[:50]))
	}

	if len(res.Data) == 0 {
		return nil, fmt.Errorf("лоты не найдены")
	}

	var results []LotInfo
	for _, lot := range res.Data {
		results = append(results, LotInfo{ID: lot.ID, IsBot: lot.My})
	}

	return results, nil
}

// Rocket выполняет поднятие лота. Поддерживает прокси-клиент.
func Rocket(client *http.Client, bearer, number, lotID string) error {
	url := fmt.Sprintf("https://yar.t2.ru/api/subscribers/7%s/exchange/lots/premium", number)
	jsonData := []byte(fmt.Sprintf(`{"lotId":"%s"}`, lotID))
	req, _ := http.NewRequest("PUT", url, bytes.NewBuffer(jsonData))
	req.Header.Set("Authorization", bearer)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Tele2-User-Agent", "mytele2-app/6.19.0")
	req.Header.Set("User-Agent", "okhttp/4.12.0")
	req.Header.Set("X-API-Version", "2")
    
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	}
	return fmt.Errorf("ошибка Т2: %v", resp.Status)
}