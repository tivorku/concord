package t2api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

func GetAccountLots(bearer, number string) ([]LotInfoDetailed, error) {
	url := fmt.Sprintf("https://%s/api/subscribers/7%s/exchange/lots/created", T2Host, number)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", bearer)
	setTele2Headers(req, "2")

	resp, err := SharedClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Т2 вернул ошибку: %d", resp.StatusCode)
	}

	var res T2LotsResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, fmt.Errorf("Ошибка парсинга JSON: %v", err)
	}

	var lots []LotInfoDetailed
	for _, lot := range res.Data {
		name := "Аноним"
		if lot.Seller.Name != nil {
			name = *lot.Seller.Name
		}

		t, err := time.Parse(time.RFC3339, lot.CreationDate)
		if err != nil {
			t, _ = time.Parse("2006-01-02T15:04:05Z", lot.CreationDate)
		}

		tooOld := time.Since(t).Hours() > 30*24

		lots = append(lots, LotInfoDetailed{
			ID:       lot.ID,
			Name:     name,
			UOM:      lot.Volume.UOM,
			Volume:   int(lot.Volume.Value),
			Cost:     int(lot.Cost.Amount),
			IsActive: lot.Status == "active" && !tooOld,
		})
	}

	return lots, nil
}

func GetSegments(lots []LotInfoDetailed) []Segment {
	segMap := make(map[string]Segment)

	for _, lot := range lots {
		if !lot.IsActive {
			continue
		}
		key := fmt.Sprintf("%s-%d-%d", lot.UOM, lot.Volume, lot.Cost)
		seg := segMap[key]
		seg.UOM = lot.UOM
		seg.Volume = lot.Volume
		seg.Cost = lot.Cost
		seg.Count++
		segMap[key] = seg
	}

	var segments []Segment
	for _, seg := range segMap {
		segments = append(segments, seg)
	}
	return segments
}

func FilterLotsBySegment(lots []LotInfoDetailed, seg Segment) []string {
	var lotIDs []string
	for _, lot := range lots {
		if lot.IsActive && lot.UOM == seg.UOM && lot.Volume == seg.Volume && lot.Cost == seg.Cost {
			lotIDs = append(lotIDs, lot.ID)
		}
	}
	return lotIDs
}

func GetTop4IDs(volume, cost int) ([]LotInfo, error) {
	url := fmt.Sprintf("https://%s/api/exchange/lots?trafficType=data&volume=%d&cost=%d&limit=4", T2Host, volume, cost)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "application/json")
	setTele2Headers(req, "")

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

func GetTop4IDsAsync(volume, cost int, callback func([]LotInfo, error)) {
	go func() {
		lots, err := GetTop4IDs(volume, cost)
		callback(lots, err)
	}()
}

func Rocket(client *http.Client, bearer, number, lotID string) error {
	url := fmt.Sprintf("https://%s/api/subscribers/7%s/exchange/lots/premium", T2Host, number)
	jsonData := []byte(fmt.Sprintf(`{"lotId":"%s"}`, lotID))
	req, _ := http.NewRequest("PUT", url, bytes.NewBuffer(jsonData))
	req.Header.Set("Authorization", bearer)
	req.Header.Set("Content-Type", "application/json")
	setTele2Headers(req, "2")

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
