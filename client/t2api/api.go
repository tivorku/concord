package t2api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type LotData struct {
	ID           string
	Status       string
	VolumeValue  float64
	VolumeUOM    string
	CostAmount   float64
	PremiumOps   int64
	CreationDate time.Time
}

func fetchAccountLots(bearer, number string) ([]LotData, error) {
	url := fmt.Sprintf("https://%s/api/subscribers/7%s/exchange/lots/created", T2Host, number)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
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

	var res struct {
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
			PremiumOps int64 `json:"premiumOps"`
			CreationDate string `json:"creationDate"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, fmt.Errorf("Ошибка парсинга JSON: %v", err)
	}

	lots := make([]LotData, 0, len(res.Data))
	for _, lot := range res.Data {
		t, err := time.Parse(time.RFC3339, lot.CreationDate)
		if err != nil {
			t, _ = time.Parse("2006-01-02T15:04:05Z", lot.CreationDate)
		}
		lots = append(lots, LotData{
			ID:           lot.ID,
			Status:       lot.Status,
			VolumeValue:  lot.Volume.Value,
			VolumeUOM:    lot.Volume.UOM,
			CostAmount:   lot.Cost.Amount,
			PremiumOps:   lot.PremiumOps,
			CreationDate: t,
		})
	}

	return lots, nil
}
func GetActiveOps(bearer, number string, lotID string) (int64, error) {
    lots, err := fetchAccountLots(bearer, number)
    if err != nil {
        return 0, err
    }
    for _, lot := range lots {
        if lot.ID == lotID {
            return lot.PremiumOps, nil
        }
    }
    return 0, nil
}

func isEligibleLot(lot LotData) bool {
	if lot.Status != "active" || lot.PremiumOps == 0 {
		return false
	}
	if time.Since(lot.CreationDate).Hours() > 30*24 {
		return false
	}
	return true
}

func GetSegments(bearer, number string) ([]Segment, []LotData, error) {
	lots, err := fetchAccountLots(bearer, number)
	if err != nil {
		return nil, nil, err
	}

	segMap := make(map[string]Segment)
	for _, lot := range lots {
		if !isEligibleLot(lot) {
			continue
		}

		key := fmt.Sprintf("%s-%d-%d", lot.VolumeUOM, int(lot.VolumeValue), int(lot.CostAmount))
		seg := segMap[key]
		seg.UOM = lot.VolumeUOM
		seg.Volume = int(lot.VolumeValue)
		seg.Cost = int(lot.CostAmount)
		seg.Count++
		segMap[key] = seg
	}

	segments := make([]Segment, 0, len(segMap))
	for _, seg := range segMap {
		segments = append(segments, seg)
	}

	return segments, lots, nil
}

func FilterLotsBySegment(lots []LotData, seg Segment) []string {
	var lotIDs []string
	for _, lot := range lots {
		if !isEligibleLot(lot) {
			continue
		}
		if lot.VolumeUOM == seg.UOM && int(lot.VolumeValue) == seg.Volume && int(lot.CostAmount) == seg.Cost {
			lotIDs = append(lotIDs, lot.ID)
		}
	}
	return lotIDs
}

type GetTop4Result struct {
	Lots []LotInfo
	Err  error
}

func GetTop4IDs(trafficType string, volume, cost int) ([]LotInfo, error) {
	url := fmt.Sprintf("https://%s/api/exchange/lots?trafficType=%s&volume=%d&cost=%d&limit=4", T2Host, trafficType, volume, cost)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
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

	preview := body
	if len(preview) > 50 {
		preview = preview[:50]
	}
	if err := json.Unmarshal(body, &res); err != nil {
		return nil, fmt.Errorf("Ошибка декодирования JSON: %v. Тело: %s", err, string(preview))
	}

	if len(res.Data) == 0 {
		return nil, fmt.Errorf("Лоты не найдены.")
	}

	results := make([]LotInfo, 0, len(res.Data))
	for _, lot := range res.Data {
		results = append(results, LotInfo{ID: lot.ID, IsBot: lot.My})
	}
	return results, nil
}

func GetTop4IDsAsync(trafficType string, volume, cost int) <-chan GetTop4Result {
	ch := make(chan GetTop4Result, 1)
	go func() {
		lots, err := GetTop4IDs(trafficType, volume, cost)
		ch <- GetTop4Result{Lots: lots, Err: err}
	}()
	return ch
}

func Rocket(client *http.Client, bearer, number, lotID string) error {
	url := fmt.Sprintf("https://%s/api/subscribers/7%s/exchange/lots/premium", T2Host, number)
	jsonData, err := json.Marshal(struct {
		LotID string `json:"lotId"`
	}{LotID: lotID})
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}
	req, err := http.NewRequest("PUT", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
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
