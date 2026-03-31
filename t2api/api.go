package t2api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
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
type LotInfoDetailed struct {
	ID       string
	Name     string
	UOM      string
	Volume   int
	Cost     int
	IsActive bool
}

type Segment struct {
	UOM    string
	Volume int
	Cost   int
	Count  int
}

func GetAccountLots(bearer, number string) ([]LotInfoDetailed, error) {
	url := fmt.Sprintf("https://%s/api/subscribers/7%s/exchange/lots/created", T2Host, number)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", bearer)
	req.Header.Set("Tele2-User-Agent", AppVersion)
	req.Header.Set("X-API-Version", "2")
	req.Header.Set("User-Agent", OkHttpVersion)

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

func UOMDisplayName(uom string) string {
	switch uom {
	case "data":
		return "Data"
	case "voice":
		return "Voice"
	case "sms":
		return "SMS"
	default:
		return uom
	}
}

func SelectSegment(segments []Segment) (Segment, error) {
	if len(segments) == 0 {
		return Segment{}, fmt.Errorf("Нет доступных сегментов")
	}

	segmentsByUOM := make(map[string][]Segment)
	for _, seg := range segments {
		segmentsByUOM[seg.UOM] = append(segmentsByUOM[seg.UOM], seg)
	}

	fmt.Println("\n=== Выберите тип трафика ===")
	var uomList []string
	i := 1
	for uom := range segmentsByUOM {
		count := 0
		for _, s := range segmentsByUOM[uom] {
			count += s.Count
		}
		fmt.Printf("%d. %s  [%d лотов]\n", i, UOMDisplayName(uom), count)
		uomList = append(uomList, uom)
		i++
	}

	fmt.Print("> ")
	var choice int
	fmt.Scanln(&choice)
	if choice < 1 || choice > len(uomList) {
		return Segment{}, fmt.Errorf("Неверный выбор типа трафика")
	}
	selectedUOM := uomList[choice-1]

	selectedSegments := segmentsByUOM[selectedUOM]
	fmt.Printf("\n=== %s сегменты ===\n", UOMDisplayName(selectedUOM))
	for j, seg := range selectedSegments {
		fmt.Printf("%d. %d %s за %d руб  (%d лотов)\n", j+1, seg.Volume, seg.UOM, seg.Cost, seg.Count)
	}

	fmt.Print("> ")
	fmt.Scanln(&choice)
	if choice < 1 || choice > len(selectedSegments) {
		return Segment{}, fmt.Errorf("Неверный выбор сегмента")
	}

	selectedSeg := selectedSegments[choice-1]
	fmt.Printf("[MDN] Выбран сегмент: %d %s за %d руб (%d лотов)\n", selectedSeg.Volume, selectedSeg.UOM, selectedSeg.Cost, selectedSeg.Count)

	return selectedSeg, nil
}

func ShowAndSelectLot(bearer, number string) (string, int, int, error) {
	lotIDs, volume, value, err := SelectLots(bearer, number)
	if err != nil {
		return "", 0, 0, err
	}
	return lotIDs[0], volume, value, nil
}

func SelectLots(bearer, number string) ([]string, int, int, error) {
	url := fmt.Sprintf("https://%s/api/subscribers/7%s/exchange/lots/created", T2Host, number)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", bearer)
	req.Header.Set("Tele2-User-Agent", AppVersion)
	req.Header.Set("X-API-Version", "2")
	req.Header.Set("User-Agent", OkHttpVersion)

	resp, err := SharedClient.Do(req)
	if err != nil {
		return nil, 0, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, 0, 0, fmt.Errorf("Т2 вернул ошибку: %d", resp.StatusCode)
	}

	var res T2LotsResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, 0, 0, fmt.Errorf("Ошибка парсинга JSON: %v", err)
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
		return nil, 0, 0, fmt.Errorf("Активные лоты не найдены. Создайте лот на Маркете вручную.")
	}

	if len(selectable) > 5 {
		selectable = selectable[:5]
		fmt.Printf("[MDN] Ограничено до 5 лотов\n")
	}

	fmt.Print("\nВыберите номера лотов (через пробел, например: 1 2 3): ")
	var choices string
	fmt.Scanln(&choices)

	var selected []int
	for _, c := range strings.Fields(choices) {
		var num int
		fmt.Sscanf(c, "%d", &num)
		selected = append(selected, num)
	}

	if len(selected) == 0 {
		return nil, 0, 0, fmt.Errorf("Не выбран ни один лот.")
	}

	var lotIDs []string
	var volume, value int

	for _, choice := range selected {
		if choice < 1 || choice > len(selectable) {
			continue
		}
		target := selectable[choice-1]
		lotIDs = append(lotIDs, target.id)
		volume = target.vol
		value = target.amount
	}

	if len(lotIDs) == 0 {
		return nil, 0, 0, fmt.Errorf("Не выбран ни один лот.")
	}

	fmt.Printf("[MDN] Выбрано лотов: %d (%d ГБ, %d руб)\n", len(lotIDs), volume, value)
	return lotIDs, volume, value, nil
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
