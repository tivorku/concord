package t2api

import (
    "fmt"
    "net/http"
    "encoding/json"
    "time"
    "bytes"
    "strings"
)
func GetTop4IDs(volume, cost int) ([]string, error) {
	lot_url := fmt.Sprintf("https://api.t2.ru/api/exchange/lots?trafficType=data&volume=%d&cost=%d&limit=4", int(volume), int(cost))
	request, _ := http.NewRequest("GET", lot_url, nil)
	request.Header.Set("Tele2-User-Agent", "mytele2-app/6.19.0")
	request.Header.Set("User-Agent", "okhttp/4.12.0")
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
	    fmt.Println(response.Status)
	}
	var res struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(response.Body).Decode(&res); err != nil {
		return nil, err
	}
	response.Body.Close()
    var ids []string
	for _, lot := range res.Data {
		ids = append(ids, lot.ID)
	}

	return ids, nil
}
type T2ErrorResponse struct {
	Meta struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	} `json:"meta"`
}

func Rocket(bearer string, number string, lotID string) error {
	url := fmt.Sprintf("https://api.t2.ru/api/subscribers/7%s/exchange/lots/premium", number)
	
	jsonData := []byte(fmt.Sprintf(`{"lotId":"%s"}`, lotID))
	request, err := http.NewRequest("PUT", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}

	request.Header.Set("Accept", "application/json, text/plain, */*")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", bearer)
	request.Header.Set("Tele2-User-Agent", "mytele2-app/6.18.0")
	request.Header.Set("User-Agent", "okhttp/4.12.0")
	request.Header.Set("X-API-Version", "2")
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("Ошибка сети: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode == http.StatusOK {
		fmt.Println("Лот поднят в топ!")
		time.Sleep(3 * time.Second)
		return nil
	}

	var errRes T2ErrorResponse
	if err := json.NewDecoder(response.Body).Decode(&errRes); err != nil {
		return err
	}

	msg := errRes.Meta.Message
	status := errRes.Meta.Status

	switch {
    	case response.StatusCode == 400 && strings.Contains(msg, "is not in ACTIVE"):
    		return fmt.Errorf("Лот неактивен")
    	case response.StatusCode == 400 && strings.Contains(msg, "has already premium state"):
    		return fmt.Errorf("Этот лот уже в топе")
    	case status == "bp_err_noReserve" || msg == "bp_err_noReserve":
    		return fmt.Errorf("Лота не существует")
    	default:
    		return fmt.Errorf("ошибка Т2 (%d): %s", response.StatusCode, msg)
	}
}