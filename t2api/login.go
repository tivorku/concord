package t2api
import (
    "net/http"
    "encoding/json"
    "fmt"
    "os"
    "strings"
    "bytes"
    "io"
    "net/url"
)
type Data struct {
	Access  string `json:"access_token"`
	Refresh string `json:"refresh_token"`
}
var (
    
	number, sms_code string
	data Data
	bearer_url = fmt.Sprintf("https://sso.t2.ru/auth/realms/tele2-b2c/protocol/openid-connect/token")
)
func RequestSms(number string) string {
	sms_url := fmt.Sprintf("https://%s/api/validation/number/7%s", T2Host, number)
	payload := map[string]string{"sender": "Tele2"}
	jsondata, _ := json.Marshal(payload)
	request, err := http.NewRequest("POST", sms_url, bytes.NewBuffer(jsondata))
	if err != nil {
		fmt.Println(err)
	}
	request.Header.Set("Tele2-User-Agent", AppVersion)
	request.Header.Set("User-Agent", OkHttpVersion)
	request.Header.Set("Content-Type", "application/json; charset=UTF-8")
	request.Header.Set("X-API-Version", "1")
	request.Header.Set("Content-Length", "18")
	response, err := SharedClient.Do(request)
	if err != nil {
		fmt.Println(err)
	}
	if response.StatusCode != http.StatusOK {
		fmt.Println(response.Status)
	}
	defer response.Body.Close()
	fmt.Print("Введите SMS-код: ")
	fmt.Scanln(&sms_code)
	return sms_code
}
func RequestBearer(number, sms_code string) (string, string) {
	rawdata := url.Values{}
	rawdata.Set("client_id", "android-app")
	rawdata.Set("grant_type", "password")
	rawdata.Set("username", "7" + string(number))
	rawdata.Set("password", sms_code)
	rawdata.Set("password_type", "sms_code")
	Body := strings.NewReader(rawdata.Encode())
	request, _ := http.NewRequest("POST", bearer_url, Body)
	request.Header.Set("X-API-Version", "1")
	request.Header.Set("Tele2-User-Agent", AppVersion)
	request.Header.Set("User-Agent", OkHttpVersion)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := SharedClient.Do(request)
	if err != nil {
		fmt.Println(err)
	}
	if response.StatusCode != http.StatusOK {
		fmt.Println(response.Status)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	json.Unmarshal(body, &data)
	os.WriteFile("refresh.txt", []byte(data.Refresh), 0644)
	return data.Refresh, data.Access
}
func GetTokens(refresh string) (string, string, error) {
	rawdata := url.Values{}
	rawdata.Set("client_id", "android-app")
	rawdata.Set("grant_type", "refresh_token")
	rawdata.Set("refresh_token", refresh)
	Body := strings.NewReader(rawdata.Encode())
	request, _ := http.NewRequest("POST", bearer_url, Body)
	request.Header.Set("X-API-Version", "1")
	request.Header.Set("Tele2-User-Agent", AppVersion)
	request.Header.Set("User-Agent", OkHttpVersion)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := SharedClient.Do(request)
	if err != nil {
		fmt.Println(err)
		return "", "", err
	}
	if response.StatusCode != http.StatusOK {
	    fmt.Println(response.Status)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		fmt.Println(response.Status)
	}
	body, _ := io.ReadAll(response.Body)
	json.Unmarshal(body, &data)
	os.WriteFile("refresh.txt", []byte(data.Refresh), 0644)
	return data.Refresh, data.Access, nil
}
