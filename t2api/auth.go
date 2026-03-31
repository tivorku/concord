package t2api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type TokenData struct {
	Access  string `json:"access_token"`
	Refresh string `json:"refresh_token"`
}

var bearerURL = fmt.Sprintf("https://sso.t2.ru/auth/realms/tele2-b2c/protocol/openid-connect/token")

func RequestSms(number string) string {
	smsURL := fmt.Sprintf("https://%s/api/validation/number/7%s", T2Host, number)
	payload := map[string]string{"sender": "Tele2"}
	jsonData, err := json.Marshal(payload)
	if err != nil {
		fmt.Println(err)
	}

	request, err := http.NewRequest("POST", smsURL, bytes.NewBuffer(jsonData))
	if err != nil {
		fmt.Println(err)
	}
	request.Header.Set("Tele2-User-Agent", AppVersion)
	request.Header.Set("User-Agent", OkHttpVersion)
	request.Header.Set("Content-Type", "application/json; charset=UTF-8")
	request.Header.Set("X-API-Version", "1")

	response, err := SharedClient.Do(request)
	if err != nil {
		fmt.Println(err)
	}
	if response.StatusCode != http.StatusOK {
		fmt.Println(response.Status)
	}
	defer response.Body.Close()

	var smsCode string
	fmt.Print("Введите SMS-код: ")
	fmt.Scanln(&smsCode)
	return smsCode
}

func RequestBearer(number, smsCode string) (string, string) {
	rawData := url.Values{}
	rawData.Set("client_id", "android-app")
	rawData.Set("grant_type", "password")
	rawData.Set("username", "7"+number)
	rawData.Set("password", smsCode)
	rawData.Set("password_type", "sms_code")

	body := strings.NewReader(rawData.Encode())
	request, _ := http.NewRequest("POST", bearerURL, body)
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

	respBody, _ := io.ReadAll(response.Body)
	var data TokenData
	json.Unmarshal(respBody, &data)
	return data.Refresh, data.Access
}

func GetTokens(refresh string) (string, string, error) {
	rawData := url.Values{}
	rawData.Set("client_id", "android-app")
	rawData.Set("grant_type", "refresh_token")
	rawData.Set("refresh_token", refresh)

	body := strings.NewReader(rawData.Encode())
	request, _ := http.NewRequest("POST", bearerURL, body)
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

	respBody, _ := io.ReadAll(response.Body)
	var data TokenData
	json.Unmarshal(respBody, &data)
	return data.Refresh, data.Access, nil
}
