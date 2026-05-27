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
	smsURL := fmt.Sprintf("https://api.t2.ru/api/validation/number/7%s", number)
	payload := map[string]string{"sender": "t2.ru"}
	jsonData, err := json.Marshal(payload)
	if err != nil {
		fmt.Println(err)
		return ""
	}

	request, err := http.NewRequest("POST", smsURL, bytes.NewBuffer(jsonData))
	if err != nil {
		fmt.Println(err)
		return ""
	}
	request.Header.Set("Content-Type", "application/json; charset=UTF-8")
	request.Header.Set("Content-Length", "18")
	setTele2Headers(request, "1")

	response, err := SharedClient.Do(request)
	if err != nil {
		fmt.Println(err)
		return ""
	}
	if response == nil {
		return ""
	}
	defer response.Body.Close()
    
    body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK {
		fmt.Println(response.Status)
		fmt.Println(string(body))
	}

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
	request, err := http.NewRequest("POST", bearerURL, body)
	if err != nil {
		fmt.Println(err)
		return "", ""
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	setTele2Headers(request, "1")

	response, err := SharedClient.Do(request)
	if err != nil {
		fmt.Println(err)
		return "", ""
	}
	if response == nil {
		return "", ""
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		fmt.Println(response.Status)
		return "", ""
	}

	respBody, err := io.ReadAll(response.Body)
	if err != nil {
		fmt.Println(err)
		return "", ""
	}
	var data TokenData
	if err := json.Unmarshal(respBody, &data); err != nil {
		fmt.Println(err)
		return "", ""
	}
	if data.Access == "" {
		fmt.Println("Пустой access_token в ответе")
		return "", ""
	}
	return data.Refresh, data.Access
}

func GetTokens(refresh string) (string, string, error) {
	rawData := url.Values{}
	rawData.Set("client_id", "android-app")
	rawData.Set("grant_type", "refresh_token")
	rawData.Set("refresh_token", refresh)

	body := strings.NewReader(rawData.Encode())
	request, err := http.NewRequest("POST", bearerURL, body)
	if err != nil {
		return "", "", fmt.Errorf("failed to create request: %w", err)
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	setTele2Headers(request, "1")

	response, err := SharedClient.Do(request)
	if err != nil {
		return "", "", fmt.Errorf("request failed: %w", err)
	}
	if response == nil {
		return "", "", fmt.Errorf("nil response")
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("token refresh failed: %s", response.Status)
	}

	respBody, err := io.ReadAll(response.Body)
	if err != nil {
		return "", "", fmt.Errorf("failed to read response: %w", err)
	}
	var data TokenData
	if err := json.Unmarshal(respBody, &data); err != nil {
		return "", "", fmt.Errorf("failed to parse token response: %w", err)
	}
	if data.Access == "" {
		return "", "", fmt.Errorf("пустой access_token в ответе")
	}
	return data.Refresh, data.Access, nil
}