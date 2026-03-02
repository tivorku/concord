package t2api

import "os"

func Login() (string, string) {
    var access string
    Check()
	
	numBytes, _ := os.ReadFile("number.txt")
    number := string(numBytes)
	if number != "" {
		TryAnotherNumber()
	}

	numBytes, _ = os.ReadFile("number.txt")
	number = string(numBytes)
	if number == "" {
		number = GetNumber()
	}
	var err error
	refBytes, _ := os.ReadFile("refresh.txt")
	refresh := string(refBytes)
	if refresh != "" {
		_, access, err = GetTokens(refresh)
		if err != nil {
			os.Exit(1)
		}
	}

	refBytes, _ = os.ReadFile("refresh.txt")
	refresh = string(refBytes)
	if refresh == "" {
		sms_code := RequestSms(number)
		_, access = RequestBearer(number, sms_code)
	}
	bearer := "Bearer " + access
	return bearer, number
}

func TryAnotherNumber() {
    var ResetNumber string
	fmt.Print("Ввести другой номер телефона? (y/N): ")
	fmt.Scanln(&ResetNumber)
	if yes_reset[ResetNumber] {
		os.Remove("refresh.txt")
		os.Remove("number.txt")
		os.OpenFile("number.txt", os.O_CREATE, 0644)
		os.OpenFile("refresh.txt", os.O_CREATE, 0644)
	}
	return
}
func GetNumber() string {
    var RememberNumber string
	os.Remove("refresh.txt") // to exclude possibility of wrong refresh token
	fmt.Print("Введите номер телефона: +7")
	fmt.Scanln(&number)
	fmt.Print("Запомнить номер телефона? (Y/n): ")
	fmt.Scanln(&RememberNumber)
	if yes[RememberNumber] {
		os.WriteFile("number.txt", []byte(number), 0644)
	}
	return number
}
func Check() {
	os.OpenFile("refresh.txt", os.O_CREATE, 0644)
	os.OpenFile("number.txt", os.O_CREATE, 0644)
}