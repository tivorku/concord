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