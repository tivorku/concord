package t2api
import (
    "net/http"
    "time"
)
var (
    yes_reset = map[string]bool{"yes": true, "ye": true, "y": true}
	no = map[string]bool{"no": true, "n": true}
	no_reset = map[string]bool{"": true, "no": true, "n": true}
	yes = map[string]bool{"": true, "yes": true, "ye": true, "y": true}
	client = &http.Client{
	    Timeout: 20 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:          1000,
			DisableCompression:    false,
			ExpectContinueTimeout: 0,
			DisableKeepAlives:     false,
			MaxIdleConnsPerHost:   1000,
			IdleConnTimeout:       1 * time.Hour,
		},
	}
)