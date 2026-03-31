package t2api

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

type AccountConfig struct {
	ID      string `json:"id"`
	Number  string `json:"number"`
	Refresh string `json:"refresh"`
}

type AccountsFile struct {
	Accounts []AccountConfig `json:"accounts"`
}

type Account struct {
	ID      string
	Number  string
	Refresh string
	Bearer  string
}
