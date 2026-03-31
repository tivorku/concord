package t2api

type LotInfo struct {
	ID    string
	IsBot bool
}

type Segment struct {
	UOM    string
	Volume int
	Cost   int
	Count  int
}
