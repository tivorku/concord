package t2api

import ("fmt"
        "math")

func getCost(volume float64, TrafficType string) float64 {
    var cost float64
	switch TrafficType {
    	case "data":
    		cost = volume * 15
    	case "voice":
    		cost = math.Ceil(volume / 1.25)
    	case "sms":
    		cost = math.Ceil(volume / 2)
	}
	return cost
}
func ParametersSet() (string, float64, float64) {
    var (TrafficTypeInput int; TrafficType string; volume float64; cost float64; mincost string)
	fmt.Print("Введите цифру трафика (1–ГБ, 2–минуты, 3–SMS): ")
	fmt.Scanln(&TrafficTypeInput)
	TrafficDesc := map[int]string{1: "data", 2: "voice", 3: "sms"}
	TrafficType = TrafficDesc[TrafficTypeInput]
	fmt.Print("Введите количество трафика: ")
	fmt.Scanln(&volume)
	fmt.Print("Минималка? (Y/n): ")
	fmt.Scanln(&mincost)
	if no[mincost] {
		fmt.Print("Введите цену лота: ")
		fmt.Scanln(&cost)
	} else {
		cost = getCost(volume, TrafficType)
	}
	return TrafficType, volume, cost
}