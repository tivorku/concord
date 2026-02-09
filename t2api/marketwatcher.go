package t2api
import (
    "fmt"
    "time"
)
func WatchMarket(bearer, number string) {
    var volume, value int
    var myID string
    fmt.Print("Введите кол-во трафика: ")
    fmt.Scanln(&volume)
    fmt.Print("Введите цену трафика: ")
    fmt.Scanln(&value)
    fmt.Print("Введите ID вашего лота: ")
    fmt.Scanln(&myID)
    for {
        time.Sleep(3 * time.Second)
        ids, err := GetTop4IDs(volume, value)
        if err != nil {
            fmt.Println(err)
        }
        foundInTop := false
		for _, id := range ids {
			if id == myID {
				fmt.Println("В топе есть наш лот. Не рыпаемся.")
				foundInTop = true
				break
            }
        }
        if !foundInTop {
            fmt.Println("В топе нет наших лотов! Выбираем кого запускать!")
            /*err := Rocket(bearer, number, myID)
            if err != nil {
                fmt.Println(err)
            }*/
        }
    }
}