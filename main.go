package main

import (
    "fmt"
	"context"
	"market-denet/p2p"
	"bufio"
	"os"
	"strings"
	"runtime"
	"time"
	"encoding/json"
	//"market-denet/t2api"
)

func main() {
    if runtime.GOOS == "android" {
        hideAndroidNetworkError()
    }
	ctx := context.Background()
	
	// 1. Создаем ноду
	privKey, _ := p2p.GetPrivateKey("identity.key")
	node, err := p2p.InitHost(ctx, privKey)
	if err != nil {
	    fmt.Errorf("node error: ", err)
	}
	rendezvous := "mdn-5gb-75r"
	p2p.StartDiscovery(ctx, node, rendezvous)
	fmt.Printf("Мой ID: %s\n", node.ID().String())
    for _, addr := range node.Addrs() {
        fmt.Printf("Я слушаю на: %s\n", addr)
    }
    topic, _ := p2p.StartPubSub(ctx, node, rendezvous)
    go func() {
        for {
            myMsg := p2p.NodeMessage{
                Type:   "ANNOUNCE",
                LotID:  "123", // Для теста в разных окнах напиши разные ID лота
                PeerID: node.ID().String(),
                T:      0, 
                R:      0,
            }
            
            raw, _ := json.Marshal(myMsg)
            topic.Publish(ctx, raw) // Выстрел в эфир

            time.Sleep(10 * time.Second)
        }
    }()

	// 2. Параметры лота (вводятся пользователем)
	//var myLotID string
	//bearer, number := t2api.Login()
    select {}
}
func hideAndroidNetworkError() {
	r, w, _ := os.Pipe()
	os.Stderr = w
	go func() {
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			line := scanner.Text()
			if !strings.Contains(line, "failed to resolve local interface addresses") {
				fmt.Fprintln(os.Stdout, line)
			}
		}
	}()
}