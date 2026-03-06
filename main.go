package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	//"encoding/json"
	"fmt"
	"market-denet/p2p"
	"market-denet/t2api"
	"os"
	"runtime"
	"strings"
	"strconv"
	"math/rand/v2"
	"time"
)
func main() {
	if runtime.GOOS == "android" {
		hideAndroidNetworkError()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	myLedger := p2p.NewLedger()
	go myLedger.StartJanitor(ctx)

	bearer, number := t2api.Login()
	//myLotID, volume, value, err := t2api.ShowAndSelectLot(bearer, number)
	/*if err != nil {
	    fmt.Println(err)
	}*/
	min := int64(10_000_000_000_000_000)
	max := int64(100_000_000_000_000_000)
	
	myLotID := strconv.Itoa(int(rand.Int64N(max-min) + min))
	var (
	    volume int = 1
	    value int = 15
	)

	privKey, _ := p2p.GetPrivateKey("identity.key")
	h, err := p2p.InitHost(ctx, privKey)
	if err != nil {
		fmt.Printf("[ОШИБКА] Хост: %v\n", err)
		return
	}
	now := time.Now().Unix()
    myLedger.Update(myLotID, h.ID(), 0, 0, now, 0, 0) 

	rendezvous := GetProtocolID(fmt.Sprintf("%d-%d", volume, value), "W2Rw_qon&lV3wxlbhFE4")
	fmt.Printf("[MDN] Вход в сегмент: %s\n", rendezvous)
	p2p.StartDiscovery(ctx, h, rendezvous)
    // регистрируем возможность быть прокси для других
	mn := &p2p.MarketNode{Host: h, Ledger: myLedger, Ctx: ctx}
	strat := p2p.NewStrategist(myLedger, myLotID, volume, value)
	mn.RegisterProxyHandler()
	mn.Topic, _ = p2p.StartPubSub(ctx, h, rendezvous, myLedger, strat, bearer, number)
	//mn.Topic = topic

  /*  go func() {
        for {
            fmt.Println("\nТекущие адреса:")
                for _, addr := range h.Addrs() {
                    fmt.Println(addr)
                }
            conns := h.Network().Conns()
            fmt.Printf("[DEBUG] Всего сетевых соединений: %d\n", len(conns))
            time.Sleep(3 * time.Second)
        }
    }()*/
	// фоновые задачи стратега (анонсы и дежурство)
	go strat.Run(ctx, mn, bearer, number)
	go func() {
        for {
            select {
            case <-ctx.Done():
                return
            default:
                strat.ShowDashboard()
                time.Sleep(3 * time.Second) 
            }
        }
    }()

	select {}
}

func GetProtocolID(base string, salt string) string {
	data := []byte(base + salt)
	hash := sha256.Sum256(data)
	return fmt.Sprintf("/mdn/v0.1/%x", hash[:8])
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