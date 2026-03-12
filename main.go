package main

import (
	"bufio"
	"context"
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
	hideAndroidRestrictedNetworkError()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	myLedger := p2p.NewLedger()
	go myLedger.StartJanitor(ctx)

	bearer, number := t2api.Login()
	//myLotID, volume, value, err := t2api.ShowAndSelectLot(bearer, number)
	/*if err != nil {
	    fmt.Println(err)
	}*/
	// MOCKING myLotID FOR TESTING
	min := int64(10_000_000_000_000_000)
	max := int64(100_000_000_000_000_000)
	myLotID := strconv.Itoa(int(rand.Int64N(max-min) + min))
	var (
	    volume int = 1
	    value int = 15
	)
    
	privKey, _ := p2p.GetPrivateKey("identity.key")
	
	h, _ := p2p.InitHost(ctx, privKey)
	
	now := time.Now().Unix()
    myLedger.Update(myLotID, h.ID(), 0, 0, now, 0, 0, p2p.GetCurrentEpoch()) 

	rendezvous := p2p.GetProtocolID(volume, value)
	fmt.Printf("[Main] Вход в сегмент: %s\n", rendezvous)
	
	p2p.StartDiscovery(ctx, h, rendezvous)
	
	mn := &p2p.MarketNode{Host: h, Ledger: myLedger, Ctx: ctx}
	core := p2p.InitLogicCore(myLedger, myLotID, volume, value)
	mn.RegisterProxyHandler()
	
	mn.Topic, _ = p2p.StartPubSub(ctx, h, rendezvous, myLedger, core, bearer, number)
	
	go core.Run(ctx, mn, bearer, number)
	
	go func() {
        for {
            select {
            case <-ctx.Done():
                return
            default:
                core.ShowDashboard()
                time.Sleep(5 * time.Second) 
            }
        }
    }()
    
	select {}
}

func hideAndroidRestrictedNetworkError() {
    if runtime.GOOS == "android" {
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
	return
}

// 2. Использование как Public Key для лицензии
/*func VerifyLicense(data, signature []byte) bool {
pepper := GetPepperSafe()
defer pepper.Destroy()
// Используем байты ключа для проверки подписи ed25519
return ed25519.Verify(pepper.Bytes(), data, signature)
}*/