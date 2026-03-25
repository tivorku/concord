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
	"time"

)
func main() {
	hideAndroidRestrictedNetworkError()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
    
	myLedger := p2p.NewLedger()

	bearer, number := t2api.Login()
	/*myLotID, volume, value, err := t2api.ShowAndSelectLot(bearer, number)
	if err != nil {
	    fmt.Println(err)
	}*/
	
	// MOCKING myLotID FOR TESTING
	LotBytes, _ := os.ReadFile("lotid.txt")
	myLotID := string(LotBytes)
	var (
	    volume int = 1
	    value int = 15
	)
    
	privKey, _ := p2p.GetPrivateKey("identity.key")
	
	h, err := p2p.InitHost(ctx, privKey)
	if err != nil {
	    fmt.Println(err)
	    os.Exit(1)
	}
    
    node := &p2p.Node{Host: h, Ledger: myLedger, Ctx: ctx}
    
	rendezvous := node.GetProtocolID(volume, value)
	fmt.Printf("[Main] Вход в сегмент: %s\n", rendezvous)
	
	p2p.StartDiscovery(ctx, h, rendezvous)
	go func() {
	    time.Sleep(200 * time.Millisecond)
    	now := time.Now().Unix() + p2p.NetworkTimeOffset
    	// lotID string, pID peer.ID, incomingT int64, incomingR int64, joinedAt int64, lastTopTick int64, incomingEpoch int64
        myLedger.Update(myLotID, h.ID(), 0, 0, now, 0, p2p.GetCurrentEpoch())
    }()
	go myLedger.StartJanitor(ctx, node)
	core := p2p.InitLogicCore(myLedger, myLotID, volume, value, privKey, bearer, number)
	node.RegisterProxyHandler()
	node.Topic, _ = p2p.StartPubSub(ctx, h, rendezvous, myLedger, core)
	
	go core.Run(ctx, node)
	
	go func() {
	    time.Sleep(200 * time.Millisecond)
        for {
            select {
            case <-ctx.Done():
                return
            default:
                core.ShowDashboard(node, rendezvous)
                time.Sleep(2 * time.Second) 
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