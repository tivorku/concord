package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"market-denet/p2p"
	"market-denet/t2api"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

var (
	useMock   = flag.Bool("mock", false, "Use lotid.txt mock instead of segment selection")
	mockUOM   = flag.String("uom", "gb", "UOM for mock mode (gb, min, sms)")
	mockVol   = flag.Int("volume", 1, "Volume for mock mode")
	mockValue = flag.Int("value", 15, "Value/cost for mock mode")
)

func main() {
	flag.Parse()

	hideAndroidRestrictedNetworkError()

	godotenv.Load()

	if err := t2api.FetchCertificateFingerprint(); err != nil {
		fmt.Printf("[SECURITY] Failed to fetch T2 certificate: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	myLedger := p2p.NewLedger()

	var myLotIDs []string
	var uom string
	var volume, value int
	var lots []t2api.LotData
	bearer, number := t2api.Login()
	if *useMock {
		LotBytes, err := os.ReadFile("lotid.txt")
		if err != nil {
			fmt.Printf("Не удалось прочитать lotid.txt: %v\n", err)
			os.Exit(1)
		}
		lines := strings.Split(strings.TrimSpace(string(LotBytes)), "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line != "" {
				myLotIDs = append(myLotIDs, line)
			}
		}
		if len(myLotIDs) == 0 {
			fmt.Println("Не найдено ни одного ID лота в lotid.txt")
			os.Exit(1)
		}
		uom = *mockUOM
		volume = *mockVol
		value = *mockValue
		fmt.Printf("[MDN] Mock mode: %d lots, %s, %d ГБ, %d руб\n", len(myLotIDs), uom, volume, value)
	} else {
	    var err error
	    var segments []t2api.Segment
		segments, lots, err = t2api.GetSegments(bearer, number)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		selectedSeg, err := t2api.SelectSegment(segments)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		myLotIDs = t2api.FilterLotsBySegment(lots, selectedSeg)

		uom = selectedSeg.UOM
		volume = selectedSeg.Volume
		value = selectedSeg.Cost
	}

	privKey, _ := p2p.GetPrivateKey("identity.key")

	h, err := p2p.InitHost(ctx, privKey)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}

	node := &p2p.Node{Host: h, Ledger: myLedger, Ctx: ctx}

	rendezvous := p2p.GetProtocolID(uom, volume, value)

	p2p.StartDiscovery(ctx, h, rendezvous)
    go func() {
		time.Sleep(200 * time.Millisecond)
		for i, lotID := range myLotIDs {
    		if i > 0 {
        		time.Sleep(10 * time.Second)
        	}
			now := time.Now().Unix() + p2p.NetworkTimeOffset
			if *useMock {
			    myLedger.Update(lotID, h.ID(), p2p.PrivKeyToPubKey(privKey), 0, now, 0, p2p.GetCurrentEpoch(), 5, 5)
			} else {
			    for _, lot := range lots {
			        if lot.ID == lotID {
    			        myLedger.Update(lotID, h.ID(), p2p.PrivKeyToPubKey(privKey), 0, now, 0, p2p.GetCurrentEpoch(), lot.PremiumOps, lot.PremiumOps)
			        }
			    }
			}
			fmt.Printf("[MDN] Лот %d/%d зарегистрирован в %d\n", i+1, len(myLotIDs), now)
		}
	}()
	go myLedger.StartJanitor(ctx, node)
	core := p2p.InitLogicCore(myLedger, myLotIDs, volume, value, uom, privKey, bearer, number, node)
	node.RegisterProxyHandler()
	node.Topic = p2p.StartPubSub(ctx, h, rendezvous, myLedger, core)

	go core.Run(ctx, node)

	go func() {
		time.Sleep(200 * time.Millisecond)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				core.ShowDashboard(node, rendezvous)
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
