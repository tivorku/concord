package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"concord/p2p"
	"concord/t2api"
	"os"
	"runtime"
	"strings"
	"time"
	"os/signal"
	"syscall"
	"sync"

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

	myLedger := p2p.NewLedger(*useMock)

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
        select {
        case <-ctx.Done():
            return
        default:
        }
		time.Sleep(200 * time.Millisecond)
		for i, lotID := range myLotIDs {
    		if i > 0 {
        		select {
        		case <-time.After(10 * time.Second):
        		case <-ctx.Done():
        		    return
        		}
        	}
			now := time.Now().Unix() + p2p.NetworkTimeOffset.Load()
			if *useMock {
			    myLedger.Update(lotID, h.ID(), 0, now, 0, p2p.GetCurrentEpoch(), 5, 5)
			} else {
			    for _, lot := range lots {
			        if lot.ID == lotID {
    			        myLedger.Update(lotID, h.ID(), 0, now, 0, p2p.GetCurrentEpoch(), lot.PremiumOps, lot.PremiumOps)
			        }
			    }
			}
			fmt.Printf("[MDN] Лот %d/%d зарегистрирован в %d\n", i+1, len(myLotIDs), now)
		}
	}()
	go myLedger.StartJanitor(ctx, node)
	var wg sync.WaitGroup
	core := p2p.InitLogicCore(myLedger, myLotIDs, volume, value, uom, bearer, number, node, *useMock, &wg)
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

	sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
    
    <-sigCh
    fmt.Printf("\nЗавершение работы...\n")
    cancel()
    done := make(chan struct{})
    go func() {
        wg.Wait()
        close(done)
    }()
    select {
    case <-done:
        fmt.Println("Все операции завершены.")
    case <-time.After(30*time.Second):
        fmt.Println("Таймаут ожидания. Принудительнон завершение.")
    }
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