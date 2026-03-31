package main

import (
	"bufio"
	"context"
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

	if err := t2api.FetchCertificateFingerprint(); err != nil {
		fmt.Printf("[SECURITY] Failed to fetch T2 certificate: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	myLedger := p2p.NewLedger()

	accounts, err := t2api.MultiLogin()
	if err != nil {
		fmt.Printf("[Error] %v\n", err)
		os.Exit(1)
	}

	type SelectedLot struct {
		LotID     string
		AccountID string
	}
	var myLots []SelectedLot

	for _, acc := range accounts {
		lotIDs, err := t2api.SelectAccountLots(acc)
		if err != nil {
			fmt.Printf("[Error] %v\n", err)
			continue
		}
		for _, lotID := range lotIDs {
			myLots = append(myLots, SelectedLot{LotID: lotID, AccountID: acc.ID})
			myLedger.SetLotAccount(lotID, acc)
		}
	}

	if len(myLots) == 0 {
		fmt.Println("Не выбран ни один лот")
		os.Exit(1)
	}

	var myLotIDs []string
	for _, lot := range myLots {
		myLotIDs = append(myLotIDs, lot.LotID)
	}

	var (
		volume int = 43
		value  int = 645
	)

	privKey, _ := p2p.GetPrivateKey("identity.key")

	h, err := p2p.InitHost(ctx, privKey)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}

	node := &p2p.Node{Host: h, Ledger: myLedger, Ctx: ctx}

	rendezvous := p2p.GetProtocolID(volume, value)

	p2p.StartDiscovery(ctx, h, rendezvous)
	go func() {
		time.Sleep(200 * time.Millisecond)
		lotIndex := 0
		for _, lot := range myLots {
			if lotIndex > 0 {
				time.Sleep(10 * time.Second)
			}
			now := time.Now().Unix() + p2p.NetworkTimeOffset
			myLedger.Update(lot.LotID, h.ID(), p2p.PrivKeyToPubKey(privKey), 0, 0, now, 0, p2p.GetCurrentEpoch(), lot.AccountID)
			fmt.Printf("[MDN] Лот %s (аккаунт: %s) зарегистрирован\n", lot.LotID, lot.AccountID)
			lotIndex++
		}
	}()
	go myLedger.StartJanitor(ctx, node)
	core := p2p.InitLogicCore(myLedger, myLotIDs, volume, value, privKey, accounts, node)
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
