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

type SelectedLot struct {
	LotID     string
	AccountID string
}

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

	type AccountLots struct {
		Account *t2api.Account
		Lots    []t2api.LotInfoDetailed
	}
	var accountLots []AccountLots

	for _, acc := range accounts {
		fmt.Printf("[MDN] Получаю лоты аккаунта %s...\n", acc.ID)
		lots, err := t2api.GetAccountLots(acc.Bearer, acc.Number)
		if err != nil {
			fmt.Printf("[Error] %v\n", err)
			continue
		}
		accountLots = append(accountLots, AccountLots{Account: acc, Lots: lots})
		fmt.Printf("[MDN] Получено %d лотов\n", len(lots))
	}

	var allLots []t2api.LotInfoDetailed
	for _, al := range accountLots {
		allLots = append(allLots, al.Lots...)
	}

	segments := t2api.GetSegments(allLots)
	if len(segments) == 0 {
		fmt.Println("Нет доступных сегментов")
		os.Exit(1)
	}

	selectedSegment, err := t2api.SelectSegment(segments)
	if err != nil {
		fmt.Printf("[Error] %v\n", err)
		os.Exit(1)
	}

	var myLots []SelectedLot
	for _, al := range accountLots {
		lotIDs := t2api.FilterLotsBySegment(al.Lots, selectedSegment)
		for _, lotID := range lotIDs {
			myLots = append(myLots, SelectedLot{LotID: lotID, AccountID: al.Account.ID})
			myLedger.SetLotAccount(lotID, al.Account)
		}
	}

	if len(myLots) == 0 {
		fmt.Println("Нет лотов в выбранном сегменте")
		os.Exit(1)
	}

	var myLotIDs []string
	for _, lot := range myLots {
		myLotIDs = append(myLotIDs, lot.LotID)
	}

	fmt.Printf("[MDN] Всего для регистрации: %d лотов\n", len(myLots))

	privKey, _ := p2p.GetPrivateKey("identity.key")

	h, err := p2p.InitHost(ctx, privKey)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}

	node := &p2p.Node{Host: h, Ledger: myLedger, Ctx: ctx}

	rendezvous := p2p.GetProtocolID(selectedSegment.UOM, selectedSegment.Volume, selectedSegment.Cost)

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
	core := p2p.InitLogicCore(myLedger, myLotIDs, selectedSegment.Volume, selectedSegment.Cost, privKey, accounts, node)
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
