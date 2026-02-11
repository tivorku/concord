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
//	"time"
)

func main() {
	if runtime.GOOS == "android" {
		hideAndroidNetworkError()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Инициализация памяти (Ledger)
	myLedger := p2p.NewLedger()
	go myLedger.StartJanitor(ctx)

	// 2. Авторизация и параметры (Твои функции из t2api)
	bearer, number := t2api.Login() // Вход по СМС или из файла
	myLotID, volume, value, err := t2api.ShowAndSelectLot(bearer, number)
	if err != nil {
	    fmt.Println(err)
	}
    //fmt.Println("Лот выбрался сам")
	// 3. Создание сетевого узла
	privKey, _ := p2p.GetPrivateKey("identity.key")
	h, err := p2p.InitHost(ctx, privKey)
	if err != nil {
		fmt.Printf("[ОШИБКА] Хост: %v\n", err)
		return
	}

	// 4. Настройка скрытого протокола
	rendezvous := GetProtocolID(fmt.Sprintf("%v-%v", volume, value), "thiztoolisverypowehfull")
	fmt.Printf("[MDN] Вход в сегмент: %s\n", rendezvous)

	// 5. Запуск поиска соседей и регистрации прокси
	p2p.StartDiscovery(ctx, h, rendezvous)
	// Важно: регистрируем возможность быть прокси для других
	mn := &p2p.MarketNode{Host: h, Ledger: myLedger, Ctx: ctx}
	mn.RegisterProxyHandler()

	// 6. Инициализация Стратега
	strat := p2p.NewStrategist(myLedger, myLotID, volume, value)

	// 7. Запуск рации (PubSub)
	topic, _ := p2p.StartPubSub(ctx, h, rendezvous, myLedger, strat, bearer, number)
	mn.Topic = topic

	// 8. Фоновые задачи Стратега (Анонсы и Дежурство)
	go strat.Run(ctx, mn, bearer, number)

	fmt.Printf("[MDN] Работаем! Мой PeerID: %s\n", h.ID().String()[len(h.ID().String())-8:])
	select {}
}

// GetProtocolID создает уникальный путь для libp2p
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