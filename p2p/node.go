package p2p

import (
	"context"
	"encoding/hex"
    "io"
	"net"
	"time"
	"os"
	"fmt"
	"runtime"
	"strings"
	"encoding/json"
	
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/p2p/discovery/routing"
	"github.com/libp2p/go-libp2p/p2p/discovery/util"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/wlynxg/anet"
	"github.com/libp2p/go-libp2p/core/protocol"
)
const ProtocolProxy protocol.ID = "/mdn/proxy/1.0.0"
type NodeMessage struct {
	Type   string `json:"type"`   // ANNOUNCE, ROCKET_FIRED, TOP_STATUS
	LotID  string `json:"lot_id"` // ID лота из Т2
	PeerID string `json:"peer_id"`// ID отправителя (ноды)
	T      int64  `json:"t"`      // Твои тики (время в топе)
	R      int    `json:"r"`      // Твои ракеты
	JoinedAt int64 `json:"joined_at"`
	IsBot  bool   `json:"is_bot"`
}
type MarketNode struct {
	Host    host.Host
	KadDHT  *dht.IpfsDHT
	PubSub  *pubsub.PubSub
	Topic   *pubsub.Topic
	Sub     *pubsub.Subscription
	Ledger  *Ledger        // Тот самый мозг-память
	Ctx     context.Context
}
func InitHost(ctx context.Context, privKey crypto.PrivKey) (host.Host, error) {
    var h host.Host
    var err error
    if runtime.GOOS == "android" {
        l, _ := net.Listen("tcp4", "0.0.0.0:0")
    	selectedPort := fmt.Sprintf("%d", l.Addr().(*net.TCPAddr).Port)
    	l.Close() 
    
    
    	var myAddrs []multiaddr.Multiaddr
    	ifaces, _ := anet.InterfaceAddrs()
    	for _, a := range ifaces {
    		raw := a.String()
    		ip := strings.Split(raw, "/")[0]
    		if ip == "127.0.0.1" || strings.Contains(ip, ":") || !strings.Contains(ip, ".") {
    			continue
    		}
    
    		// Внутри твоего цикла по интерфейсам
            mTCP, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%s", ip, selectedPort))
            if err == nil {
                myAddrs = append(myAddrs, mTCP)
            }
            
            // Добавь это для скорости и стабильности
            mQUIC, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/%s/udp/%s/quic-v1", ip, selectedPort))
            if err == nil {
                myAddrs = append(myAddrs, mQUIC)
            }
    		
    	}
    	h, err = libp2p.New(
    		libp2p.Identity(privKey),
    		libp2p.ListenAddrs(myAddrs...),
    		libp2p.AddrsFactory(func(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
    			return myAddrs
    		}),
    		libp2p.NATPortMap(),
    		libp2p.EnableNATService(),
    		libp2p.EnableHolePunching(),
    		libp2p.EnableAutoRelayWithStaticRelays(dht.GetDefaultBootstrapPeerAddrInfos()),
    	)
	} else {
	    h, err = libp2p.New(
    		libp2p.Identity(privKey),
    		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0", "/ip4/0.0.0.0/udp/0/quic-v1"),
    		libp2p.NATPortMap(),
    		libp2p.EnableNATService(),
    		libp2p.EnableHolePunching(),
    		libp2p.EnableAutoRelayWithStaticRelays(dht.GetDefaultBootstrapPeerAddrInfos()),
	    )
	}

	return h, err
}
func StartDiscovery(ctx context.Context, h host.Host, rendezvous string) *dht.IpfsDHT {

	kad, err := dht.New(ctx, h, dht.Mode(dht.ModeClient))
	if err != nil {
		panic(err)
	}

	// 2. Запускаем поиск путей
	if err = kad.Bootstrap(ctx); err != nil {
		panic(err)
	}

	// 3. Подключаемся к стандартным бутстрап-узлам
	for _, addr := range dht.DefaultBootstrapPeers {
		pi, _ := peer.AddrInfoFromP2pAddr(addr)
		/*if err != nil {
		    fmt.Println(err)
		}*/
		err = h.Connect(ctx, *pi) // Соединяемся с "гигантами" сети
		/*if err != nil {
		    fmt.Println(err)
		}*/
	}

	// 4. Объявляем о себе и ищем других
	routingDiscovery := routing.NewRoutingDiscovery(kad)
	
	// Горутина для "объявления" нас в сети
	util.Advertise(ctx, routingDiscovery, rendezvous)

	// Цикл поиска соседей
	go func() {
		for {
			peersChan, err := routingDiscovery.FindPeers(ctx, rendezvous)
			if err != nil {
				return
			}

			for p := range peersChan {
                if p.ID == h.ID() { continue }
                
                // Запускаем процесс для каждого соседа в отдельном потоке
                go func(peerInfo peer.AddrInfo) {
                    // 1. Если адресов нет - ищем их
                    if len(peerInfo.Addrs) == 0 {
                        found, err := kad.FindPeer(ctx, peerInfo.ID)
                        if err != nil { return }
                        peerInfo = found
                    }
                    
                    // 2. Подключаемся
                    if h.Network().Connectedness(peerInfo.ID) != network.Connected {
                        err := h.Connect(ctx, peerInfo)
                        if err == nil {
                            fmt.Println("Найден сосед: ", peerInfo.ID)
                        }
                    }
                }(p) // Передаем переменную p в горутину
            }
			time.Sleep(10*time.Second)
		}
	}()
	return kad
}
func StartPubSub(ctx context.Context, h host.Host, topicName string, l *Ledger, s *Strategist, bearer, number string) (*pubsub.Topic, error) {
    ps, _ := pubsub.NewGossipSub(ctx, h)
    topic, _ := ps.Join(topicName)
    sub, _ := topic.Subscribe()

    go func() {
        mn := &MarketNode{Host: h, Topic: topic, Ledger: l, Ctx: ctx}
        for {
            msg, err := sub.Next(ctx)
            if err != nil { return }
            if msg.ReceivedFrom == h.ID() { continue }

            var m NodeMessage
            if err := json.Unmarshal(msg.Data, &m); err != nil { continue }

            // ПЕРЕДАЕМ УПРАВЛЕНИЕ СТРАТЕГУ
            // Он сам решит: обновить Ledger или нажать на курок
            s.HandleMessage(ctx, mn, m, bearer, number)
        }
    }()
    return topic, nil
}
func GetPrivateKey(path string) (crypto.PrivKey, error) {
    if _, err := os.Stat(path); os.IsNotExist(err) {
        priv, _, _ := crypto.GenerateEd25519Key(nil)
        raw, _ := priv.Raw()
        os.WriteFile(path, []byte(hex.EncodeToString(raw)), 0600)
        return priv, nil
    }
    data, _ := os.ReadFile(path)
    seed, _ := hex.DecodeString(string(data))
    return crypto.UnmarshalEd25519PrivateKey(seed)
}

// RegisterProxyHandler — настраивает ноду на роль "Посредника"
func (mn *MarketNode) RegisterProxyHandler() {
	// Мы говорим хосту: "Если кто-то обратится по этому ID протокола — запусти эту функцию"
	mn.Host.SetStreamHandler(ProtocolProxy, func(stream network.Stream) {
		defer stream.Close()

		// 1. Подключаемся к реальному API Tele2
		// Мы используем net.Dial, потому что это выход из P2P-сети в обычный интернет
		conn, err := net.DialTimeout("tcp", "api.t2.ru:443", 10*time.Second)
		if err != nil {
			fmt.Printf("[ПРОКСИ] Ошибка соединения с T2: %v\n", err)
			stream.Reset()
			return
		}
		defer conn.Close()

		// 2. Запускаем двустороннее копирование байтов (Туннель)
		errCh := make(chan error, 2)

		// Поток А: От соседа к серверу T2
		go func() {
			_, err := io.Copy(conn, stream)
			errCh <- err
		}()

		// Поток Б: От сервера T2 обратно соседу (ответ)
		go func() {
			_, err := io.Copy(stream, conn)
			errCh <- err
		}()

		// Ждем, пока одна из сторон не закроет соединение
		err = <-errCh
		if err != nil {
			// Это нормальная ситуация, когда выстрел завершен
		}
	})
}