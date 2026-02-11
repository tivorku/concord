package p2p

import (
	"context"
	"encoding/hex"
/*	"io"
	"log"*/
	"net"
	"time"
	"os"
	"fmt"
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
)

type NodeMessage struct {
	Type   string `json:"type"`   // ANNOUNCE, ROCKET_FIRED, TOP_STATUS
	LotID  string `json:"lot_id"` // ID лота из Т2
	PeerID string `json:"peer_id"`// ID отправителя (ноды)
	T      int64  `json:"t"`      // Твои тики (время в топе)
	R      int    `json:"r"`      // Твои ракеты
}

func InitHost(ctx context.Context, privKey crypto.PrivKey) (host.Host, error) {
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

	h, err := libp2p.New(
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
	if err != nil {
		return nil, fmt.Errorf("не удалось запустить хост: %w", err)
	}

	return h, nil
}
func StartDiscovery(ctx context.Context, h host.Host, rendezvous string) {

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
}

func StartPubSub(ctx context.Context, h host.Host, topicName string) (*pubsub.Topic, error) {
	// 1. Создаем движок GossipSub
	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		return nil, err
	}

	// 2. Входим в топик (комнату рынка)
	topic, err := ps.Join(topicName)
	if err != nil {
		return nil, err
	}

	// 3. Подписываемся на сообщения
	sub, err := topic.Subscribe()
	if err != nil {
		return nil, err
	}

	// 4. Запускаем "Слушателя" в фоновом потоке
	go func() {
		for {
			msg, err := sub.Next(ctx) // Ждем пакет из сети
			if err != nil {
				return // Контекст закрыт или ошибка
			}

			// Пропускаем свои же сообщения
			if msg.ReceivedFrom == h.ID() {
				continue
			}

			// Распаковываем JSON
			var m NodeMessage
			if err := json.Unmarshal(msg.Data, &m); err != nil {
				continue // Мусор игнорируем
			}

			// Атом вывода: теперь мы видим, что делают другие
			fmt.Printf("\n[GOSSIP] От: %s | Лот: %s | T: %d | R: %d\n", 
				m.PeerID[:8], m.LotID, m.T, m.R)
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
