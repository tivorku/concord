package p2p

import (
	"context"
	"encoding/hex"
    "io"
	"net"
	"time"
	"os"
	"fmt"
	//"runtime"
	//"strings"
	"encoding/json"
	
	"market-denet/t2api"
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
	//"github.com/wlynxg/anet"
	tcp "github.com/libp2p/go-libp2p/p2p/transport/tcp"
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/core/protocol"
)
const ProtocolProxy protocol.ID = "/mdn/proxy/1.0.0"
type NodeMessage struct {
	Type   string `json:"type"`   // ANNOUNCE, ROCKET_FIRED, TOP_STATUS
	LotID  string `json:"lot_id"` // ID лота из Т2
	PeerID peer.ID `json:"peer_id"`// ID отправителя (ноды)
	T      int64  `json:"t"`      // Твои тики (время в топе)
	R      int    `json:"r"`      // Твои ракеты
	JoinedAt int64 `json:"joined_at"`
	LastTopTick int64 `json:"last_top_tick"`
	GlobalTick int64 `json:"global_tick"`
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
    relayAddr, _ := multiaddr.NewMultiaddr("/ip4/144.31.152.128/tcp/4001/p2p/12D3KooWS8gfSiFMenXBPDdyCqEDKsUJZXTby1nENpCjt2hLwS3N")
    info, _ := peer.AddrInfoFromP2pAddr(relayAddr)
    h, err = libp2p.New(
		libp2p.Identity(privKey),
		libp2p.EnableRelay(),
		libp2p.EnableHolePunching(),
		libp2p.NATPortMap(),
		libp2p.EnableAutoNATv2(),
		libp2p.ForceReachabilityPrivate(),
        libp2p.Transport(tcp.NewTCPTransport),
        libp2p.Transport(quic.NewTransport),
        libp2p.ListenAddrStrings(
            "/ip4/0.0.0.0/tcp/0",         // Для TCP
            "/ip4/0.0.0.0/udp/0/quic-v1", // Для QUIC (UDP)
        ),
		libp2p.EnableAutoRelayWithStaticRelays([]peer.AddrInfo{*info}),
    )
	return h, err
}

func StartDiscovery(ctx context.Context, h host.Host, rendezvous string) *dht.IpfsDHT {

    relayAddr, _ := multiaddr.NewMultiaddr("/ip4/144.31.152.128/tcp/4001/p2p/12D3KooWS8gfSiFMenXBPDdyCqEDKsUJZXTby1nENpCjt2hLwS3N")
    relayInfo, _ := peer.AddrInfoFromP2pAddr(relayAddr)
    err := h.Connect(ctx, *relayInfo)
    if err == nil {
        authenticateAtRelay(ctx, h, relayInfo.ID)
    }
    
	kad, err := dht.New(ctx, h, dht.Mode(dht.ModeClient), dht.BootstrapPeers(*relayInfo))
	if err != nil {
		panic(err)
	}
    
    // 1.5. Подключаемся к нашему Маяку как к единственному Bootstrap-узлу
	// 2. Запускаем поиск путей
	if err = kad.Bootstrap(ctx); err != nil {
		panic(err)
	}

	// 3. Подключаемся к стандартным бутстрап-узлам
	for _, addr := range dht.DefaultBootstrapPeers {
		pi, _ := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
		    fmt.Println(err)
		}
		h.Connect(ctx, *pi) // Соединяемся с "гигантами" сети
	}

	// 4. Объявляем о себе и ищем других
	routingDiscovery := routing.NewRoutingDiscovery(kad)
	// Горутина для "объявления" нас в сети
	util.Advertise(ctx, routingDiscovery, rendezvous)

	// Цикл поиска соседей
	go func() {
		for {
		    // В цикле дебага
           /* conns := h.Network().Conns()
            fmt.Printf("[DEBUG] Всего сетевых соединений: %d\n", len(conns))
            for _, c := range conns {
            fmt.Printf("   -> Соединен с: %s\n", c.RemotePeer().String())
            }*/
			peersChan, err := routingDiscovery.FindPeers(ctx, rendezvous)
			if err != nil {
				return
			}

            for p := range peersChan {
                if p.ID == h.ID() { continue }
                if h.Network().Connectedness(p.ID) == network.Connected { continue }
                // Запускаем процесс для каждого соседа в отдельном потоке
                go func(peerInfo peer.AddrInfo) {
                    // 1. Если адресов нет - ищем их
                    if len(peerInfo.Addrs) == 0 {
                        found, err := kad.FindPeer(ctx, peerInfo.ID)
                        if err != nil { return }
                        peerInfo = found
                    }
                    
                    // 2. Подключаемся
                    err := h.Connect(ctx, peerInfo)
                    if err == nil {
                        fmt.Println("Найден сосед: ", peerInfo.ID)
                    }
                }(p) // Передаем переменную p в горутину
            }
			time.Sleep(10*time.Second)
		}
	}()
	return kad
}
func StartPubSub(ctx context.Context, h host.Host, topicName string, l *Ledger, s *Strategist, bearer, number string) (*pubsub.Topic, error) {
    ps, _ := pubsub.NewGossipSub(ctx, h, pubsub.WithFloodPublish(true), pubsub.WithDirectConnectTicks(1))
    topic, _ := ps.Join(topicName)
    sub, _ := topic.Subscribe()
    
    go func() {
        
        mn := &MarketNode{Host: h, Topic: topic, Ledger: l, Ctx: ctx}
        for {
            //fmt.Printf("[DEBUG-PUBSUB] Соседей в топике %s: %d\n", topicName, len(topic.ListPeers()))
            msg, err := sub.Next(ctx)
            if err != nil { return }
            if msg.ReceivedFrom == h.ID() { continue }

            var m NodeMessage
            if err := json.Unmarshal(msg.Data, &m); err != nil { continue }
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
		conn, err := net.DialTimeout("tcp", t2api.T2FullHost, 10*time.Second)
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
func authenticateAtRelay(ctx context.Context, h host.Host, relayID peer.ID) error {
	// Открываем поток авторизации к реле
	s, err := h.NewStream(ctx, relayID, "/mdn-private-auth/1.0.0")
	if err != nil {
		return err
	}
	defer s.Close()

	// Отправляем пароль
	password := "2a35442281f13052136c53589ae2f51b"
	_, err = s.Write([]byte(password))
	if err != nil {
		return err
	}

	// Читаем ответ "OK"
	buf := make([]byte, 2)
	_, err = io.ReadFull(s, buf)
	if string(buf) != "OK" {
		fmt.Println("[Auth] Авторизация в реле прошла неудачно.")
	}
	return err
}