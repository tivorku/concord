package p2p

import (
	"context"
	"encoding/hex"
    "io"
	"net"
	"time"
	"os"
	"fmt"
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
	tcp "github.com/libp2p/go-libp2p/p2p/transport/tcp"
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/core/protocol"
)
const ProtocolProxy protocol.ID = "/mdn/proxy/1.0.0"
type NodeMessage struct {
	Type   string `json:"type"`  // ANNOUNCE, ROCKET_FIRED, TOP_STATUS
	LotID  string `json:"lot_id"`
	PeerID peer.ID `json:"peer_id"`
	T      int64  `json:"t"` // тики
	R      int    `json:"r"` // ракеты
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
	Ledger  *Ledger
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
		libp2p.AddrsFactory(func(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {return addrs}),
		libp2p.ForceReachabilityPrivate(),
        libp2p.Transport(tcp.NewTCPTransport),
        libp2p.Transport(quic.NewTransport),
        libp2p.ListenAddrStrings(
            "/ip4/0.0.0.0/tcp/0",
            "/ip4/0.0.0.0/udp/0/quic-v1",
        ),
		libp2p.EnableAutoRelayWithStaticRelays([]peer.AddrInfo{*info}),
    )
	return h, err
}

func StartDiscovery(ctx context.Context, h host.Host, rendezvous string) *dht.IpfsDHT {

    relayAddr, _ := multiaddr.NewMultiaddr("/ip4/144.31.152.128/tcp/4001/p2p/12D3KooWS8gfSiFMenXBPDdyCqEDKsUJZXTby1nENpCjt2hLwS3N")
    relayInfo, _ := peer.AddrInfoFromP2pAddr(relayAddr)
    err := h.Connect(ctx, *relayInfo)
    /*if err == nil {
        authenticateAtRelay(ctx, h, relayInfo.ID)
    }*/
    
	kad, err := dht.New(ctx, h, dht.Mode(dht.ModeClient), dht.BootstrapPeers(*relayInfo))
	if err != nil {
		panic(err)
	}
    time.Sleep(5 * time.Second)
	if err = kad.Bootstrap(ctx); err != nil {
		panic(err)
	}

	for _, addr := range dht.DefaultBootstrapPeers {
		pi, _ := peer.AddrInfoFromP2pAddr(addr)
		if err := h.Connect(ctx, *pi); err != nil {
		    fmt.Println(err)
		} else {
		    fmt.Println("подключен к бутстрапу")
		}// соединяемся с "гигантами" сети
	}

	// объявляем о себе и ищем других
	routingDiscovery := routing.NewRoutingDiscovery(kad)
	util.Advertise(ctx, routingDiscovery, rendezvous)

	go func() {
		for {
			peersChan, err := routingDiscovery.FindPeers(ctx, rendezvous)
			if err != nil {
				return
			}

            for p := range peersChan {
                if p.ID == h.ID() { continue }
                if h.Network().Connectedness(p.ID) == network.Connected { continue }
                go func(peerInfo peer.AddrInfo) {
                    // если адресов нет - ищем их
                    if len(peerInfo.Addrs) == 0 {
                        found, err := kad.FindPeer(ctx, peerInfo.ID)
                        if err != nil { return }
                        peerInfo = found
                    }
                    err := h.Connect(ctx, peerInfo)
                    if err == nil {
                        fmt.Println("Найден сосед: ", peerInfo.ID)
                    }
                }(p)
            }
			time.Sleep(5*time.Second)
		}
	}()
	return kad
}
func StartPubSub(ctx context.Context, h host.Host, topicName string, l *Ledger, s *Strategist, bearer, number string) (*pubsub.Topic, error) {
    ps, _ := pubsub.NewGossipSub(ctx, h, pubsub.WithFloodPublish(true), pubsub.WithPeerExchange(true))
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

// RegisterProxyHandler — настраивает ноду на роль "посредника"
func (mn *MarketNode) RegisterProxyHandler() {
	// мы говорим хосту: "Если кто-то обратится по этому ID протокола — запусти эту функцию"
	mn.Host.SetStreamHandler(ProtocolProxy, func(stream network.Stream) {
		defer stream.Close()

		// используем net.Dial, потому что это выход из P2P-сети в обычный интернет
		conn, err := net.DialTimeout("tcp", t2api.T2FullHost, 10*time.Second)
		if err != nil {
			fmt.Printf("[ПРОКСИ] Ошибка соединения с T2: %v\n", err)
			stream.Reset()
			return
		}
		defer conn.Close()

		// запускаем двустороннее копирование байтов (туннель)
		errCh := make(chan error, 2)

		// поток А: от соседа к серверу T2
		go func() {
			_, err := io.Copy(conn, stream)
			errCh <- err
		}()

		// поток Б: от сервера T2 обратно соседу (ответ)
		go func() {
			_, err := io.Copy(stream, conn)
			errCh <- err
		}()

		// ждем, пока одна из сторон не закроет соединение
		err = <-errCh
		if err != nil {
		}
	})
}
func authenticateAtRelay(ctx context.Context, h host.Host, relayID peer.ID) error {
	// открываем поток авторизации к реле
	s, err := h.NewStream(ctx, relayID, "/mdn-private-auth/1.0.0")
	if err != nil {
		return err
	}
	defer s.Close()

	// отправляем пароль
	password := "2a35442281f13052136c53589ae2f51b"
	_, err = s.Write([]byte(password))
	if err != nil {
		return err
	}

	buf := make([]byte, 2)
	_, err = io.ReadFull(s, buf)
	if string(buf) != "OK" {
		fmt.Println("[Auth] Авторизация в реле прошла неудачно.")
	}
	return err
}