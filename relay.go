package main

import (
	"context"
	"log"
    "time"
    "sync"
    "io"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/crypto"
	"encoding/hex"
	"github.com/libp2p/go-libp2p/core/peerstore"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/network"
	connmgr "github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/core/host"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)
var (
	// Список авторизованных PeerID и время их авторизации
	trustedPeers = make(map[peer.ID]bool)
	authMu       sync.RWMutex
	password     = "2a35442281f13052136c53589ae2f51b"
	bootstrapIDs = make(map[peer.ID]bool)
)

func main() {
	ctx := context.Background()
	
	privKey, _ := loadKey()
	relayResources := relay.Resources{
        MaxReservations:        1024, // Вместо 128 по умолчанию
        MaxReservationsPerIP:   64,   // Вместо 4 по умолчанию
        MaxReservationsPerPeer: 64,
        ReservationTTL:         1 * time.Hour,
    }
    cm, err := connmgr.NewConnManager(
		256, // Low water mark
		512, // High water mark
		connmgr.WithGracePeriod(time.Minute),
	)
	if err != nil {
		panic(err)
	}
	h, err := libp2p.New(
	    libp2p.Identity(privKey),
		libp2p.ListenAddrStrings(
        "/ip4/0.0.0.0/tcp/4001",
        "/ip4/0.0.0.0/udp/4001/quic-v1",
        ),
        libp2p.EnableRelayService(relay.WithResources(relayResources)),
        libp2p.ResourceManager(&network.NullResourceManager{}),
        libp2p.ConnectionManager(cm),
        libp2p.EnableNATService(),
		libp2p.EnableAutoNATv2(),
		libp2p.ForceReachabilityPublic(),
		libp2p.EnableHolePunching(),
	)
	if err != nil {
		panic(err)
	}
	initBootstrapList()
    setupAuthHandler(h)
	defer h.Close()
    
    bootstrapPeers := dht.GetDefaultBootstrapPeerAddrInfos()
    kad, err := dht.New(ctx, h, dht.Mode(dht.ModeServer), dht.BootstrapPeers(bootstrapPeers...))
    if err != nil {
        panic(err)
    }
    localDHT, err := dht.New(ctx, h, dht.Mode(dht.ModeServer), dht.ProtocolPrefix("/mdenet"))
    if err != nil {
        panic(err)
    }
    for _, peerAddr := range bootstrapPeers {
        h.Peerstore().AddAddrs(peerAddr.ID, peerAddr.Addrs, peerstore.PermanentAddrTTL)
        go func(p peer.AddrInfo) {
            if err := h.Connect(ctx, p); err != nil {
            }
        }(peerAddr)
    }
    
    if err = kad.Bootstrap(ctx); err != nil {
        panic(err)
    }
    if err = localDHT.Bootstrap(ctx); err != nil {
        panic(err)
    }
    
    ps, _ := pubsub.NewGossipSub(ctx, h, pubsub.WithPeerExchange(true), pubsub.WithFloodPublish(true))
    startGossipOrchestrator(ctx, ps)
    
    addrs, _ := peer.AddrInfoToP2pAddrs(&peer.AddrInfo{
        ID:    h.ID(),
        Addrs: h.Addrs(),
    })
	log.Println("\n========================================================")
	log.Println("   MDN LIGHTHOUSE DEPLOYED")
	log.Println("========================================================")
	log.Println("Connection addresses:")
    for _, addr := range addrs {
        log.Println(addr.String())
    }
	log.Println("========================================================\n")
	select {}
}
func loadKey() (crypto.PrivKey, error) {
	data := "c79bbaf8b30abeb7d67680b2091ff8961f9022e9100ae321c840c5e769a717eef26c5b882d34161082675f553790aa3d6f9d04a5425ea2f64bd040f66c93606d"
	seed, _ := hex.DecodeString(string(data))
	return crypto.UnmarshalEd25519PrivateKey(seed)
}

func isBootstrap(id peer.ID) bool {
	return bootstrapIDs[id]
}
func setupAuthHandler(h host.Host) {
	h.SetStreamHandler("/mdn-private-auth/1.0.0", func(s network.Stream) {
		defer s.Close()
		
		// Читаем пароль от клиента
		buf := make([]byte, len(password))
		s.SetDeadline(time.Now().Add(5 * time.Second))
		_, err := io.ReadFull(s, buf)
		
		if err == nil && string(buf) == password {
			authMu.Lock()
			trustedPeers[s.Conn().RemotePeer()] = true
			authMu.Unlock()
			s.Write([]byte("OK")) // Подтверждаем клиенту успех
			log.Printf("[Auth] Peer %s successfully authorized\n", s.Conn().RemotePeer())
		}
	})

	// Фоновый процесс: разрыв соединений с неавторизованными
	go func() {
		for {
			time.Sleep(5 * time.Second)
			for _, conn := range h.Network().Conns() {
				pid := conn.RemotePeer()
				
				// Пропускаем бутстрап-узлы
				if isBootstrap(pid) {
					continue
				}
                authMu.RLock()
                trusted := trustedPeers[pid]
                authMu.RUnlock()
				// Если пир не авторизован и подключен более 10 секунд
				if !trusted && time.Since(conn.Stat().Opened) > 10*time.Second {
					//log.Printf("[Auth] Кикаем неавторизованного: %s\n", pid)
					conn.Close()
				}
			}
		}
	}()
	go func() {
        ticker := time.NewTicker(5 * time.Minute)
        defer ticker.Stop()
        for {
            select {
            case <-ticker.C:
                authMu.Lock()
                for pid := range trustedPeers {
                    if h.Network().Connectedness(pid) != network.Connected {
                        delete(trustedPeers, pid)
                        log.Printf("[Auth] Peer %s disconnected\n", pid)
                    }
                }
                authMu.Unlock()
            }
        }
    }()
}
func initBootstrapList() {
	for _, addr := range dht.DefaultBootstrapPeers {
		pi, err := peer.AddrInfoFromP2pAddr(addr)
		if err == nil {
			bootstrapIDs[pi.ID] = true
		}
	}
	log.Printf("[System] Loaded %d Bootstrap exceptions\n", len(bootstrapIDs))
}
func startGossipOrchestrator(ctx context.Context, ps *pubsub.PubSub) {
    joined := make(map[string]bool)
    controlTopic, _ := ps.Join("_global_discovery")
    sub, _ := controlTopic.Subscribe()

    go func() {
        for {
            msg, _ := sub.Next(ctx)
            topic := string(msg.Data)
            if !joined[topic] {
                t, err := ps.Join(topic)
                if err == nil {
                    joined[topic] = true
                    log.Printf("[Bones] Joined %s\n", topic)
                    t.Subscribe()
                } else {
                    log.Println(err)
                }
            } else {
                log.Println("Already connected to topic")
            }
        }
    }()
}