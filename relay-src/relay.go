package main

import (
	"context"
	"encoding/hex"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	"log"
	"runtime"
	"time"

	"github.com/libp2p/go-libp2p/core/protocol"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	connmgr "github.com/libp2p/go-libp2p/p2p/net/connmgr"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	manet "github.com/multiformats/go-multiaddr/net"
)

var password = "2a35442281f13052136c53589ae2f51b"

func init() {
	runtime.GOMAXPROCS(1)
}
func main() {
	ctx := context.Background()

	privKey, _ := loadKey()
	wm := NewWhitelistManager("whitelist.json")
	for _, addr := range dht.DefaultBootstrapPeers {
		if ip, err := manet.ToIP(addr); err == nil {
			wm.IPs[ip.String()] = true
		}
	}

	cm, err := connmgr.NewConnManager(
		256, // Low water mark
		512, // High water mark
		connmgr.WithGracePeriod(time.Minute),
	)
	if err != nil {
		panic(err)
	}

	scalingLimits := rcmgr.DefaultLimits
	libp2p.SetDefaultServiceLimits(&scalingLimits)
	concreteLimits := scalingLimits.Scale(1<<30, 1024)
	cfg := rcmgr.PartialLimitConfig{
		System: rcmgr.ResourceLimits{
			ConnsInbound:   rcmgr.LimitVal(512),
			StreamsInbound: rcmgr.LimitVal(512),
			Memory:         128 << 20,
		},
		Transient: rcmgr.ResourceLimits{
			ConnsInbound:   rcmgr.LimitVal(256),
			ConnsOutbound:  rcmgr.LimitVal(256),
			StreamsInbound: rcmgr.LimitVal(512),
		},
		Protocol: map[protocol.ID]rcmgr.ResourceLimits{
			"/ipfs/kad/1.0.0": {
				StreamsInbound:  rcmgr.LimitVal(512),
				StreamsOutbound: rcmgr.LimitVal(512),
				Memory:          64 << 20,
			},
			"/ipfs/id/1.0.0": {
				StreamsInbound:  rcmgr.LimitVal(128),
				StreamsOutbound: rcmgr.LimitVal(128),
				Memory:          64 << 20,
			},
		},
	}
	finalLimits := cfg.Build(concreteLimits)
	limiter := rcmgr.NewFixedLimiter(finalLimits)
	rm, err := rcmgr.NewResourceManager(limiter)
	if err != nil {
		panic(err)
	}
	myResources := relay.DefaultResources()
	myResources.Limit.Duration = 30 * time.Minute
	h, err := libp2p.New(
		libp2p.Identity(privKey),
		libp2p.ListenAddrStrings(
			"/ip4/0.0.0.0/tcp/42954",
			"/ip4/0.0.0.0/udp/42954/quic-v1",
			"/ip6/::/tcp/42954",
			"/ip6/::/udp/42954/quic-v1",
		),
		libp2p.EnableRelayService(relay.WithResources(myResources)),
		libp2p.ResourceManager(rm),
		libp2p.ConnectionManager(cm),
		libp2p.EnableNATService(),
		libp2p.ConnectionGater(&RelayGater{w: wm}),
		libp2p.ForceReachabilityPublic(),
		libp2p.EnableHolePunching(),
	)
	if err != nil {
		panic(err)
	}
	defer h.Close()
	go RunAuthServer(wm, "8080", password, h)
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
	log.Println("   LIGHTHOUSE DEPLOYED")
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
