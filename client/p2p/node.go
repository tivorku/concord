package p2p

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"market-denet/pb"
	"market-denet/t2api"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/control"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/discovery/routing"
	"github.com/libp2p/go-libp2p/p2p/discovery/util"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"google.golang.org/protobuf/proto"
)

type ClientGater struct {
	relayID peer.ID
	relayIP string
	pass    string
	myID    peer.ID
}

var NetworkTimeOffset int64

const (
	ColorRed   = "\033[31m"
	ColorReset = "\033[0m"
)

const ProtocolProxy protocol.ID = "/mdn/proxy/1.0.0"

type NodeMessage struct {
	Type        string  `json:"type"` // ANNOUNCE, ROCKET, TOP, SYNC, VIOLATION
	LotID       string  `json:"lot_id"`
	PeerID      peer.ID `json:"peer_id"`
	T           int64   `json:"t"` // тики
	ActiveOps   int64   `json:"active_ops"`
	NetOps      int64   `json:"net_ops"`
	JoinedAt    int64   `json:"joined_at"`
	LastEpoch   int64   `json:"last_epoch"`
	LastTopTick int64   `json:"last_top_tick"`
	IsBot       bool    `json:"is_bot"`
	Signature   []byte  `json:"signature"`
}
type Node struct {
	Host          host.Host
	Topic         *pubsub.Topic
	Ledger        *Ledger
	Ctx           context.Context
	lastProxyTime time.Time
	proxyMu       sync.Mutex
}

func InitHost(ctx context.Context, privKey crypto.PrivKey) (host.Host, error) {
	var h host.Host
	var err error
	s := os.Getenv("RELAY_ADDR")
	if s == "" {
		return nil, fmt.Errorf("RELAY_ADDR не установлен")
	}
	relayAddr, err := multiaddr.NewMultiaddr(s)
	if err != nil {
		return nil, fmt.Errorf("неверный RELAY_ADDR: %w", err)
	}
	relayInfo, err := peer.AddrInfoFromP2pAddr(relayAddr)
	if err != nil {
		return nil, fmt.Errorf("неверный адрес relay: %w", err)
	}
	relayIP, err := manet.ToIP(relayAddr)
	if err != nil {
		return nil, fmt.Errorf("не удалось извлечь IP из адреса relay: %w", err)
	}
	sharedPass := os.Getenv("RELAY_PASSWORD")
	if sharedPass == "" {
		return nil, fmt.Errorf("RELAY_PASSWORD не установлен")
	}
	myID, err := peer.IDFromPrivateKey(privKey)
	if err != nil {
		return nil, fmt.Errorf("не удалось получить peer ID из приватного ключа: %w", err)
	}
	myGater := &ClientGater{
		relayID: relayInfo.ID,
		relayIP: relayIP.String(),
		pass:    sharedPass,
		myID:    myID,
	}

	h, err = libp2p.New(
		libp2p.Identity(privKey),
		libp2p.EnableRelay(),
		libp2p.EnableHolePunching(),
		libp2p.NATPortMap(),
		libp2p.EnableAutoNATv2(),
		libp2p.ForceReachabilityPrivate(),
		libp2p.ConnectionGater(myGater),
		libp2p.DefaultTransports,
		libp2p.AddrsFactory(func(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
			var filtered []multiaddr.Multiaddr
			for _, addr := range addrs {
				if manet.IsPublicAddr(addr) && !manet.IsIPLoopback(addr) {
					filtered = append(filtered, addr)
				}
			}
			return filtered
		}),
		libp2p.ListenAddrStrings(
			"/ip4/0.0.0.0/tcp/0",
			"/ip4/0.0.0.0/udp/0/quic-v1",
			"/ip6/::/tcp/0",
			"/ip6/::/udp/0/quic-v1",
		),
		libp2p.EnableAutoRelayWithStaticRelays([]peer.AddrInfo{*relayInfo}),
	)
	return h, err
}
func StartDiscovery(ctx context.Context, h host.Host, rendezvous string) {
	s := os.Getenv("RELAY_ADDR")
	relayAddr, err := multiaddr.NewMultiaddr(s)
	if err != nil {
		fmt.Printf("[Error] Неверный RELAY_ADDR: %v\n", err)
		return
	}
	relayInfo, err := peer.AddrInfoFromP2pAddr(relayAddr)
	if err != nil {
		fmt.Printf("[Error] Неверный адрес relay: %v\n", err)
		return
	}
	go MaintainRelayConn(ctx, h, relayInfo)
	go pingRelay(ctx, h, relayInfo.ID)
	kad, err := dht.New(ctx, h, dht.Mode(dht.ModeClient), dht.BootstrapPeers(*relayInfo))
	if err != nil {
		panic(err)
	}
	localDHT, err := dht.New(ctx, h, dht.Mode(dht.ModeClient), dht.BootstrapPeers(*relayInfo), dht.ProtocolPrefix("/mdenet"))
	if err != nil {
		panic(err)
	}

	if err = kad.Bootstrap(ctx); err != nil {
		panic(err)
	}
	if err = localDHT.Bootstrap(ctx); err != nil {
		panic(err)
	}
	localRouting := routing.NewRoutingDiscovery(localDHT)
	go func() {
		for {
			time.Sleep(2 * time.Second)
			util.Advertise(ctx, localRouting, rendezvous)
		}
	}()
    /*go func() {
		time.Sleep(1 * time.Second)
		for {
			fmt.Println("\n[Debug] Текущие адреса:")
			for _, addr := range h.Addrs() {
				fmt.Println(addr)
			}
			conns := h.Network().Conns()
			if h.Network().Connectedness(relayInfo.ID) == network.Connected {
				fmt.Println("[Debug] Есть соединение с реле")
			}
			fmt.Printf("[Debug] Всего сетевых соединений: %d\n", len(conns))
			fmt.Println("[Debug] Разница во времени:", NetworkTimeOffset)
			time.Sleep(3 * time.Second)
		}
	}()*/
	go func() {
		for {
			peersChan, err := localRouting.FindPeers(ctx, rendezvous)
			if err != nil {
				return
			}
			for p := range peersChan {
				if p.ID == h.ID() {
					continue
				}
				go func(peerInfo peer.AddrInfo) {
					// если адресов нет - ищем их
					if len(peerInfo.Addrs) == 0 {
						found, err := localDHT.FindPeer(ctx, peerInfo.ID)
						if err != nil {
							return
						}
						peerInfo = found
					}
					if h.Network().Connectedness(peerInfo.ID) != network.Connected {
						h.Connect(ctx, peerInfo)
					}
				}(p)
			}
			time.Sleep(10 * time.Second)
		}
	}()
	return
}
func StartPubSub(ctx context.Context, h host.Host, topicName string, l *Ledger, lc *LogicCore) *pubsub.Topic {
	params := pubsub.DefaultGossipSubParams()
	params.HeartbeatInterval = 500 * time.Millisecond
	params.PruneBackoff = 5 * time.Second
	ps, _ := pubsub.NewGossipSub(ctx, h, pubsub.WithPeerExchange(true), pubsub.WithFloodPublish(true), pubsub.WithGossipSubParams(params))
	topic, _ := ps.Join(topicName)
	sub, _ := topic.Subscribe()
	go func() {
		metaTopic, _ := ps.Join("_global_discovery")
		for {
			if len(metaTopic.ListPeers()) > 0 {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if err := metaTopic.Publish(ctx, []byte(topicName)); err != nil {
			fmt.Println(err)
		}
	}()
	go func() {
		node := &Node{Host: h, Topic: topic, Ledger: l, Ctx: ctx}
		for {
			fmt.Printf("[Debug] Соседей в топике %s: %d\n", topicName, len(topic.ListPeers()))
			msg, err := sub.Next(ctx)
			if err != nil {
				return
			}

			senderPID := msg.ReceivedFrom
			if senderPID == h.ID() {
				continue
			}

			var pm pb.NodeMessage
			if err := proto.Unmarshal(msg.Data, &pm); err != nil {
				continue
			}
			
			pID, _ := peer.Decode(pm.PeerId)
			if !lc.VerifyIncomingMessage(&pm, pID) {
				continue
			}

			m := NodeMessage{
				Type:        pm.Type,
				LotID:       pm.LotId,
				PeerID:      pID,
				T:           pm.T,
				ActiveOps:   pm.ActiveOps,
				NetOps:      pm.NetOps,
				JoinedAt:    pm.JoinedAt,
				LastEpoch:   pm.LastEpoch,
				LastTopTick: pm.LastTopTick,
				IsBot:       pm.IsBot,
				Signature:   pm.Signature,
			}
			lc.HandleMessage(ctx, node, m)
		}
	}()
	return topic
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
func (node *Node) RegisterProxyHandler() {
	node.Host.SetStreamHandler(ProtocolProxy, func(stream network.Stream) {
		caller := stream.Conn().RemotePeer()

		node.Ledger.mu.Lock()
		_, exists := node.Ledger.Members[caller.String()]
		node.Ledger.mu.Unlock()

		if !exists {
			fmt.Printf("[PROXY] Denied: %s not in ledger\n", caller)
			stream.Reset()
			return
		}

		node.proxyMu.Lock()
		if time.Since(node.lastProxyTime) < 5*time.Second {
			node.proxyMu.Unlock()
			fmt.Printf("[PROXY] Rate limit (429) for %s\n", caller)
			resp := "HTTP/1.1 429 Too Many Requests\r\n" +
				"Content-Length: 0\r\n" +
				"Connection: close\r\n" +
				"\r\n"
			stream.Write([]byte(resp))
			stream.Close()
			return
		}
		node.lastProxyTime = time.Now()
		node.proxyMu.Unlock()

		fmt.Printf("%s[PROXY] Tunnel from %s to T2...%s\n", ColorRed, caller[len(caller)-8:], ColorReset)
		defer stream.Close()

		conn, err := net.DialTimeout("tcp", t2api.T2FullHost, 10*time.Second)
		if err != nil {
			fmt.Printf("[Proxy] Connection error: %v\n", err)
			stream.Reset()
			return
		}
		defer conn.Close()

		if fp := t2api.GetCertFingerprint(); fp != "" {
			if err := t2api.ValidateCert(conn, fp); err != nil {
				fmt.Printf("[Proxy] Certificate validation failed: %v\n", err)
				conn.Close()
				stream.Reset()
				return
			}
		}

		errCh := make(chan error, 2)

		go func() {
			_, err := io.Copy(conn, stream)
			errCh <- err
		}()

		go func() {
			_, err := io.Copy(stream, conn)
			errCh <- err
		}()

		err = <-errCh
		if err != nil {
		}
	})
}

func GetProtocolID(uom string, volume, value int) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s-%d-%d", uom, volume, value)
	res := h.Sum(nil)
	return fmt.Sprintf("/mdn/v0.1/%x", res[len(res)-4:])
}

func KnockToRelay(relayIP string, myID peer.ID, pass string) {
	url := fmt.Sprintf("http://%s:8080/register", relayIP)

	jsonData := map[string]string{
		"id":   myID.String(),
		"pass": pass,
	}
	jsonBytes, err := json.Marshal(jsonData)
	if err != nil {
		fmt.Println("Не удалось подготовить запрос:", err)
		os.Exit(7)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBytes))
	if err != nil {
		fmt.Println("Не удалось создать запрос:", err)
		os.Exit(7)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	t1 := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Не удалось подключиться к relay:", err)
		os.Exit(7)
	}
	defer resp.Body.Close()

	t2 := time.Now()
	rtt := t2.Sub(t1)

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Не удалось прочитать ответ relay:", err)
		os.Exit(7)
	}
	respBody := string(body)
	parts := strings.Split(respBody, ":")
	if len(parts) < 2 {
		fmt.Printf("Неверный ответ relay: %s\n", respBody)
		os.Exit(7)
	}
	if parts[0] == "OK" {
		serverTime, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			fmt.Printf("Неверная метка времени relay: %v\n", err)
			os.Exit(7)
		}
		actualNetworkNow := time.Unix(serverTime, 0).Add(rtt / 2)
		NetworkTimeOffset = actualNetworkNow.Unix() - t2.Unix()
	} else {
		fmt.Print(respBody)
		os.Exit(7)
	}

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("[Auth] Shield rejected us: %d\n", resp.StatusCode)
		os.Exit(8)
	}
}
func (g *ClientGater) InterceptPeerDial(p peer.ID) bool {
	if p == g.relayID {
		KnockToRelay(g.relayIP, g.myID, g.pass)
	}
	return true
}
func (g *ClientGater) InterceptAddrDial(peer.ID, multiaddr.Multiaddr) bool { return true }
func (g *ClientGater) InterceptAccept(network.ConnMultiaddrs) bool         { return true }
func (g *ClientGater) InterceptSecured(network.Direction, peer.ID, network.ConnMultiaddrs) bool {
	return true
}
func (g *ClientGater) InterceptUpgraded(network.Conn) (bool, control.DisconnectReason) {
	return true, 0
}

func MaintainRelayConn(ctx context.Context, h host.Host, relayInfo *peer.AddrInfo) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if h.Network().Connectedness(relayInfo.ID) != network.Connected {
				// сбрасываем кулдаун перед попыткой переподключения
				if s, ok := h.Network().(*swarm.Swarm); ok {
					s.Backoff().Clear(relayInfo.ID)
				}
				err := h.Connect(ctx, *relayInfo)
				if err != nil {
					fmt.Println(err)
				}
			}
		}
	}
}

func pingRelay(ctx context.Context, h host.Host, relayID peer.ID) {
	ps := ping.NewPingService(h)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			<-ps.Ping(pingCtx, relayID)
			cancel()
		}
	}
}
