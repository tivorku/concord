package p2p

import (
	"context"
	"encoding/hex"
	"encoding/json"
    "io"
	"net"
	"time"
	"os"
	"fmt"
	"strconv"
	"sync"
	"strings"
	"crypto/sha256"
	"crypto/ed25519"
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
	"github.com/libp2p/go-libp2p/core/protocol"
	"net/http"
	"market-denet/pb"
	"google.golang.org/protobuf/proto"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	"github.com/libp2p/go-libp2p/core/control"
	manet "github.com/multiformats/go-multiaddr/net"
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
var (
    relayAddr, _ = multiaddr.NewMultiaddr("/ip4/144.31.152.128/tcp/42954/p2p/12D3KooWS8gfSiFMenXBPDdyCqEDKsUJZXTby1nENpCjt2hLwS3N")
    relayInfo, _ = peer.AddrInfoFromP2pAddr(relayAddr)
    relayIP = "144.31.152.128"
    relayID, _ = peer.Decode("12D3KooWS8gfSiFMenXBPDdyCqEDKsUJZXTby1nENpCjt2hLwS3N")
    sharedPass = "2a35442281f13052136c53589ae2f51b")
const ProtocolProxy protocol.ID = "/mdn/proxy/1.0.0"
type NodeMessage struct {
	Type   string `json:"type"`  // ANNOUNCE, ROCKET, TOP, SYNC
	LotID  string `json:"lot_id"`
	PeerID peer.ID `json:"peer_id"`
	T      int64  `json:"t"` // тики
	R      int64    `json:"r"` // ракеты
	JoinedAt int64 `json:"joined_at"`
	LastEpoch int64 `json:"last_epoch_id"`
	LastTopTick int64 `json:"last_top_tick"`
	IsBot  bool   `json:"is_bot"`
}
type Node struct {
	Host    host.Host
	Topic   *pubsub.Topic
	Ledger  *Ledger
	Ctx     context.Context
}
func InitHost(ctx context.Context, privKey crypto.PrivKey) (host.Host, error) {
    var h host.Host
    var err error
    
    myID, _ := peer.IDFromPrivateKey(privKey)
    myGater := &ClientGater{
        relayID: relayID,
        relayIP: relayIP,
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
            if manet.IsPublicAddr(addr) && !manet.IsIPLoopback(addr) { filtered = append(filtered, addr) }
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

	go MaintainRelayConn(ctx, h, *relayInfo)
    
	kad, err := dht.New(ctx, h, dht.Mode(dht.ModeClient), dht.BootstrapPeers(*relayInfo))
	if err != nil {
		panic(err)
	}
	localDHT, err := dht.New(ctx, h, dht.Mode(dht.ModeClient), dht.BootstrapPeers(*relayInfo), dht.ProtocolPrefix("/mdenet"))
	if err != nil {
		panic(err)
	}

    if err = kad.Bootstrap(ctx); err != nil { panic(err) }
    if err = localDHT.Bootstrap(ctx); err != nil { panic(err) }
	localRouting := routing.NewRoutingDiscovery(localDHT)
	go func () {
	    for {
    	    time.Sleep(2 * time.Second)
    	    util.Advertise(ctx, localRouting, rendezvous)
	    }
	}()
    go func() {
        for {
            fmt.Println("\n[Debug] Текущие адреса:")
                for _, addr := range h.Addrs() {
                    fmt.Println(addr)
                }
            conns := h.Network().Conns()
            if h.Network().Connectedness(relayID) == network.Connected {
                fmt.Println("[Debug] Есть соединение с реле")
            }
            fmt.Printf("[Debug] Всего сетевых соединений: %d\n", len(conns))
            fmt.Println("[Debug] Разница во времени:", NetworkTimeOffset)
            time.Sleep(3 * time.Second)
        }
    }()
	go func() {
		for {
			peersChan, err := localRouting.FindPeers(ctx, rendezvous)
			if err != nil {
				return
			}
            for p := range peersChan {
                if p.ID == h.ID() { continue }
                go func(peerInfo peer.AddrInfo) {
                    // если адресов нет - ищем их
                    if len(peerInfo.Addrs) == 0 {
                        found, err := localDHT.FindPeer(ctx, peerInfo.ID)
                        if err != nil { return }
                        peerInfo = found
                    }
                    if h.Network().Connectedness(peerInfo.ID) != network.Connected {
                        /*err := */h.Connect(ctx, peerInfo)
                        /*if err == nil {
                            fmt.Println("Найден сосед: ", peerInfo.ID)
                        }*/
                    }
                }(p)      
            }
			time.Sleep(10*time.Second)
		}
	}()
	return
}
func StartPubSub(ctx context.Context, h host.Host, topicName string, l *Ledger, lc *LogicCore) (*pubsub.Topic, error) {
    params := pubsub.DefaultGossipSubParams()
    params.HeartbeatInterval = 500 * time.Millisecond
    params.PruneBackoff = 10 * time.Second // Уменьшаем до 10 секунд (вместо 60)
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
            if err != nil { return }
            senderPID := msg.ReceivedFrom
            if senderPID == h.ID() { continue }
            var pm pb.NodeMessage
            if err := proto.Unmarshal(msg.Data, &pm); err != nil { continue }
            if !verifyMessage(&pm) { continue }
            pID, _ := peer.Decode(pm.PeerId)

            m := NodeMessage{
                Type:        pm.Type,
                LotID:       pm.LotId,
                PeerID:      pID,
                T:           pm.T,
                R:           pm.R,
                JoinedAt:    pm.JoinedAt,
                LastEpoch:   pm.LastEpoch,
                LastTopTick: pm.LastTopTick,
                IsBot:       pm.IsBot,
            }
            lc.HandleMessage(ctx, node, m)
        }
    }()
    return topic, nil
}
// Кэш — чтобы не проверять лицензию каждый раз
var verifiedPeers sync.Map

func verifyMessage(pm *pb.NodeMessage) bool {
    peerID, err := peer.Decode(pm.PeerId)
    if err != nil { return false }

    // 1. Извлекаем и обнуляем подпись
    sig := pm.MsgSig
    pm.MsgSig = nil
    payload, _ := proto.Marshal(pm)
    pm.MsgSig = sig // возвращаем обратно

    // 2. Проверяем подпись сообщения (владеет ли он этим PeerID)
    pubKey, err := peerID.ExtractPublicKey()
    if err != nil { return false }
    ok, err := pubKey.Verify(payload, sig)
    if !ok || err != nil { return false }

    // 3. Лицензию проверяем только один раз
    if _, cached := verifiedPeers.Load(peerID); cached {
        return true
    }

    // 4. Первое сообщение — полная проверка лицензии
    if !VerifyLicense(pm.License, peerID) {
        return false
    }

    verifiedPeers.Store(peerID, true)
    return true
}

func VerifyLicense(fileData []byte, p peer.ID) bool {
    if len(fileData) < 65 { return false }

    sig     := fileData[:64]
    rawJSON := fileData[64:]

    // Подпись корневым ключом
    if !ed25519.Verify(PubKey(), rawJSON, sig) { return false }

    var payload struct {
        ID  string `json:"id"`
        Exp int64  `json:"exp"`
    }
    if json.Unmarshal(rawJSON, &payload) != nil { return false }

    // Привязка к PeerID + не истекла
    return payload.ID == p.String() && payload.Exp > time.Now().Unix()
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
	// мы говорим хосту: "Если кто-то обратится по этому ID протокола — запусти эту функцию"
	node.Host.SetStreamHandler(ProtocolProxy, func(stream network.Stream) {
	    fmt.Printf("%s[PROXY] Входящий запрос от %s! Перенаправляю на Т2...%s\n", ColorRed, stream.Conn().RemotePeer(), ColorReset)
		defer stream.Close()

		// используем net.Dial, потому что это выход из P2P-сети в обычный интернет
		conn, err := net.DialTimeout("tcp", t2api.T2FullHost, 10*time.Second)
		if err != nil {
			fmt.Printf("[Proxy] Ошибка соединения с T2: %v\n", err)
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

// ActivateLicense — активирует сеть или отправляет в изоляцию
func PubKey() ed25519.PublicKey {
    pubKeyBytes, _ := hex.DecodeString("89114cf009d8be62dfdae1fd4125bcd891b31c80a2b3eab91d5832904357b10e")
    return ed25519.PublicKey(pubKeyBytes)
}
func (node *Node) GetProtocolID(volume, value int) string {
	h := sha256.New()
	fmt.Fprintf(h, "%d-%d", volume, value)
	return fmt.Sprintf("/mdn/v0.1/%x", h.Sum(nil)[:6])
}
func KnockToRelay(relayIP string, myID peer.ID, pass string) error {
	url := fmt.Sprintf("http://%s:8080/register?id=%s&pass=%s", relayIP, myID.String(), pass)
	t1 := time.Now()
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	t2 := time.Now()
	rtt := t2.Sub(t1)
	body, _ := io.ReadAll(resp.Body)
	respBody := string(body)
    parts := strings.Split(respBody, ":")
    if parts[0] == "OK" {
        serverTime, _ := strconv.ParseInt(parts[1], 10, 64)
        // Вычисляем, насколько наши часы врут относительно реле
        actualNetworkNow := time.Unix(serverTime, 0).Add(rtt / 2)
        NetworkTimeOffset = actualNetworkNow.Unix() - t2.Unix()
    }
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("[Auth] Shield rejected us")
	}
	return nil
}
func (g *ClientGater) InterceptPeerDial(p peer.ID) bool {
    if p == g.relayID {
        KnockToRelay(g.relayIP, g.myID, g.pass)
    }
    return true
}
func (g *ClientGater) InterceptAddrDial(peer.ID, multiaddr.Multiaddr) bool { return true }
func (g *ClientGater) InterceptAccept(network.ConnMultiaddrs) bool { return true }
func (g *ClientGater) InterceptSecured(network.Direction, peer.ID, network.ConnMultiaddrs) bool { return true }
func (g *ClientGater) InterceptUpgraded(network.Conn) (bool, control.DisconnectReason) { return true, 0 }

func MaintainRelayConn(ctx context.Context, h host.Host, relayInfo peer.AddrInfo) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
            if h.Network().Connectedness(relayInfo.ID) != network.Connected {
                // мы оффлайн - реконнект
                if s, ok := h.Network().(*swarm.Swarm); ok {
                    s.Backoff().Clear(relayInfo.ID)
                }
                err := h.Connect(ctx, relayInfo)
                if err != nil {
                    fmt.Println(err)
                }
            }
		}
	}
}