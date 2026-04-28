package p2p

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	utls "github.com/refraction-networking/utls"
	"market-denet/t2api"
)

type Shooter struct {
	bearer         string
	number         string
	lastMyShotTime map[string]time.Time
	isExecuting    bool
	mu             sync.Mutex
	shootMu        sync.Mutex
	ledger         *Ledger
}

func NewShooter(bearer, number string, myLotIDs []string, ledger *Ledger) *Shooter {
	s := &Shooter{
		bearer:         bearer,
		number:         number,
		lastMyShotTime: make(map[string]time.Time),
		ledger:         ledger,
	}
	for _, lotID := range myLotIDs {
		s.lastMyShotTime[lotID] = time.Now().Add(-1 * time.Hour)
	}
	return s
}

func (s *Shooter) CanShoot(lotID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if time.Since(s.lastMyShotTime[lotID]) < 5*time.Second {
		return false
	}
	s.lastMyShotTime[lotID] = time.Now()
	return true
}

func (s *Shooter) TryLock() bool {
	s.shootMu.Lock()
	defer s.shootMu.Unlock()
	if s.isExecuting {
		return false
	}
	s.isExecuting = true
	return true
}

func (s *Shooter) Unlock() {
	s.shootMu.Lock()
	defer s.shootMu.Unlock()
	s.isExecuting = false
}

func (s *Shooter) PerformExecution(ctx context.Context, node *Node, lotID string, broadcaster *Broadcaster) {
	defer s.Unlock()

	if !s.CanShoot(lotID) {
		return
	}

	wait := rand.Intn(1000) + 2000
	fmt.Printf("[Brain] Враг обнаружен! Имитирую раздумья (%d сек)...\n", wait/1000)

	select {
	case <-time.After(time.Duration(wait) * time.Millisecond):
	case <-ctx.Done():
		return
	}

	proxyID := s.SelectRandomProxy(node.Host)

	if proxyID == "" {
		err := t2api.Rocket(t2api.SharedClient, s.bearer, s.number, lotID)
		if err == nil {
			broadcaster.broadcastRocketFired(node.Topic, lotID)
		} else {
			fmt.Printf("[Brain] Выстрел не удался: %v\n", err)
		}
		return
	}

	fmt.Printf("%s[Brain] Использую туннель через: %s%s\n", ColorRed, proxyID.String()[len(proxyID.String())-8:], ColorReset)
	

	var err error
	for attempt := 1; attempt <= 2; attempt++ {
		client := s.CreateProxiedClient(ctx, node.Host, proxyID)
		err = t2api.Rocket(client, s.bearer, s.number, lotID)

		if err == nil {
			break
		}

		isRateLimit := strings.Contains(err.Error(), "429")
		if attempt == 2 || !isRateLimit {
			fmt.Printf("[Brain] Прямой запрос...\n")
			err = t2api.Rocket(t2api.SharedClient, s.bearer, s.number, lotID)
			break
		}

		fmt.Printf("[Brain] Прокси рейт-лимит. Жду 3 сек...\n")
		select {
		case <-time.After(3 * time.Second):
		case <-ctx.Done():
			return
		}
	}
	if err == nil {
		broadcaster.broadcastRocketFired(node.Topic, lotID)
	} else {
		fmt.Printf("[Brain] Выстрел не удался: %v\n", err)
	}
}

func (s *Shooter) SelectRandomProxy(h host.Host) peer.ID {
	s.ledger.mu.RLock()
	defer s.ledger.mu.RUnlock()

	var directCandidates []peer.ID

	sRelayAddr := os.Getenv("RELAY_ADDR")
	if sRelayAddr == "" {
		fmt.Println("[Brain] RELAY_ADDR не установлен.")
		return ""
	}
	relayAddr, err := multiaddr.NewMultiaddr(sRelayAddr)
	if err != nil {
		fmt.Printf("[Brain] Неверный RELAY_ADDR: %v\n", err)
		return ""
	}
	relayInfo, err := peer.AddrInfoFromP2pAddr(relayAddr)
	if err != nil {
		fmt.Printf("[Brain] Неверный адрес relay: %v\n", err)
		return ""
	}

	for _, pid := range h.Network().Peers() {
		if pid == h.ID() || pid.String() == relayInfo.ID.String() {
			continue
		}

		_, exists := s.ledger.Members[pid.String()]
		if !exists {
			continue
		}

		conns := h.Network().ConnsToPeer(pid)
		if len(conns) == 0 {
			continue
		}

		isDirectlyConnected := false
		for _, conn := range conns {
			addr := conn.RemoteMultiaddr()
			hasRelayInAddr := false
			for _, proto := range addr.Protocols() {
				if proto.Code == 290 {
					hasRelayInAddr = true
					break
				}
			}
			if !hasRelayInAddr {
				isDirectlyConnected = true
				break
			}
		}
		if isDirectlyConnected {
			directCandidates = append(directCandidates, pid)
		}
	}

	if len(directCandidates) > 0 {
		return directCandidates[rand.Intn(len(directCandidates))]
	}
	fmt.Println("[Brain] Прокси не найдено. Использую прямой запрос.")
	return ""
}

func (s *Shooter) CreateProxiedClient(ctx context.Context, h host.Host, proxyPeerID peer.ID) *http.Client {
	return &http.Client{
		Timeout: 20 * time.Second,
		Transport: &http.Transport{
			TLSNextProto: make(map[string]func(authority string, c *tls.Conn) http.RoundTripper),
			DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				stream, err := h.NewStream(ctx, proxyPeerID, ProtocolProxy)
				if err != nil {
					return nil, err
				}

				uConn, err := wrapWithUTLS(&StreamConn{stream})
				if err != nil {
					stream.Reset()
					return nil, err
				}

				return uConn, nil
			},
			DisableKeepAlives: true,
		},
	}
}

type StreamConn struct {
	network.Stream
}

func (c *StreamConn) LocalAddr() net.Addr                { return nil }
func (c *StreamConn) RemoteAddr() net.Addr               { return nil }
func (c *StreamConn) SetDeadline(t time.Time) error      { return c.Stream.SetDeadline(t) }
func (c *StreamConn) SetReadDeadline(t time.Time) error  { return c.Stream.SetReadDeadline(t) }
func (c *StreamConn) SetWriteDeadline(t time.Time) error { return c.Stream.SetWriteDeadline(t) }

func wrapWithUTLS(conn net.Conn) (net.Conn, error) {
	config := &utls.Config{
		ServerName: t2api.T2Host,
		NextProtos: []string{"http/1.1"},
	}

	uConn := utls.UClient(conn, config, utls.HelloAndroid_11_OkHttp)

	return uConn, nil
}
