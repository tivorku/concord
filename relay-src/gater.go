package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
    "time"
    "sync"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/libp2p/go-libp2p/core/control"
	"github.com/libp2p/go-libp2p/core/host"
)

// WhitelistManager хранит разрешенные IP и PeerID
type WhitelistManager struct {
	mu    sync.RWMutex
	Peers map[peer.ID]bool `json:"peers"`
	IPs   map[string]bool  `json:"ips"`
	path  string
}

func NewWhitelistManager(path string) *WhitelistManager {
	w := &WhitelistManager{
		Peers: make(map[peer.ID]bool),
		IPs:   make(map[string]bool),
		path:  path,
	}
	w.load()
	return w
}

func (w *WhitelistManager) load() {
	data, err := os.ReadFile(w.path)
	if err == nil {
		json.Unmarshal(data, w)
	}
}

func (w *WhitelistManager) save() {
	data, _ := json.Marshal(w)
	os.WriteFile(w.path, data, 0644)
}

func (w *WhitelistManager) Add(id peer.ID, ip string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.Peers[id] = true
	w.IPs[ip] = true
	w.save()
}

type RelayGater struct {
	w *WhitelistManager
}

// InterceptAccept — ГЛАВНЫЙ ЩИТ. Срабатывает до хендшейка.
func (g *RelayGater) InterceptAccept(addrs network.ConnMultiaddrs) bool {
	remoteIP, err := manet.ToIP(addrs.RemoteMultiaddr())
	if err != nil {
		return false
	}

	g.w.mu.RLock()
	defer g.w.mu.RUnlock()

	// пускаем только если IP в вайтлисте
	allowed := g.w.IPs[remoteIP.String()]
	return allowed
}

// InterceptSecured — проверка PeerID после того, как Noise/TLS завершен
func (g *RelayGater) InterceptSecured(dir network.Direction, p peer.ID, addrs network.ConnMultiaddrs) bool {
	if dir == network.DirOutbound {
		return true // разрешаем реле самому звонить кому угодно
	}
	g.w.mu.RLock()
	defer g.w.mu.RUnlock()
	return g.w.Peers[p]
}

// остальные методы просто разрешаем (Dial всегда true, чтобы реле могло искать бутстрапы)
func (g *RelayGater) InterceptPeerDial(p peer.ID) bool { return true }
func (g *RelayGater) InterceptAddrDial(peer.ID, multiaddr.Multiaddr) bool { return true }
func (g *RelayGater) InterceptUpgraded(network.Conn) (bool, control.DisconnectReason) { return true, 0 }

func RunAuthServer(w *WhitelistManager, port string, password string, h host.Host) {
	http.HandleFunc("/register", func(rw http.ResponseWriter, r *http.Request) {
		idStr := r.URL.Query().Get("id")
		pass := r.URL.Query().Get("pass")

		if pass != password {
			http.Error(rw, "Unauthorized", http.StatusUnauthorized)
			return
		}

		pid, err := peer.Decode(idStr)
		if err != nil {
			http.Error(rw, "Invalid PeerID", http.StatusBadRequest)
			return
		}

		// получаем реальный IP клиента
		host, _, _ := net.SplitHostPort(r.RemoteAddr)
		
		w.Add(pid, host)
		h.ConnManager().Protect(pid, "auth-client")
		fmt.Printf("[SHIELD] %s (IP: %s) successfully authorized\n", pid, host)
		serverTime := time.Now().Unix()
		fmt.Fprintf(rw, "OK:%d", serverTime)
	})

	fmt.Printf("[SHIELD] The registration server running on %s\n", port)
	http.ListenAndServe(":"+port, nil)
}