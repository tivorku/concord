package p2p

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/joho/godotenv"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	utls "github.com/refraction-networking/utls"
	"google.golang.org/protobuf/proto"
	"market-denet/pb"
	"market-denet/t2api"
)

type LogicCore struct {
	ledger         *Ledger
	myLotIDs       []string
	volume         int
	value          int
	privKey        crypto.PrivKey
	mu             sync.Mutex
	lastMyShotTime map[string]time.Time
	isExecuting    bool
	shootMu        sync.Mutex
	bearer         string
	number         string
}

func InitLogicCore(l *Ledger, myLotIDs []string, vol int, val int, privKey crypto.PrivKey, bearer, number string, node *Node) *LogicCore {
	lc := &LogicCore{
		ledger:         l,
		myLotIDs:       myLotIDs,
		volume:         vol,
		value:          val,
		privKey:        privKey,
		lastMyShotTime: make(map[string]time.Time),
		isExecuting:    false,
		bearer:         bearer,
		number:         number,
	}
	for _, lotID := range myLotIDs {
		lc.lastMyShotTime[lotID] = time.Now().Add(-1 * time.Hour)
	}
	return lc
}

func (lc *LogicCore) isMyLot(lotID string) bool {
	for _, id := range lc.myLotIDs {
		if id == lotID {
			return true
		}
	}
	return false
}

func (lc *LogicCore) VerifyIncomingMessage(pm *pb.NodeMessage, senderID peer.ID) bool {
	if len(pm.Signature) == 0 {
		fmt.Printf("[SECURITY] Message from %s has no signature\n", senderID)
		return false
	}

	pID, err := peer.Decode(pm.PeerId)
	if err != nil {
		fmt.Printf("[SECURITY] Cannot decode peer ID from message: %v\n", err)
		return false
	}

	pubKey, err := PubKeyFromPeerID(pID)
	if err != nil {
		fmt.Printf("[SECURITY] Cannot extract pubkey from %s: %v\n", pID, err)
		return false
	}

	if !VerifySignature(pubKey, pm, pm.Signature) {
		fmt.Printf("[SECURITY] Invalid signature from %s (author=%s)\n", senderID, pID)
		return false
	}

	return true
}

func (lc *LogicCore) AnalyzeTarget(topLotID string, isBot bool) bool {
	if isBot {
		return false
	}

	if lc.ledger.IsLotKnown(topLotID) {
		return false
	}

	fmt.Printf("[Brain] Обнаружен посторонний на 1-м месте: %s\n", topLotID)
	return true
}

func (lc *LogicCore) GetMyLotsSortedByPriority(node *Node) []Item {
	items := lc.ledger.GetQueueWithMetrics(node)
	var myItems []Item
	for _, item := range items {
		if lc.isMyLot(item.LotID) {
			myItems = append(myItems, item)
		}
	}
	return myItems
}

func (lc *LogicCore) AmITheShooter(node *Node) (string, bool) {
	queue := lc.ledger.GetSortedQueue(node)
	if len(queue) == 0 {
		return "", false
	}

	hasForeign := false
	for _, lotID := range queue {
		if !lc.isMyLot(lotID) {
			hasForeign = true
			break
		}
	}
	if !hasForeign {
		return "", false
	}

	leaderLotID := queue[0]

	if !lc.isMyLot(leaderLotID) {
		return "", false
	}

	myItems := lc.GetMyLotsSortedByPriority(node)
	if len(myItems) == 0 {
		return "", false
	}

	bestLot := myItems[0].LotID

	if leaderLotID == bestLot {
		fmt.Println("[Brain] Моя очередь!")
		return leaderLotID, true
	}

	return "", false
}

func ClearScreen() {
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.Command("cmd", "/c", "cls")
	} else {
		cmd = exec.Command("clear")
	}
	cmd.Stdout = os.Stdout
	cmd.Run()
}

func (lc *LogicCore) ShowDashboard(node *Node, rendezvous string) {
	double_delim := "=================================================================="
	delim := "------------------------------------------------------------------"
	ClearScreen()

	fmt.Println(double_delim)
	fmt.Printf("          СЕГМЕНТ: %d ГБ / %d РУБ | %s\n", lc.volume, lc.value, rendezvous)
	fmt.Println(double_delim)
	fmt.Printf("%-6s | %-12s | %-4s | %-4s | %-4s | %-6s | %-4s\n", "#", "PeerID", "T", "R", "W", "P", "Trust")
	fmt.Println(delim)

	items := lc.ledger.GetQueueWithMetrics(node)

	peerTrust := make(map[string]float64)
	for _, item := range items {
		if item.Trust > peerTrust[item.PeerID] {
			peerTrust[item.PeerID] = item.Trust
		}
	}

	dutyLotID, _ := lc.AmITheShooter(node)

	for i, item := range items {
		prefix := "  "
		suffix := "   "
		if lc.isMyLot(item.LotID) {
			prefix = ">>"
			if item.LotID == dutyLotID {
				suffix = " * "
			}
		}
		shortPID := item.PeerID
		if len(shortPID) > 8 {
			shortPID = shortPID[len(shortPID)-8:]
		}

		trust := peerTrust[item.PeerID]
		fmt.Printf("%s%-1d%s | %-12s | %-4d | %-4d | %-4d | %-6.2f | %-4.0f\n",
			prefix, i+1, suffix, shortPID, item.T, item.R, item.WaitTime, item.Priority, trust)
	}
	fmt.Println(double_delim)
}

func (lc *LogicCore) checkDutyRole(numDutyNodes int, node *Node) bool {
	activePeers := lc.ledger.GetActivePeers(node)
	if len(activePeers) == 0 {
		return true
	}

	if len(activePeers) <= numDutyNodes {
		return true
	}

	// Собираем Trust для каждого пира
	peerTrust := make(map[peer.ID]float64)
	lc.ledger.mu.RLock()
	for _, participants := range lc.ledger.Members {
		for _, p := range participants {
			peerTrust[p.PeerID] += p.TrustScore()
		}
	}
	lc.ledger.mu.RUnlock()

	// Создаём взвешенный список
	type weightedPeer struct {
		peer  peer.ID
		trust float64
	}
	var weighted []weightedPeer
	for _, pid := range activePeers {
		trust := peerTrust[pid]
		if trust < 1.0 {
			trust = 1.0
		}
		for i := 0; i < int(trust*10); i++ {
			weighted = append(weighted, weightedPeer{peer: pid, trust: trust})
		}
	}

	seed := (time.Now().Unix() + NetworkTimeOffset) / 300
	rng := rand.New(rand.NewSource(seed))
	rng.Shuffle(len(weighted), func(i, j int) {
		weighted[i], weighted[j] = weighted[j], weighted[i]
	})

	seen := make(map[peer.ID]bool)
	var dutyNodes []peer.ID
	for _, w := range weighted {
		if !seen[w.peer] {
			seen[w.peer] = true
			dutyNodes = append(dutyNodes, w.peer)
			if len(dutyNodes) >= numDutyNodes {
				break
			}
		}
	}

	for _, p := range dutyNodes {
		if p == node.Host.ID() {
			return true
		}
	}
	return false
}

func (lc *LogicCore) broadcastRocketFired(node *Node, lotID string) {
	lc.ledger.mu.Lock()

	participants := lc.ledger.Members[node.Host.ID().String()]
	var me *Participant
	for _, p := range participants {
		if p.LotID == lotID {
			me = p
			break
		}
	}
	if me == nil {
		return
	}
	me.R++
	me.T = me.T + int64(rand.Intn(3)) + 1
	me.LastTopTick = time.Now().Unix() + NetworkTimeOffset
	currentEpoch := GetCurrentEpoch()
	msg := &pb.NodeMessage{
		Type:        "ROCKET",
		LotId:       lotID,
		PeerId:      node.Host.ID().String(),
		T:           me.T,
		R:           me.R,
		LastTopTick: me.LastTopTick,
		JoinedAt:    me.JoinedAt,
		LastEpoch:   currentEpoch,
	}
	lc.ledger.mu.Unlock()
	lc.publish(node.Topic, msg)
}

func (lc *LogicCore) broadcastTopStatus(node *Node, info t2api.LotInfo) {
	msg := &pb.NodeMessage{
		Type:   "TOP",
		LotId:  info.ID,
		PeerId: node.Host.ID().String(),
		IsBot:  info.IsBot,
	}
	lc.publish(node.Topic, msg)
}

func (lc *LogicCore) broadcastSyncCorrection(m NodeMessage, node *Node) {
	lc.ledger.mu.RLock()
	defer lc.ledger.mu.RUnlock()

	participants := lc.ledger.Members[m.PeerID.String()]
	var p *Participant
	for _, part := range participants {
		if part.LotID == m.LotID {
			p = part
			break
		}
	}
	if p == nil {
		return
	}

	correction := &pb.NodeMessage{
		Type:        "SYNC",
		LotId:       m.LotID,
		PeerId:      m.PeerID.String(),
		T:           p.T,
		R:           p.R,
		LastEpoch:   p.LastEpoch,
		LastTopTick: p.LastTopTick,
	}
	lc.publish(node.Topic, correction)
}

func (lc *LogicCore) publish(topic *pubsub.Topic, msg *pb.NodeMessage) {
	sig, err := SignMessage(lc.privKey, msg)
	if err == nil {
		msg.Signature = sig
	}
	raw, err := proto.Marshal(msg)
	if err != nil {
		return
	}
	topic.Publish(context.Background(), raw)
}

type StreamConn struct {
	network.Stream
}

func (c *StreamConn) LocalAddr() net.Addr                { return nil }
func (c *StreamConn) RemoteAddr() net.Addr               { return nil }
func (c *StreamConn) SetDeadline(t time.Time) error      { return c.Stream.SetDeadline(t) }
func (c *StreamConn) SetReadDeadline(t time.Time) error  { return c.Stream.SetReadDeadline(t) }
func (c *StreamConn) SetWriteDeadline(t time.Time) error { return c.Stream.SetWriteDeadline(t) }

func (lc *LogicCore) SelectRandomProxy(h host.Host, ctx context.Context) (peer.ID, string) {
	lc.ledger.mu.RLock()
	defer lc.ledger.mu.RUnlock()

	var directCandidates []peer.ID
	var transientCandidates []peer.ID

	err := godotenv.Load()
	if err != nil {
		fmt.Println("Ошибка чтения .env файла!")
		os.Exit(4)
	}

	s := os.Getenv("RELAY_ADDR")
	relayAddr, _ := multiaddr.NewMultiaddr(s)
	relayInfo, _ := peer.AddrInfoFromP2pAddr(relayAddr)

	for _, pid := range h.Network().Peers() {
		if pid == h.ID() || pid.String() == relayInfo.ID.String() {
			continue
		}

		_, exists := lc.ledger.Members[pid.String()]
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
		} else {
			transientCandidates = append(transientCandidates, pid)
		}
	}

	if len(directCandidates) > 0 {
		return directCandidates[rand.Intn(len(directCandidates))], "direct"
	}
	if len(transientCandidates) > 0 {
		return transientCandidates[rand.Intn(len(transientCandidates))], "relay"
	}
	fmt.Println("[Brain] Прокси не найдено. Использую прямой запрос.")
	return "", ""
}

func (lc *LogicCore) Run(ctx context.Context, node *Node) {
	go lc.dutyLoop(ctx, node)
	go lc.announceLoop(ctx, node)
}

func (lc *LogicCore) HandleMessage(ctx context.Context, node *Node, m NodeMessage) {
	if m.PeerID != node.Host.ID() {
		fmt.Printf("[Debug] Пришел тип: %s | Лот: %s | От: %s\n", m.Type, m.LotID, m.PeerID)
	}

	switch m.Type {
	case "ANNOUNCE", "ROCKET":
		pubKey, _ := PubKeyFromPeerID(m.PeerID)
		needsCorrection := lc.ledger.Update(m.LotID, m.PeerID, pubKey, m.T, m.R, m.JoinedAt, m.LastTopTick, m.LastEpoch)
		if needsCorrection && m.Type == "ANNOUNCE" {
			go lc.broadcastSyncCorrection(m, node)
		}

	case "TOP":
		lc.ledger.UpdateTicks(m.LotID, m.PeerID)
		status := lc.AnalyzeTarget(m.LotID, m.IsBot)

		if status {
			lotID, isMyTurn := lc.AmITheShooter(node)
			if isMyTurn {
				lc.shootMu.Lock()
				if lc.isExecuting {
					lc.shootMu.Unlock()
					return
				}
				lc.isExecuting = true
				lc.shootMu.Unlock()
				go lc.PerformExecution(ctx, node, lotID)
			}
		}
	case "SYNC":
		lc.ledger.mu.Lock()
		participants := lc.ledger.Members[m.PeerID.String()]
		var p *Participant
		for _, part := range participants {
			if part.LotID == m.LotID {
				p = part
				break
			}
		}
		if p != nil && lc.isMyLot(m.LotID) {
			if m.LastEpoch == GetCurrentEpoch() {
				if m.T > p.T {
					p.T = m.T
				}
				if m.R > p.R {
					p.R = m.R
				}
				if m.LastTopTick > p.LastTopTick {
					p.LastTopTick = m.LastTopTick
				}
			}
		}
		lc.ledger.mu.Unlock()
	}
}

func (lc *LogicCore) wrapWithUTLS(conn net.Conn) (net.Conn, error) {
	config := &utls.Config{
		ServerName: t2api.T2Host,
		NextProtos: []string{"http/1.1"},
	}

	uConn := utls.UClient(conn, config, utls.HelloAndroid_11_OkHttp)

	return uConn, nil
}

func (lc *LogicCore) PerformExecution(ctx context.Context, node *Node, lotID string) {
	defer func() {
		lc.shootMu.Lock()
		lc.isExecuting = false
		lc.shootMu.Unlock()
	}()

	lc.mu.Lock()
	if time.Since(lc.lastMyShotTime[lotID]) < 5*time.Second {
		lc.mu.Unlock()
		return
	}
	lc.lastMyShotTime[lotID] = time.Now()
	lc.mu.Unlock()

	wait := rand.Intn(1000) + 2000
	fmt.Printf("[Brain] Враг обнаружен! Имитирую раздумья (%d сек)...\n", wait/1000)

	select {
	case <-time.After(time.Duration(wait) * time.Millisecond):
	case <-ctx.Done():
		return
	}

	proxyID, mode := lc.SelectRandomProxy(node.Host, ctx)

	if proxyID == "" {
		err := t2api.Rocket(t2api.SharedClient, lc.bearer, lc.number, lotID)
		if err == nil {
			lc.broadcastRocketFired(node, lotID)
		} else {
			fmt.Printf("[Brain] Выстрел не удался: %v\n", err)
		}
		return
	}

	fmt.Printf("%s[Brain] Использую туннель через: %s (%s)%s\n", ColorRed, proxyID.String()[len(proxyID.String())-8:], mode, ColorReset)

	var err error
	for attempt := 1; attempt <= 2; attempt++ {
		client := lc.CreateProxiedClient(ctx, node.Host, proxyID)
		err = t2api.Rocket(client, lc.bearer, lc.number, lotID)

		if err == nil {
			break
		}

		isRateLimit := strings.Contains(err.Error(), "429")

		if attempt == 2 || !isRateLimit {
			fmt.Printf("[Brain] Прямой запрос...\n")
			err = t2api.Rocket(t2api.SharedClient, lc.bearer, lc.number, lotID)
			break
		}

		fmt.Printf("[Brain] Прокси рейтлимит. Жду 3 сек...\n")
		select {
		case <-time.After(3 * time.Second):
		case <-ctx.Done():
			return
		}
	}
	err = nil
	if err == nil {
		lc.broadcastRocketFired(node, lotID)
	} else {
		fmt.Printf("[Brain] Выстрел не удался: %v\n", err)
	}
}

func (lc *LogicCore) CreateProxiedClient(ctx context.Context, h host.Host, proxyPeerID peer.ID) *http.Client {
	return &http.Client{
		Timeout: 20 * time.Second,
		Transport: &http.Transport{
			TLSNextProto: make(map[string]func(authority string, c *tls.Conn) http.RoundTripper),
			DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				stream, err := h.NewStream(ctx, proxyPeerID, ProtocolProxy)
				if err != nil {
					return nil, err
				}

				uConn, err := lc.wrapWithUTLS(&StreamConn{stream})
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

func (lc *LogicCore) dutyLoop(ctx context.Context, node *Node) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			isDuty := lc.checkDutyRole(1, node)
			if isDuty {
				t2api.GetTop4IDsAsync(lc.volume, lc.value, func(lots []t2api.LotInfo, err error) {
					if err != nil {
						fmt.Println(err)
						return
					}
					if len(lots) > 0 {
						lc.broadcastTopStatus(node, lots[0])
						m := NodeMessage{
							Type:   "TOP",
							LotID:  lots[0].ID,
							PeerID: node.Host.ID(),
							IsBot:  lots[0].IsBot,
						}
						lc.HandleMessage(ctx, node, m)
					}
				})
			}
		}
	}
}

func (lc *LogicCore) announceLoop(ctx context.Context, node *Node) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			currentEpoch := GetCurrentEpoch()
			lc.ledger.mu.Lock()

			participants := lc.ledger.Members[node.Host.ID().String()]
			for _, me := range participants {
				if currentEpoch > me.LastEpoch {
					me.T /= 2
					me.R /= 2
					me.LastEpoch = currentEpoch
				}
				me.LastSeen = time.Now()

				msg := &pb.NodeMessage{
					Type:        "ANNOUNCE",
					LotId:       me.LotID,
					PeerId:      node.Host.ID().String(),
					T:           me.T,
					R:           me.R,
					JoinedAt:    me.JoinedAt,
					LastTopTick: me.LastTopTick,
					LastEpoch:   me.LastEpoch,
				}
				lc.publish(node.Topic, msg)
			}

			lc.ledger.mu.Unlock()
		}
	}
}
