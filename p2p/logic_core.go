package p2p

import (
	"fmt"
//	"encoding/json"
	"net"
	"net/http"
	"time"
	"sync"
	"context"
	"sort"
	"os"
	"os/exec"
	"runtime"
	"crypto/tls"
	//"crypto"
	//"crypto/ed25519"
	"math/rand"
	"strings"
	"github.com/libp2p/go-libp2p/core/crypto"
	"market-denet/pb"
	utls "github.com/refraction-networking/utls"
	"market-denet/t2api"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"google.golang.org/protobuf/proto"
	//"github.com/multiformats/go-multiaddr"
)

type LogicCore struct {
	ledger   *Ledger
	myLotID  string
	volume   int
	value    int
	mu             sync.Mutex
	myLastEpoch int64
	lastMyShotTime time.Time
	isExecuting    bool
	licenseFile []byte
	privKey crypto.PrivKey
	bearer string
	number string
}

func InitLogicCore(l *Ledger, myLotID string, vol int, val int, privKey crypto.PrivKey, bearer, number string) *LogicCore {
    licenseFile, err := os.ReadFile("license.bin")
    if err != nil {
        if strings.Contains(err.Error(), "no such file or directory") {
            fmt.Println("Файл лицензии не найден.")
        }
        os.Exit(1)
    }
	return &LogicCore{
		ledger:  l,
		myLotID: myLotID,
		volume:  vol,
		value:   val,
		lastMyShotTime: time.Now().Add(-1 * time.Hour),
		licenseFile: licenseFile,
		privKey: privKey,
		bearer: bearer,
		number: number,
	}
}

// AnalyzeTarget — классифицирует лот на 1-м месте
func (lc *LogicCore) AnalyzeTarget(topLotID string, isBot bool) bool {
	// проверка на Бота Т2
	if isBot {
		return false
	}

	// проверка на союзника
	lc.ledger.mu.RLock()
	_, isMember := lc.ledger.Members[topLotID]
	lc.ledger.mu.RUnlock()

	if isMember {
		// если это наш лот или лот соседа
		return false
	}

	// если это не бот и не участник сети — это посторонний
	fmt.Printf("[Brain] Обнаружен посторонний на 1-м месте: %s\n", topLotID)
	return true
}

// AmITheShooter — решает, является ли наша нода первой в очереди на выстрел
func (lc *LogicCore) AmITheShooter(node *Node) bool {

	queue := lc.ledger.GetSortedQueue(node)

	// если в очереди наша нода единственная, стрелять нельзя
	if len(queue) == 1 {
		return false
	}
    leaderLotID := queue[0]
	if leaderLotID == lc.myLotID {
		fmt.Println("[Brain] Моя очередь!")
		return true
	}
	fmt.Printf("[Brain] Жду очередь. Сейчас лидер: %s\n", leaderLotID)
	return false
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

	ClearScreen()

	fmt.Println("==================================================================")
	fmt.Printf(" СЕГМЕНТ: %d ГБ / %d РУБ | ЛОТ: %s\n", lc.volume, lc.value, lc.myLotID)
	fmt.Println("==================================================================")
	fmt.Printf("%-3s | %-12s | %-4s | %-4s | %-4s | %-6s\n", "#", "PeerID", "T", "R", "W", "P")
	fmt.Println(rendezvous)
	fmt.Println("------------------------------------------------------------------")

	queue := lc.ledger.GetSortedQueue(node)

	lc.ledger.mu.RLock()
	defer lc.ledger.mu.RUnlock()

	for i, lotID := range queue {
		p := lc.ledger.Members[lotID]
		if p == nil { continue }
		
		// пометка "это я"
		prefix := "  "
		if lotID == lc.myLotID {
			prefix = ">>"
		}
        shortPID := p.PeerID.String()
		if len(shortPID) > 8 {
            shortPID = shortPID[len(shortPID)-8:]
        }
        fmt.Println(p.PeerID.String())

		fmt.Printf("%s%-1d | %-12s | %-4d | %-4d | %-4.0f | %-6.2f\n", 
			prefix, i+1, shortPID, p.T, p.R, float64(p.WaitTime), p.PriorityVar)
	}
	fmt.Println("==================================================================")
}

func (lc *LogicCore) checkDutyRole(myID peer.ID, numDutyNodes int, node *Node) bool {
	activePeers := lc.ledger.GetSortedActivePeers(node)
	if len(activePeers) == 0 {
		return true
	}

	// если нод меньше, чем нужно дежурных - дежурят все
	if len(activePeers) <= numDutyNodes {
		return true
	}

	// создаем детерминированный рандом на основе времени
	seed := (time.Now().Unix() + NetworkTimeOffset) / 300 // окно 5 минут
	rng := rand.New(rand.NewSource(seed))

	shuffled := make([]peer.ID, len(activePeers))
	copy(shuffled, activePeers)
	
	// детерминированно сортируем
	sort.Slice(shuffled, func(i, j int) bool {
        return shuffled[i].String() < shuffled[j].String()
    })
    
    // детерминированно рандомизируем
	rng.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	// берем первых N дежурных
	dutyNodes := shuffled[:numDutyNodes]
    
	// проверяем, входит ли нода в этот список
	isDuty := false
	for _, p := range dutyNodes {
		if p.String() == myID.String() {
			isDuty = true
			break
		}
	}

	return isDuty
}

func (lc *LogicCore) broadcastRocketFired(node *Node) {
    lc.ledger.mu.Lock()
    p := lc.ledger.Members[lc.myLotID]
	p.R++ // увеличиваем количество потраченных ракет
	p.T = p.T + int64(rand.Intn(3)) + 1
	p.LastTopTick = time.Now().Unix() + NetworkTimeOffset
	currentEpoch := GetCurrentEpoch()
	// вещаем на всю сеть: "я потратился, мой приоритет упал"
	msg := &pb.NodeMessage{
		Type:        "ROCKET",
		LotId:       lc.myLotID,
		PeerId:      node.Host.ID().String(),
		T:           p.T,
		R:           p.R,
		LastTopTick: p.LastTopTick,
		JoinedAt:    p.JoinedAt,
        LastEpoch:   currentEpoch, 
	}
	lc.ledger.mu.Unlock()
    lc.publish(node.Topic, msg)
	return
}
func (lc *LogicCore) broadcastTopStatus(node *Node, info t2api.LotInfo) {
	msg := &pb.NodeMessage{
		Type:  "TOP",
		LotId: info.ID, // ID того, кого мы увидели на 1 месте
		PeerId: node.Host.ID().String(),  
		IsBot: info.IsBot,
	}
    lc.publish(node.Topic, msg)
}
func (lc *LogicCore) broadcastSyncCorrection(m NodeMessage, node *Node) {
    lc.ledger.mu.RLock()
    p := lc.ledger.Members[m.LotID]
    correction := &pb.NodeMessage{
        Type:        "SYNC",
        LotId:       m.LotID,
        PeerId:      m.PeerID.String(),
        T:           p.T,
        R:           p.R,
        LastEpoch:   p.LastEpoch,
        LastTopTick: p.LastTopTick,
    }
    lc.ledger.mu.RUnlock()
    lc.publish(node.Topic, correction)
}
func (lc *LogicCore) publish(topic *pubsub.Topic, msg *pb.NodeMessage) {
    msg.License = lc.licenseFile

    msg.MsgSig = nil
    payload, err := proto.Marshal(msg)
    if err != nil {
        return
    }

    sig, err := lc.privKey.Sign(payload)
    if err != nil {
        return
    }
    msg.MsgSig = sig

    // 4. Сериализуем с подписью и отправляем
    raw, err := proto.Marshal(msg)
    if err != nil {
        return
    }
    topic.Publish(context.Background(), raw)
}

// StreamConn делает из P2P-стрима обычное сетевое соединение
type StreamConn struct {
	network.Stream
}

func (c *StreamConn) LocalAddr() net.Addr                { return nil }
func (c *StreamConn) RemoteAddr() net.Addr               { return nil }
func (c *StreamConn) SetDeadline(t time.Time) error      { return c.Stream.SetDeadline(t) }
func (c *StreamConn) SetReadDeadline(t time.Time) error  { return c.Stream.SetReadDeadline(t) }
func (c *StreamConn) SetWriteDeadline(t time.Time) error { return c.Stream.SetWriteDeadline(t) }

func (lc *LogicCore) SelectRandomProxy(h host.Host, ctx context.Context) (peer.ID, string) {
	const LighthouseID = "12D3KooWS8gfSiFMenXBPDdyCqEDKsUJZXTby1nENpCjt2hLwS3N"
	lc.ledger.mu.RLock()
	defer lc.ledger.mu.RUnlock()
	var directCandidates []peer.ID
	var transientCandidates []peer.ID
	for _, pid := range h.Network().Peers() {
		if pid == h.ID() || pid.String() == LighthouseID {
			continue
		}
		isMDNMember := false
		for _, p := range lc.ledger.Members {
			if p.PeerID == pid {
				isMDNMember = true
				break
			}
		}
		if !isMDNMember { continue }
		conns := h.Network().ConnsToPeer(pid)
        if len(conns) == 0 { continue }
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
	// 1. Приоритет - прямые (Hole Punching)
	if len(directCandidates) > 0 {
		return directCandidates[rand.Intn(len(directCandidates))], "direct"
	}
	// 2. Фолбек - релейные
	if len(transientCandidates) > 0 {
		return transientCandidates[rand.Intn(len(transientCandidates))], "relay"
	}
	fmt.Println("[Brain] Прокси не найдено. Использую прямой запрос.")
	return "", ""
}
/*func (lc *LogicCore) SelectRandomProxy(h host.Host, ctx context.Context) peer.ID {
	lc.ledger.mu.RLock()
	defer lc.ledger.mu.RUnlock()

    uniquePeers := make(map[peer.ID]bool)
    var candidates []peer.ID
    
	for _, participant := range lc.ledger.Members {
		if participant.PeerID == h.ID() { continue }
		
		if !uniquePeers[participant.PeerID] {
            if h.Network().Connectedness(participant.PeerID) == network.Connected {
                uniquePeers[participant.PeerID] = true
                candidates = append(candidates, participant.PeerID)
            }
        }
    }
	// если в Леджере никого на связи нет
	if len(candidates) == 0 {
		fmt.Println("[Brain] Прокси не найдено. Использую прямой запрос.")
		return "" 
	}
	
	return candidates[rand.Intn(len(candidates))]
}*/
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
    		needsCorrection := lc.ledger.Update(m.LotID, m.PeerID, m.T, m.R, m.JoinedAt, m.LastTopTick, m.LastEpoch)
    		if needsCorrection && m.Type == "ANNOUNCE" {
		        go lc.broadcastSyncCorrection(m, node)
    		}

    	case "TOP":
    		// инкрементируем тики для того, кто сейчас в топе
    		lc.ledger.UpdateTicks(m.LotID)
    		status := lc.AnalyzeTarget(m.LotID, m.IsBot)
            
    		if status && lc.AmITheShooter(node) {
    			lc.mu.Lock()
                if lc.isExecuting {
                    lc.mu.Unlock()
                    return // мы уже в процессе выстрела, игнорируем новые сигналы
                }
				lc.isExecuting = true 
                lc.mu.Unlock()
				go lc.PerformExecution(ctx, node)
			
    		}
    	case "SYNC":
    	    if m.LotID == lc.myLotID {
    	        lc.ledger.mu.Lock()
                p, exists := lc.ledger.Members[lc.myLotID]
    			if exists {
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
}
// wrapWithUTLS - маскировка. превращает "голый" стрим в "мобильное" соединение.
func (lc *LogicCore) wrapWithUTLS(conn net.Conn) (net.Conn, error) {
	// настройка параметров (SNI)
	config := &utls.Config{
		ServerName: t2api.T2Host,
		NextProtos: []string{"http/1.1"},
	}

	// создаем специальный uTLS клиент с профилем Android
	uConn := utls.UClient(conn, config, utls.HelloAndroid_11_OkHttp)

	return uConn, nil
}
// PerformExecution — Главный процесс. Объединяет джиттер, прокси и отчетность.
func (lc *LogicCore) PerformExecution(ctx context.Context, node *Node) {

    defer func() {
		lc.mu.Lock()
		lc.isExecuting = false
		lc.mu.Unlock()
	}()
    lc.mu.Lock()
	if time.Since(lc.lastMyShotTime) < 5*time.Second {
		lc.mu.Unlock()
		return 
	}
	lc.lastMyShotTime = time.Now() // фиксируем попытку сразу
	lc.mu.Unlock()
	wait := rand.Intn(1000)+2000
	fmt.Printf("[Brain] Враг обнаружен! Имитирую раздумья (%d сек)...\n", wait/1000)
    
	select {
    	case <-time.After(time.Duration(wait) * time.Millisecond):
    	case <-ctx.Done():
    		return
	}

	proxyID, mode := lc.SelectRandomProxy(node.Host, ctx)
	var client *http.Client

	if proxyID == "" {
		client = t2api.SharedClient
	} else {
		fmt.Printf("%s[Brain] Использую туннель через: %s (%s)%s\n", ColorRed, proxyID.String()[len(proxyID)-8:], mode, ColorReset)
		client = lc.CreateProxiedClient(ctx, node.Host, proxyID)
	}

	// САМ ВЫСТРЕЛ
	/*err := */t2api.Rocket(client, lc.bearer, lc.number, lc.myLotID)
	var err error
	if err == nil {
	   // fmt.Println("Успешная имитация ракеты!")
		lc.broadcastRocketFired(node)
	} else {
		fmt.Printf("[Brain] Выстрел не удался: %v\n", err)
	}
}

func (lc *LogicCore) CreateProxiedClient(ctx context.Context, h host.Host, proxyPeerID peer.ID) *http.Client {
	return &http.Client{
		Timeout: 20 * time.Second,
		Transport: &http.Transport{
		    TLSNextProto:        make(map[string]func(authority string, c *tls.Conn) http.RoundTripper),
			DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				// открываем P2P стрим к соседу-посреднику
				stream, err := h.NewStream(ctx, proxyPeerID, ProtocolProxy)
				if err != nil {
					return nil, err
				}

				// оборачиваем стрим в uTLS
				uConn, err := lc.wrapWithUTLS(&StreamConn{stream})
				if err != nil {
					stream.Reset()
					return nil, err
				}

				return uConn, nil
			},
			DisableKeepAlives: true, // каждый выстрел — новая маска
		},
	}
}
func (lc *LogicCore) dutyLoop(ctx context.Context, node *Node) {
	ticker := time.NewTicker(3 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:

			// решаем, нужно ли нам стать дежурным принудительно
			isDuty := lc.checkDutyRole(node.Host.ID(), 1, node)
			if isDuty {
				lots, err := t2api.GetTop4IDs(lc.volume, lc.value)
				if err == nil && len(lots) > 0 {
					lc.broadcastTopStatus(node, lots[0])
					m := NodeMessage{
						Type:       "TOP",
						LotID:      lots[0].ID,
						PeerID:     node.Host.ID(),
						IsBot:      lots[0].IsBot,
					}
					lc.HandleMessage(ctx, node, m)
				} else {
				    fmt.Println(err)
				}
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
			me, exists := lc.ledger.Members[lc.myLotID]
			if !exists {
				lc.ledger.mu.Unlock()
				continue
			}
			if currentEpoch > me.LastEpoch {
				me.T /= 2
				me.R /= 2
				me.LastEpoch = currentEpoch
			}
			me.LastSeen = time.Now()
			msg := &pb.NodeMessage{
				Type:        "ANNOUNCE",
				LotId:       lc.myLotID,
				PeerId:      node.Host.ID().String(),
				T:           me.T,
				R:           me.R,
				JoinedAt:    me.JoinedAt,
				LastTopTick: me.LastTopTick,
				LastEpoch:   me.LastEpoch,
			}
			lc.ledger.mu.Unlock()
			lc.publish(node.Topic, msg)
		}
	}
}