package p2p

import (
	"fmt"
	//"crypto/sha256"
	"encoding/json"
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
	"math/rand"
	
	utls "github.com/refraction-networking/utls"
	"market-denet/t2api"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

type Strategist struct {
	ledger   *Ledger
	myLotID  string
	volume   int
	value    int
	mu             sync.Mutex
	lastMyShotTime time.Time
	isExecuting    bool
}

func NewStrategist(l *Ledger, myLotID string, vol int, val int) *Strategist {
	return &Strategist{
		ledger:  l,
		myLotID: myLotID,
		volume:  vol,
		value:   val,
		lastMyShotTime: time.Now().Add(-1 * time.Hour),
	}
}

// AnalyzeTarget — классифицирует лот на 1-м месте
func (s *Strategist) AnalyzeTarget(topLotID string, isBot bool) bool {
	// проверка на Бота Т2
	if isBot {
		return false
	}

	// проверка на союзника
	s.ledger.mu.RLock()
	_, isMember := s.ledger.Members[topLotID]
	s.ledger.mu.RUnlock()

	if isMember {
		// если это наш лот или лот соседа
		return false
	}

	// если это не бот и не участник сети — это посторонний
	fmt.Printf("[СТРАТЕГ] Обнаружен посторонний на 1-м месте: %s\n", topLotID)
	return true
}

// AmITheShooter — решает, является ли наша нода первой в очереди на выстрел
func (s *Strategist) AmITheShooter() bool {

	queue := s.ledger.GetSortedQueue()

	// если в очереди наша нода единственная, стрелять нельзя
	if len(queue) == 1 {
		return false
	}
    leaderLotID := queue[0]
	if leaderLotID == s.myLotID {
		fmt.Println("[СТРАТЕГ] Моя очередь! Я — лидер очереди.")
		return true
	}
	fmt.Printf("[СТРАТЕГ] Жду очередь. Сейчас лидер: %s\n", leaderLotID)
	return false
}

func (l *Ledger) GetSortedActivePeers() []peer.ID {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var active []peer.ID
	for _, p := range l.Members {
		// считаем живыми тех, кто подавал знак последние 60 секунд
		if time.Since(p.LastSeen) < 1*time.Minute {
			active = append(active, p.PeerID)
		}
	}

	// сортировка обязательна для детерминизма!
	sort.Slice(active, func(i, j int) bool {
		return active[i].String() < active[j].String()
	})

	return active
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
func (s *Strategist) ShowDashboard() {

	ClearScreen()

	fmt.Println("==================================================================")
	fmt.Printf(" СЕГМЕНТ: %d ГБ / %d РУБ | ЛОТ: %s\n", s.volume, s.value, s.myLotID)
	fmt.Println("==================================================================")
	fmt.Printf("%-3s | %-12s | %-4s | %-4s | %-6s | %s\n", "#", "PeerID", "T", "R", "P", "Статус")
	fmt.Println("------------------------------------------------------------------")

	queue := s.ledger.GetSortedQueue()

	s.ledger.mu.RLock()
	defer s.ledger.mu.RUnlock()

	for i, lotID := range queue {
		p := s.ledger.Members[lotID]
		if p == nil { continue }
		
		// пометка "это я"
		prefix := "  "
		if lotID == s.myLotID {
			prefix = ">>"
		}

		// пометка карантина
		status := "Active"
		/*if time.Now().Unix() - p.JoinedAt < 1200 {
			status = "Quarantine"
			priority += 1000000.0
		}*/
        shortPID := p.PeerID.String()
		if len(shortPID) > 8 {
            shortPID = shortPID[len(shortPID)-8:]
        }

		fmt.Printf("%s%-1d | %-12s | %-4d | %-4d | %-6.2f | %s\n", 
			prefix, i+1, shortPID, p.T, p.R, p.PriorityVar, status)
	}
	fmt.Println("==================================================================")
}

func (s *Strategist) checkDutyRole(myID peer.ID, numDutyNodes int) bool {
	activePeers := s.ledger.GetSortedActivePeers()
	if len(activePeers) == 0 {
		return true
	}

	// если нод меньше, чем нужно дежурных - дежурят все
	if len(activePeers) <= numDutyNodes {
		return true
	}

	// создаем детерминированный рандом на основе времени
	seed := time.Now().Unix() / 300 // окно 5 минут
	rng := rand.New(rand.NewSource(seed))
	// делаем копию списка и перемешиваем её
	shuffled := make([]peer.ID, len(activePeers))
	copy(shuffled, activePeers)
	
	sort.Slice(shuffled, func(i, j int) bool {
        return shuffled[i].String() < shuffled[j].String()
    })
	
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

func (s *Strategist) broadcastRocketFired(topic *pubsub.Topic, myPeerID peer.ID, myT int64, myR int, joinedAt int64, lastTopTick int64) {
	msg := NodeMessage{
		Type:     "ROCKET_FIRED",
		LotID:    s.myLotID,
		PeerID:   myPeerID,
		T:        myT,
		R:        myR,
		LastTopTick: lastTopTick,
		JoinedAt: joinedAt,
	}
	s.publish(topic, msg)
}
func (s *Strategist) broadcastTopStatus(topic *pubsub.Topic, info t2api.LotInfo, myPeerID peer.ID) {
	msg := NodeMessage{
		Type:  "TOP_STATUS",
		LotID: info.ID, // ID того, кого мы увидели на 1 месте
		PeerID: myPeerID,  
		IsBot: info.IsBot, // бот это или нет
	}
    s.publish(topic, msg)
    
}
func (s *Strategist) publish(topic *pubsub.Topic, msg NodeMessage) {
	raw, _ := json.Marshal(msg)
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

func (s *Strategist) SelectRandomProxy(h host.Host, ctx context.Context) peer.ID {
	s.ledger.mu.RLock()
	defer s.ledger.mu.RUnlock()

	// создаем список потенциальных прокси
    uniquePeers := make(map[peer.ID]bool)
    var candidates []peer.ID
    
	for _, participant := range s.ledger.Members {
		// игнорируем себя
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
		fmt.Println("[MDN] Прокси не найдено. Использую прямой запрос.")
		return "" 
	}
	
	return candidates[rand.Intn(len(candidates))]
}
func (s *Strategist) Run(ctx context.Context, mn *MarketNode, bearer, number string) {
	// запускаем фоновый цикл дежурного
	go s.dutyLoop(ctx, mn)
	// запускаем фоновый цикл анонсера (говорим сети, что мы живы)
	go s.announceLoop(ctx, mn)
}
func (s *Strategist) HandleMessage(ctx context.Context, mn *MarketNode, m NodeMessage, bearer, number string) {
    fmt.Printf("[DEBUG] Пришел тип: %s | Лот: %s | От: %s\n", m.Type, m.LotID, m.PeerID)
	switch m.Type {
    	case "ANNOUNCE", "ROCKET_FIRED":
    	    s.ledger.mu.Lock()
    	    if m.GlobalTick > s.ledger.GlobalTick {
    	        s.ledger.GlobalTick = m.GlobalTick
    	    }
    	    s.ledger.mu.Unlock()
    		s.ledger.Update(m.LotID, m.PeerID, m.T, m.R, m.JoinedAt, m.LastTopTick, m.GlobalTick)
    
    	case "TOP_STATUS":
    		// инкрементируем тики для того, кто сейчас в топе
    		s.ledger.UpdateTicks(m.LotID)
    		status := s.AnalyzeTarget(m.LotID, m.IsBot)
            
    		if status && s.AmITheShooter() {
    			s.mu.Lock()
                if s.isExecuting {
                    s.mu.Unlock()
                    return // мы уже в процессе выстрела, игнорируем новые сигналы
                }
				s.isExecuting = true 
                s.mu.Unlock()
				go s.PerformExecution(ctx, mn, bearer, number)
			
    		}
	}
}
// wrapWithUTLS - маскировка. превращает "голый" стрим в "мобильное" соединение.
func (s *Strategist) wrapWithUTLS(conn net.Conn) (net.Conn, error) {
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
func (s *Strategist) PerformExecution(ctx context.Context, mn *MarketNode, bearer, number string) {

    defer func() {
		s.mu.Lock()
		s.isExecuting = false
		s.mu.Unlock()
	}()
    s.mu.Lock()
	if time.Since(s.lastMyShotTime) < 5*time.Second {
		s.mu.Unlock()
		// мы недавно стреляли. пропускаем этот цикл, чтобы не злить антифрод.
		fmt.Println("Уже недавно стреляли. Ждём 5 секунд")
		return 
	}
	s.lastMyShotTime = time.Now() // фиксируем попытку сразу
	s.mu.Unlock()
	wait := rand.Intn(1000)+2000
	fmt.Printf("[MDN] Враг обнаружен! Имитирую раздумья (%d сек)...\n", wait/1000)
    
	select {
	case <-time.After(time.Duration(wait) * time.Millisecond):
	case <-ctx.Done():
		return
	}

	// выбор пути (прокси или прямой выстрел)
	proxyID := s.SelectRandomProxy(mn.Host, ctx)
	var client *http.Client

	if proxyID == "" {
		client = t2api.SharedClient
	} else {
		fmt.Printf("[MDN] Использую стелс-туннель через: %s\n", proxyID.String()[len(proxyID)-8:])
		client = s.CreateProxiedClient(ctx, mn.Host, proxyID)
	}

	// САМ ВЫСТРЕЛ
	err := t2api.Rocket(client, bearer, number, s.myLotID)
	//var err error = nil
	if err == nil {
	   // fmt.Println("Успешная имитация ракеты!")
		
		// синхронизация своего состояния
		s.ledger.mu.Lock()
		if p, ok := s.ledger.Members[s.myLotID]; ok {
			p.R++ // увеличиваем количество потраченных ракет
			p.T = p.T + int64(rand.Intn(3)) + 1
			p.LastTopTick = s.ledger.GlobalTick
			// вещаем на всю сеть: "я потратился, мой приоритет упал"
			s.broadcastRocketFired(mn.Topic, mn.Host.ID(), p.T, p.R, p.JoinedAt, p.LastTopTick)
		}
		s.ledger.mu.Unlock()
	} else {
		fmt.Printf("[MDN] Выстрел не удался: %v\n", err)
	}
}

func (s *Strategist) CreateProxiedClient(ctx context.Context, h host.Host, proxyPeerID peer.ID) *http.Client {
	return &http.Client{
		Timeout: 20 * time.Second,
		Transport: &http.Transport{
		    TLSNextProto:        make(map[string]func(authority string, c *tls.Conn) http.RoundTripper),
			ForceAttemptHTTP2:   false,
			DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				// открываем P2P стрим к соседу-посреднику
				stream, err := h.NewStream(ctx, proxyPeerID, ProtocolProxy)
				if err != nil {
					return nil, err
				}

				// оборачиваем стрим в uTLS
				uConn, err := s.wrapWithUTLS(&StreamConn{stream})
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
func (s *Strategist) dutyLoop(ctx context.Context, mn *MarketNode) {
	ticker := time.NewTicker(3 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:

			// решаем, нужно ли нам стать дежурным принудительно
			isDuty := s.checkDutyRole(mn.Host.ID(), 3)
			if isDuty {
				lots, err := t2api.GetTop4IDs(s.volume, s.value)
				if err == nil && len(lots) > 0 {
					s.broadcastTopStatus(mn.Topic, lots[0], mn.Host.ID())
				} else {
				    fmt.Println(err)
				}
			}
		}
	}
}
func (s *Strategist) announceLoop(ctx context.Context, mn *MarketNode) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.ledger.mu.RLock()
			me, exists := s.ledger.Members[s.myLotID]
			if !exists {
				s.ledger.mu.RUnlock()
				continue
			}
			s.ledger.GlobalTick++
			// обновляем себя в своем же леджере
			myT := me.T
			myR := me.R
			myL := me.LastTopTick
			myJ := me.JoinedAt
			myG := s.ledger.GlobalTick
			s.ledger.mu.RUnlock()
			s.ledger.Update(s.myLotID, mn.Host.ID(), myT, myR, myJ, myL, myG)
			
			msg := NodeMessage{
				Type:     "ANNOUNCE",
				LotID:    s.myLotID,
				PeerID:   mn.Host.ID(),
				T:        me.T,
				R:        me.R,
				JoinedAt: me.JoinedAt,
				LastTopTick: me.LastTopTick,
				GlobalTick: s.ledger.GlobalTick,
			}
			s.publish(mn.Topic, msg)
		}
	}
}