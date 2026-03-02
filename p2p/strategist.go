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

// Константы для классификации действий
const (
	ActionRelax  = "RELAX"  // Наш лот в топе, всё хорошо
	ActionIgnore = "IGNORE" // Это бот Т2, не тратим ракеты
	ActionAttack = "ATTACK" // Это чужак, его нужно сбить
)

type Strategist struct {
	ledger   *Ledger
	myLotID  string
	volume   int    // Гб/Мин/Смс
	value    int    // Цена
	mu             sync.Mutex
	lastMyShotTime time.Time
	isExecuting    bool
}

// Обнови конструктор
func NewStrategist(l *Ledger, myLotID string, vol int, val int) *Strategist {
	return &Strategist{
		ledger:  l,
		myLotID: myLotID,
		volume:  vol,
		value:   val,
		lastMyShotTime: time.Now().Add(-1 * time.Hour),
	}
}

// AnalyzeTarget — Атом №1: классифицирует лот на 1-м месте
func (s *Strategist) AnalyzeTarget(topLotID string, isBot bool) string {
	// 1. Проверка на Бота Т2
	// Твой принцип: с ботами не воюем, не кормим систему
	if isBot {
		return ActionIgnore
	}

	// 2. Проверка на "Своего" (участника картеля)
	s.ledger.mu.RLock()
	_, isMember := s.ledger.Members[topLotID]
	s.ledger.mu.RUnlock()

	if isMember {
		// Если это наш лот или лот соседа по MDN
		return ActionRelax
	}

	// 3. Если это не бот и не участник сети — это Чужак
	fmt.Printf("[СТРАТЕГ] Обнаружен чужак на 1-м месте: %s\n", topLotID)
	return ActionAttack
}

// AmITheShooter — Атом №2: решает, является ли эта нода первой в очереди на выстрел
func (s *Strategist) AmITheShooter() bool {
	// 1. Получаем актуальную отсортированную очередь
	queue := s.ledger.GetSortedQueue()

	// 2. Если очередь пуста (например, мы еще никого не нашли), стрелять нельзя
	if len(queue) == 0 {
		return false
	}

	// 3. Лидер очереди — это самый первый элемент в отсортированном списке
    leaderLotID := queue[0]

	// 4. Проверяем, совпадает ли лидер с нашим лотом
	if leaderLotID == s.myLotID {
		fmt.Println("[СТРАТЕГ] Моя очередь! Я — лидер очереди.")
		return true
	}

	// Если не мы — просто выводим, кого мы ждем (для отладки)
	fmt.Printf("[СТРАТЕГ] Жду очередь. Сейчас лидер: %s\n", leaderLotID)
	return false
}
// GetSortedActivePeers возвращает список ID всех живых нод, отсортированный по алфавиту
func (l *Ledger) GetSortedActivePeers() []peer.ID {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var active []peer.ID
	for _, p := range l.Members {
		// Считаем живыми тех, кто подавал знак последние 60 секунд
		if time.Since(p.LastSeen) < 5*time.Minute {
			active = append(active, p.PeerID)
		}
	}

	// Сортировка обязательна для детерминизма!
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
		// Для Linux, macOS и Android (Termux)
		cmd = exec.Command("clear")
	}
	cmd.Stdout = os.Stdout
	cmd.Run()
}
func (s *Strategist) ShowDashboard() {
	// Команда очистки экрана для Android/Linux
	ClearScreen()

	fmt.Println("================================================================")
	fmt.Printf("   MDN v0.1 | СЕГМЕНТ: %d ГБ / %d РУБ | ЛОТ: %s\n", s.volume, s.value, s.myLotID)
	fmt.Println("================================================================")
	fmt.Printf("%-3s | %-12s | %-4s | %-4s | %-6s | %s\n", "#", "PeerID", "T", "R", "P", "Статус")
	fmt.Println("----------------------------------------------------------------")

	// Получаем отсортированную очередь
	queue := s.ledger.GetSortedQueue()

	s.ledger.mu.RLock()
	defer s.ledger.mu.RUnlock()

	for i, lotID := range queue {
		p := s.ledger.Members[lotID]
		if p == nil { continue }


    	// 1. Рассчитываем W (Wait Time) — Труд в очереди
    	// Если лот еще ни разу не был в топе, используем время JoinedAt
    	lastActivity := p.LastTopTick
    	if lastActivity == 0 {
    		lastActivity = p.JoinedAt
    	}
    
    	waitTime := float64(p.GlobalTick - lastActivity)
    	if waitTime < 1 {
    		waitTime = 1 // Защита от деления на 0
    	}
    
    	// 2. Рассчитываем Индекс Сытости (Satiety)
    	// T — обычные циклы, R — ракеты (вес 1.2)
    	satiety := float64(p.T) + (float64(p.R) * 1.2) + 1.0
    
    	// 3. Итоговая формула MDN Equilibrium
    	// P = (S^2) / W
    	// Победит тот, у кого число будет САМЫМ МАЛЕНЬКИМ
    	priority := (satiety * satiety) / waitTime + 1.0
		
		// Пометка "это я"
		prefix := "  "
		if lotID == s.myLotID {
			prefix = ">>"
		}

		// Пометка карантина
		status := "Active"
		/*if time.Now().Unix() - p.JoinedAt < 1200 {
			status = "WAIT (Q)"
			priority += 1000000.0 // Визуально отображаем штраф
		}*/
        shortPID := p.PeerID.String()
		if len(shortPID) > 8 {
            shortPID = shortPID[len(shortPID)-8:]
        }

		fmt.Printf("%s%-1d | %-12s | %-4d | %-4d | %-6.2f | %s\n", 
			prefix, i+1, shortPID, p.T, p.R, priority, status)
	}
	fmt.Println("================================================================")
}
// checkDutyRole возвращает: я дежурный?, мой ранг (для таймера)
func (s *Strategist) checkDutyRole(myID peer.ID, numDutyNodes int) (bool, int) {
	activePeers := s.ledger.GetSortedActivePeers()
	if len(activePeers) == 0 {
		return true, 0
	}

	// 1. Если нод меньше, чем нужно дежурных - дежурят все
	if len(activePeers) <= numDutyNodes {
		myRank := -1
		for i, p := range activePeers {
			if p == myID { myRank = i; break }
		}
		return true, myRank
	}

	// 2. Создаем детерминированный рандом на основе времени
	seed := time.Now().Unix() / 300 // Окно 5 минут
	rng := rand.New(rand.NewSource(seed))
	// 3. Делаем копию списка и перемешиваем её
	shuffled := make([]peer.ID, len(activePeers))
	copy(shuffled, activePeers)
	
	sort.Slice(shuffled, func(i, j int) bool {
        return shuffled[i].String() < shuffled[j].String()
    })
	
	rng.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	// 4. Берем первых N дежурных
	dutyNodes := shuffled[:numDutyNodes]
    
	// 5. Проверяем, входим ли мы в этот список
	isDuty := false
	myRank := 99 // По умолчанию низкий приоритет для таймера
	for i, p := range dutyNodes {
		if p.String() == myID.String() {
			isDuty = true
			myRank = i
			break
		}
	}

	return isDuty, myRank
}
/*func (s *Strategist) isDuty(myID peer.ID) bool {
	// 1. Создаем временное окно (меняется каждые 5 минут)
	//
	slot := time.Now().Unix() / 300
	
	// 2. Генерируем уникальный хеш для этой пары (Я + Время)
	data := fmt.Sprintf("%s-%d", myID.String(), slot)
	hash := sha256.Sum256([]byte(data))
	
	// 3. Порог: например, если значение первого байта < 50 (~20% всех нод станут дежурными)
	fmt.Println(hash[0])
	// Это обеспечит 3-5 дежурных в группе из 20 человек.
	return hash[0] < (255/4)
}*/
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
	// Создаем сообщение типа TOP_STATUS
	msg := NodeMessage{
		Type:  "TOP_STATUS",
		LotID: info.ID, // ID того, кого мы увидели на 1 месте
		PeerID: myPeerID,  
		IsBot: info.IsBot, // Бот это или нет
		// T и R в этом типе сообщения не важны, можно оставить 0
	}

	raw, err := json.Marshal(msg)
	if err != nil {
		return
	}

	// Публикуем в топик
	topic.Publish(context.Background(), raw)
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

func (s *Strategist) SelectRandomProxy(h host.Host) peer.ID {
	s.ledger.mu.RLock()
	defer s.ledger.mu.RUnlock()

	// 1. Создаем список потенциальных "своих" прокси
	var mdnPeers []peer.ID

	for _, participant := range s.ledger.Members {
		// Игнорируем себя
		if participant.PeerID == h.ID() {
			continue
		}
		
		// Проверяем, на связи ли этот участник прямо сейчас
		if h.Network().Connectedness(participant.PeerID) == network.Connected {
			mdnPeers = append(mdnPeers, participant.PeerID)
		}
	}

	// 2. Если в Леджере никого "своего" на связи нет
	if len(mdnPeers) == 0 {
		fmt.Println("[MDN] Своих нод для прокси не найдено. Использую прямой выстрел.")
		return "" 
	}

	// 3. Выбираем случайного именно из нашего картеля
	return mdnPeers[rand.Intn(len(mdnPeers))]
}
func (s *Strategist) Run(ctx context.Context, mn *MarketNode, bearer, number string) {
	// 1. Запускаем фоновый цикл Дежурного
	go s.dutyLoop(ctx, mn)

	// 2. Запускаем фоновый цикл Анонсера (говорим сети, что мы живы)
	go s.announceLoop(ctx, mn)
	
	//fmt.Println("[MDN] Боевой модуль Strategist активирован.")
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
		// Обновляем память о соседе
		s.ledger.Update(m.LotID, m.PeerID, m.T, m.R, m.JoinedAt, m.LastTopTick, m.GlobalTick)
		fmt.Println("[DEBUG] Ledger обновлен!")

	case "TOP_STATUS":
		// 1. Инкрементируем тики для того, кто сейчас в топе
		s.ledger.UpdateTicks(m.LotID)

		// 2. Атом №1: Анализ цели
		status := s.AnalyzeTarget(m.LotID, m.IsBot)
        
		if status == ActionAttack {
			s.mu.Lock()
            if s.isExecuting {
                s.mu.Unlock()
                return // Мы уже в процессе выстрела, игнорируем новые сигналы
            }
			if s.AmITheShooter() {
				// 4. Атом №3: Скрытный выстрел (в отдельной горутине, чтобы не вешать сеть)
				s.isExecuting = true 
                s.mu.Unlock()
				go s.PerformExecution(ctx, mn, bearer, number)
			} else {
			    s.mu.Unlock()
			}
		}
	}
}
// wrapWithUTLS — Атом маскировки. Превращает "голый" стрим в "мобильное" соединение.
func (s *Strategist) wrapWithUTLS(conn net.Conn) (net.Conn, error) {
	// 1. Настройка параметров (SNI)
	config := &utls.Config{
		ServerName: t2api.T2Host,
		NextProtos: []string{"http/1.1"},
	}

	// 2. Создаем специальный uTLS клиент с профилем Android
	uConn := utls.UClient(conn, config, utls.HelloAndroid_11_OkHttp)

	// 3. Выполняем Handshake вручную. 
	// Это критично, чтобы зафиксировать маскировку до начала передачи данных.
	if err := uConn.Handshake(); err != nil {
		return nil, fmt.Errorf("utls handshake failed: %w", err)
	}

	return uConn, nil
}
// PerformExecution — Главный "Боевой Атом". Объединяет джиттер, прокси и отчетность.
func (s *Strategist) PerformExecution(ctx context.Context, mn *MarketNode, bearer, number string) {

    defer func() {
		s.mu.Lock()
		s.isExecuting = false
		s.mu.Unlock()
	}()
    s.mu.Lock()
	if time.Since(s.lastMyShotTime) < 10*time.Second {
		s.mu.Unlock()
		// Мы недавно стреляли. Пропускаем этот цикл, чтобы не злить антифрод.
		fmt.Println("Уже недавно стреляли. Ждём 10 секунд")
		return 
	}
	s.lastMyShotTime = time.Now() // Фиксируем попытку сразу
	s.mu.Unlock()
	// 1. Джиттер (Маскировка под человека)
	wait := rand.Intn(3) 
	fmt.Printf("[MDN] Враг обнаружен! Имитирую раздумья (%d сек)...\n", wait)
    
	select {
	case <-time.After(time.Duration(wait) * time.Second):
	case <-ctx.Done():
		return
	}

	// 2. Выбор пути (Прокси или Прямой выстрел)
	/*proxyID := s.SelectRandomProxy(mn.Host)
	var client *http.Client

	if proxyID == "" {
		fmt.Println("[MDN] Соседей-прокси нет. Использую ПРЯМОЙ выстрел (риск палева IP)!")
		client = t2api.SharedClient
	} else {
		fmt.Printf("[MDN] Использую стелс-туннель через: %s\n", proxyID.String()[len(proxyID)-8:])
		client = s.CreateProxiedClient(ctx, mn.Host, proxyID)
	}*/

	// 3. САМ ВЫСТРЕЛ
	//err := t2api.Rocket(client, bearer, number, s.myLotID)
	//fmt.Println("Успешная имитация ракеты!")
	var err error = nil
	if err == nil {
	    fmt.Println("Успешная имитация ракеты!")
		//fmt.Println("[MDN] !!! ПОБЕДА !!! Чужак сбит, наш лот в топе.")
		
		// 4. Синхронизация своего состояния
		s.ledger.mu.Lock()
		if p, ok := s.ledger.Members[s.myLotID]; ok {
			p.R++ // Увеличиваем количество потраченных ракет
			p.T = p.T + int64(rand.Intn(3)) + 1
			p.LastTopTick = s.ledger.GlobalTick
			// Вещаем на всю сеть: "Я потратился, мой приоритет упал"
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
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				// 1. Открываем P2P стрим к соседу-посреднику
				stream, err := h.NewStream(ctx, proxyPeerID, ProtocolProxy)
				if err != nil {
					return nil, err
				}

				// 2. Оборачиваем стрим в нашу "Маску" (uTLS)
				// Теперь через P2P канал полетят байты, имитирующие Android-телефон
				uConn, err := s.wrapWithUTLS(&StreamConn{stream})
				if err != nil {
					stream.Reset()
					return nil, err
				}

				return uConn, nil
			},
			DisableKeepAlives: true, // Каждый выстрел — новая маска
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

			// Решаем, нужно ли нам стать дежурным принудительно
			isMathematicalDuty, _ := s.checkDutyRole(mn.Host.ID(), 3)
			if isMathematicalDuty {
				lots, err := t2api.GetTop4IDs(int(s.volume), int(s.value))
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