package p2p

import (
	"sync"
	"time"
	"sort"
    "context"
	"github.com/libp2p/go-libp2p/core/peer"
)
var (
    PriorityVar float64
)
// Participant — это запись об одном конкретном лоте в сети.
// это то, что наша нода будет помнить о каждом участнике сети.
type Participant struct {
	LotID     string
	PeerID    peer.ID
	T         int64
	R         int
	PriorityVar float64
	LastTopTick int64
	GlobalTick int64
	JoinedAt  int64
	LastSeen  time.Time
}
// Ledger — это общая память сети. 
// каждая нода хранит свою копию этой структуры.
type Ledger struct {
    GlobalTick int64
	mu      sync.RWMutex
	Members map[string]*Participant // карта всех лотов: ключ — это LotID
}

// NewLedger создает и инициализирует чистую память.
// вызывается один раз при старте программы.
func NewLedger() *Ledger {
	return &Ledger{
		Members: make(map[string]*Participant),
	}
}
func (l *Ledger) Update(lotID string, pID peer.ID, incomingT int64, incomingR int, joinedAt int64, lastTopTick int64, incomingTick int64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	p, exists := l.Members[lotID]

	if !exists {
		l.Members[lotID] = &Participant{
			LotID:    lotID,
			PeerID:   pID,
			T:        incomingT,
			R:        incomingR,
			LastTopTick: lastTopTick,
			JoinedAt: joinedAt,
			LastSeen: time.Now(),
			GlobalTick: incomingTick,
		}
		return
	}
	// если присоединился позже, чем в базе, то обновляем информацию
	if joinedAt > p.JoinedAt {
		p.JoinedAt = joinedAt // обновляем на более актуальное время
	}
	// если у соседа счетчик больше — подтягиваемся за ним
    if incomingTick > l.GlobalTick {
        l.GlobalTick = incomingTick
    }
	if lastTopTick > p.LastTopTick {
	    p.LastTopTick = lastTopTick
	}

	// синхронизация T и R
	if incomingT != p.T { p.T = incomingT }
	if incomingR != p.R { p.R = incomingR }
	
	p.LastSeen = time.Now()
}
// Item — вспомогательная структура для сортировки
type Item struct {
	LotID    string
	Priority float64
	PeerID   string
	JoinedAt int64
}

func (l *Ledger) GetSortedQueue() []string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	
	var activeItems []Item

	for id, p := range l.Members {
		// игнорируем тех, кто молчит дольше 2 минут (оффлайн)
		if time.Since(p.LastSeen) > 2*time.Minute {
			continue
		}

    	lastActivity := p.LastTopTick
    	if lastActivity == 0 {
    		lastActivity = p.JoinedAt
    	}
    
    	waitTime := float64(p.GlobalTick - lastActivity)
    	if waitTime < 1 {
    		waitTime = 1 // защита от деления на 0
    	}
    
    	// рассчитываем satiety
    	// T — обычные циклы, R — ракеты (вес 1.2)
    	satiety := float64(p.T) + (float64(p.R) * 1.2)
    
    	// итоговая формула
    	// P = S² / W
    	// победит тот, у кого число будет САМЫМ МАЛЕНЬКИМ
        p.PriorityVar = (satiety * satiety) + 0.01 / waitTime
    

		// проверка на карантин (20 минут)
		/*now := time.Now().Unix()
		if now-p.JoinedAt < 1200 {
			p.PriorityVar += 1000000.0
		}*/

		activeItems = append(activeItems, Item{
			LotID:    id,
			Priority: p.PriorityVar,
			PeerID:   p.PeerID.String(),
			JoinedAt: p.JoinedAt, // понадобится для Tie-break
		})
	}

	// детерминированная сортировка
	sort.Slice(activeItems, func(i, j int) bool {
		// сначала сравниваем по приоритету
		if activeItems[i].Priority != activeItems[j].Priority {
			return activeItems[i].Priority < activeItems[j].Priority
		}
		// если оба "старички" или оба "новички" с равным P — смотрим кто зашел раньше
		if activeItems[i].JoinedAt != activeItems[j].JoinedAt {
			return activeItems[i].JoinedAt < activeItems[j].JoinedAt
		}
		// если даже время захода совпало — финальный Tie-break по PeerID
		return activeItems[i].PeerID < activeItems[j].PeerID
	})

	// собираем только LotID
	result := make([]string, len(activeItems))
	for i, item := range activeItems {
		result[i] = item.LotID
	}
	return result
}
func (l *Ledger) StartJanitor(ctx context.Context) {
	// будем проверять список раз в минуту
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.mu.Lock() // запираем на время уборки
			
			now := time.Now()

			for id, p := range l.Members {
				// если мы не слышали о ноде больше 2 минут — она ушла из сети
				if now.Sub(p.LastSeen) > 2*time.Minute {
					delete(l.Members, id)
				}
			}
			l.mu.Unlock()
		}
	}
}
func (l *Ledger) UpdateTicks(lotID string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if p, exists := l.Members[lotID]; exists {
		p.T++ // прибавляем 1 цикл нахождения в топе
	}
}