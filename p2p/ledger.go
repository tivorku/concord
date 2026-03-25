package p2p

import (
	"sync"
	"time"
	"sort"
    "context"
	"github.com/libp2p/go-libp2p/core/peer"
)
/*var (
    PriorityVar float64
)*/
// Participant — это запись об одном конкретном лоте в сети.
// это то, что наша нода будет помнить о каждом участнике сети.
type Participant struct {
	LotID     string
	PeerID    peer.ID
	T         int64
	R         int64
	PriorityVar float64
	WaitTime  int64
	LastTopTick int64
	LastEpoch int64
	JoinedAt  int64
	LastSeen  time.Time
}
// Ledger — это общая память сети. 
// каждая нода хранит свою копию этой структуры.
type Ledger struct {
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
func (l *Ledger) Update(lotID string, pID peer.ID, incomingT int64, incomingR int64, joinedAt int64, lastTopTick int64, incomingEpoch int64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
    diff := incomingEpoch - GetCurrentEpoch()
    if diff < -1 || diff > 1 {
        return false
    }
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
			LastEpoch: incomingEpoch,
		}
		return false
	}
	if incomingEpoch < p.LastEpoch {
        return false 
    }
    if p.LastEpoch == incomingEpoch {
        if incomingT < p.T || incomingR < p.R {
            return true 
        }
	} else if incomingEpoch > p.LastEpoch {
	    wasOnline := time.Since(p.LastSeen) < 20*time.Second
        if wasOnline {
            if incomingT < (p.T / 2) || incomingR < (p.R / 2) {
                return true
            }
        } else {
            if incomingT < p.T || incomingR < p.R {
                return true
            }
        }
	}
	if joinedAt > p.JoinedAt { p.JoinedAt = joinedAt }
    if lastTopTick > p.LastTopTick { p.LastTopTick = lastTopTick }
    
    if incomingT > p.T {
        p.T = incomingT   
    }
    if incomingR > p.R {
        p.R = incomingR
    }
    p.LastEpoch = incomingEpoch
	p.LastSeen = time.Now()
	return false
}
// Item — вспомогательная структура для сортировки
type Item struct {
	LotID    string
	Priority float64
	PeerID   string
	JoinedAt int64
}
func GetCurrentEpoch() int64 {
    networkUnix := time.Now().Unix() + NetworkTimeOffset
    return networkUnix / 900
}
func (l *Ledger) GetSortedQueue(node *Node) []string {
	l.mu.RLock()
	
	var activeItems []Item

	for id, p := range l.Members {
		// игнорируем тех, кто молчит дольше 15 секунд (оффлайн), кроме себя
		if time.Since(p.LastSeen) > 15*time.Second && p.PeerID != node.Host.ID() {
			continue
		}

    	
    	if p.LastTopTick == 0 {
    	    p.WaitTime = time.Now().Unix() + NetworkTimeOffset - p.JoinedAt + NetworkTimeOffset
    	} else {
    	    p.WaitTime = time.Now().Unix() + NetworkTimeOffset - p.LastTopTick
    	}
    	if p.WaitTime < 1 {
    	    p.WaitTime = 1 // защита от деления на 0
    	}
    
    	// рассчитываем satiety
    	// T — обычные циклы, R — ракеты
    	satiety := (float64(p.T) * 0.1) + (float64(p.R) * 0.5)
    
    	// итоговая формула
    	// P = S² / W
    	// победит тот, у кого число будет САМЫМ МАЛЕНЬКИМ
    	if satiety == 0 {
            satiety = 1
        }
        p.PriorityVar = (satiety * satiety) / (float64(p.WaitTime))
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
	l.mu.RUnlock()
	return result
}
func (l *Ledger) GetSortedActivePeers(node *Node) []peer.ID {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var active []peer.ID
	for _, p := range l.Members {
		// считаем живыми тех, кто подавал знак последние 10 секунд и себя
		if time.Since(p.LastSeen) <= 10*time.Second || p.PeerID == node.Host.ID(){
			active = append(active, p.PeerID)
		}
	}

	return active
}
func (l *Ledger) StartJanitor(ctx context.Context, node *Node) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.mu.Lock()
			
			now := time.Now()

			for id, p := range l.Members {
				// если мы не слышали о ноде больше 30 минут — она ушла из сети
				if now.Sub(p.LastSeen) > 30*time.Minute && p.PeerID != node.Host.ID() {
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