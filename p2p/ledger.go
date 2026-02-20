package p2p

import (
	"sync"
	"time"
	"sort"
    "fmt"
    "context"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Participant — это запись об одном конкретном лоте в сети.
// Это то, что твоя нода будет помнить о каждом конкуренте или союзнике.
type Participant struct {
	LotID     string
	PeerID    peer.ID
	T         int64
	R         int
	LastTopTick int64
	GlobalTick int64
	JoinedAt  int64 // Добавляем дату первого появления
	LastSeen  time.Time
}
// Ledger — это общая память "картеля". 
// Каждая нода хранит свою копию этой структуры.
type Ledger struct {
    GlobalTick int64
	mu      sync.RWMutex            // "Замок" для безопасной работы из разных потоков
	Members map[string]*Participant // Карта всех лотов: ключ — это LotID
}

// NewLedger создает и инициализирует чистую память.
// Вызывается один раз при старте программы.
func NewLedger() *Ledger {
	return &Ledger{
		Members: make(map[string]*Participant),
	}
}
func (l *Ledger) Update(lotID string, pID peer.ID, incomingT int64, incomingR int, joinedAt int64, lastTopTick int64, incomingTick int64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	p, exists := l.Members[lotID]
	
	// Если лот новый
	if !exists {
		// Небольшая проверка: JoinedAt не может быть в будущем

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

	// Если лот старый, но пришел более "молодой"JoinedAt (кто-то пытается обмануть возраст)
	if joinedAt > p.JoinedAt {
		p.JoinedAt = joinedAt // Обновляем на более актуальное (безопасное) время
	}
	// Если у соседа счетчик больше — подтягиваемся за ним
    if incomingTick > l.GlobalTick {
        l.GlobalTick = incomingTick
    }
	if lastTopTick > p.LastTopTick {
	    p.LastTopTick = lastTopTick
	}

	// Синхронизация T и R (твой принцип максимума)
	if incomingT > p.T { p.T = incomingT }
	if incomingR > p.R { p.R = incomingR }
	
	p.LastSeen = time.Now()
}
// Item — вспомогательная структура для сортировки
type Item struct {
	LotID    string
	Priority float64
	PeerID   string
	JoinedAt int64
}

// GetSortedQueue анализирует память и выдает текущую очередь картеля.
func (l *Ledger) GetSortedQueue() []string {
	l.mu.RLock()
	defer l.mu.RUnlock()

	//now := time.Now().Unix()
	var activeItems []Item

	for id, p := range l.Members {
		// 1. Игнорируем тех, кто молчит дольше 2 минут (оффлайн)
		if time.Since(p.LastSeen) > 5*time.Minute {
			continue
		}

		// 2. Считаем базовый приоритет по твоей формуле

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
    

		// 3. ПРОВЕРКА НА КАРАНТИН (1200 секунд = 20 минут)
		/*if now-p.JoinedAt < 1200 {
			// Если нода молодая — выкидываем её в самый конец очереди,
			// прибавляя к приоритету огромное число.
			priority += 1000000.0
		}*/

		activeItems = append(activeItems, Item{
			LotID:    id,
			Priority: priority,
			PeerID:   p.PeerID.String(),
			JoinedAt: p.JoinedAt, // Понадобится для Tie-break
		})
	}

	// 4. Детерминированная сортировка
	sort.Slice(activeItems, func(i, j int) bool {
		// Сначала сравниваем по приоритету (с учетом штрафа за карантин)
		if activeItems[i].Priority != activeItems[j].Priority {
			return activeItems[i].Priority < activeItems[j].Priority
		}
		// Если оба "старички" или оба "новички" с равным P — смотрим кто зашел раньше
		if activeItems[i].JoinedAt != activeItems[j].JoinedAt {
			return activeItems[i].JoinedAt < activeItems[j].JoinedAt
		}
		// Если даже время захода совпало — финальный Tie-break по PeerID
		return activeItems[i].PeerID < activeItems[j].PeerID
	})

	// 5. Собираем только LotID
	result := make([]string, len(activeItems))
	for i, item := range activeItems {
		result[i] = item.LotID
	}
	return result
}
// StartJanitor — это фоновый процесс, который удаляет "мертвые" ноды.
func (l *Ledger) StartJanitor(ctx context.Context) {
	// Будем проверять список раз в минуту
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// Если основная программа закрывается, выходим из цикла
			return
		case <-ticker.C:
			l.mu.Lock() // Запираем на время уборки
			
			now := time.Now()
			countBefore := len(l.Members)

			for id, p := range l.Members {
				// Если мы не слышали о ноде больше 2 минут — она ушла из сети
				if now.Sub(p.LastSeen) > 5*time.Minute {
					delete(l.Members, id)
				}
			}

			countAfter := len(l.Members)
			if countBefore != countAfter {
				fmt.Printf("[LEDGER] Уборка завершена: удалено %d оффлайн-нод\n", countBefore-countAfter)
			}
			
			l.mu.Unlock() // Отпираем
		}
	}
}
func (l *Ledger) UpdateTicks(lotID string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if p, exists := l.Members[lotID]; exists {
		p.T++ // Прибавляем 1 цикл нахождения в топе
	}
}