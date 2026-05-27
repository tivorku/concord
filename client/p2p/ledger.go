package p2p

import (
	"context"
	"fmt"
	"sync"
	"time"
	"math"

	"github.com/libp2p/go-libp2p/core/peer"
	"sort"
)

type Participant struct {
	LotID       string
	PeerID      peer.ID
	T           int64
	ActiveOps   int64
	NetOps      int64
	LastTopTick int64
	LastEpoch   int64
	JoinedAt    int64
	OpsCooldown time.Time
	LastSeen    time.Time
}

type Ledger struct {
	mu      sync.RWMutex
	Members map[string][]*Participant // ключ = PeerID.String()
	Blocklist map[string]time.Time
	UseMock bool
}

func NewLedger(useMock bool) *Ledger {
	return &Ledger{
		Members: make(map[string][]*Participant),
		Blocklist: make(map[string]time.Time),
		UseMock: useMock,
	}
}

func (p *Participant) TrustScore() float64 {
    // 1 интервал - 5 минут
	intervals := float64((time.Now().Unix())-p.JoinedAt) / (5 * 60.0)
	if intervals < 0 {
		return 0
	}
	return intervals
}

func (l *Ledger) IsLotKnown(lotID string) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, participants := range l.Members {
		for _, p := range participants {
			if _, banned := l.Blocklist[lotID]; p.LotID == lotID && !banned {
				return true
			}
		}
	}
	return false
}

func (l *Ledger) Update(lotID string, pID peer.ID, incomingT int64, joinedAt int64, lastTopTick int64, incomingEpoch int64, incomingActiveOps int64, incomingNetOps int64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	incomingPID := pID.String()
	for otherPeer, participants := range l.Members {
		if otherPeer == incomingPID {
			continue
		}
		for _, p := range participants {
			if p.LotID == lotID {
				fmt.Printf("[Security] Попытка пира %s скопировать лот %s у %s!\n", incomingPID[len(incomingPID)-8:], lotID, otherPeer[len(otherPeer)-8:])
				return false 
			}
		}
	}

	diff := incomingEpoch - GetCurrentEpoch()
	if diff < -1 || diff > 1 {
		fmt.Println("Слишком большая разница эпох.")
		return false
	}

	participants := l.Members[incomingPID]

	// Ищем существующий лот от этого пира
	var p *Participant
	for _, part := range participants {
		if part.LotID == lotID {
			p = part
			break
		}
	}

	if p == nil {
		// Новый лот
		l.Members[incomingPID] = append(participants, &Participant{
			LotID:       lotID,
			PeerID:      pID,
			T:           incomingT,
			ActiveOps:   incomingActiveOps,
			NetOps:      incomingNetOps,
			LastTopTick: lastTopTick,
			JoinedAt:    joinedAt,
			LastSeen:    time.Now(),
			OpsCooldown: time.Now(),
			LastEpoch:   incomingEpoch,
		})
		return false
	}

	if incomingEpoch < p.LastEpoch {
		fmt.Println("Отклоняю сообщение из прошлой эпохи.")
		return false
	}
	if p.LastEpoch == incomingEpoch {
		if incomingT < p.T || incomingNetOps > p.NetOps || incomingActiveOps > p.ActiveOps {
		    switch {
		    case incomingActiveOps > p.ActiveOps:
		        fmt.Printf("Шлю коррекционный пакет на сообщение из этой эпохи с ActiveOps. old (%d) -> new (%d)\n", p.ActiveOps, incomingActiveOps)
	        case incomingT < p.T:
	            fmt.Printf("Шлю коррекционный пакет на сообщение из этой эпохи с T. old (%d) -> new (%d)\n", p.T, incomingT)
            case incomingNetOps > p.NetOps:
                fmt.Printf("Шлю коррекционный пакет на сообщение из этой эпохи с NetOps. old (%d) -> new (%d)\n", p.NetOps, incomingNetOps)
		    }
			
			return true
		}
	} else if incomingEpoch > p.LastEpoch {
		wasOnline := time.Since(p.LastSeen) < 20*time.Second
		if wasOnline {
			if incomingT < (p.T/2) {
				fmt.Println("Шлю коррекционный пакет. Новые значения меньше, чем должны быть после слайдинга.")
				return true
			}
		} else {
			if incomingT < p.T {
				fmt.Println("Шлю коррекционный пакет. Этой ноды не было онлайн, так что ей нельзя слайдиться.")
				return true
			}
		}
	}

    p.ActiveOps = incomingActiveOps
    p.NetOps = incomingNetOps
    
    if incomingActiveOps == 0 && !l.UseMock {
        delete(l.Blocklist, lotID)
        parts := l.Members[pID.String()]
        for i, p := range parts {
            if p.LotID == lotID {
                copy(parts[i:], parts[i+1:])
                parts[len(parts)-1] = nil
                l.Members[pID.String()] = parts[:len(parts)-1]
                break
            }
        }
        return false
    }
	if joinedAt > p.JoinedAt {
		p.JoinedAt = joinedAt
	}
	if lastTopTick > p.LastTopTick {
		p.LastTopTick = lastTopTick
	}

	if incomingT > p.T {
		diff := incomingT - p.T
		if diff > 3 {
			diff = 3
		}
		p.T = p.T + diff
	} else {
		p.T = incomingT
	}

	p.LastEpoch = incomingEpoch
	p.LastSeen = time.Now()
	return false
}

type Item struct {
	LotID     string
	Priority  float64
	PeerID    string
	JoinedAt  int64
	WaitTime  int64
	T         int64
	Trust     float64
	ActiveOps int64
	NetOps    int64
}

func GetCurrentEpoch() int64 {
	networkUnix := time.Now().Unix() + NetworkTimeOffset.Load()
	return networkUnix / 300
}

func (l *Ledger) GetQueueWithMetrics(node *Node) []Item {
	l.mu.RLock()
	defer l.mu.RUnlock()

	now := time.Now().Unix() + NetworkTimeOffset.Load()
	var items []Item
	for _, participants := range l.Members {
		for _, p := range participants {
			if _, banned := l.Blocklist[p.LotID]; time.Since(p.LastSeen) > 15*time.Second && p.PeerID != node.Host.ID() || banned {
				continue
			}
			waitTime := p.LastTopTick
			if waitTime == 0 {
				waitTime = p.JoinedAt + NetworkTimeOffset.Load()
			}
			waitTime = now - waitTime
			if waitTime < 1 {
				waitTime = 1
			}

            spent_rockets := 5 - p.NetOps
			satiety := float64(p.T) + float64(spent_rockets)*5
			if satiety == 0 {
				satiety = 1
			}
			satietyFactor := (satiety * math.Pow(satiety, 0.5))
			trustFactor := (1.0 + math.Log2(1.0 + p.TrustScore()))
			
			priority := satietyFactor / (float64(waitTime) * trustFactor)
			items = append(items, Item{
				LotID:     p.LotID,
				Priority:  priority,
				PeerID:    p.PeerID.String(),
				JoinedAt:  p.JoinedAt,
				WaitTime:  waitTime,
				T:         p.T,
				Trust:     p.TrustScore(),
				ActiveOps: p.ActiveOps,
				NetOps:    p.NetOps,
			})
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Priority != items[j].Priority {
			return items[i].Priority < items[j].Priority
		}
		if items[i].JoinedAt != items[j].JoinedAt {
			return items[i].JoinedAt < items[j].JoinedAt
		}
		return items[i].PeerID < items[j].PeerID
	})
	return items
}

func (l *Ledger) GetSortedQueue(node *Node) []string {
	items := l.GetQueueWithMetrics(node)
	result := make([]string, len(items))
	for i, item := range items {
		result[i] = item.LotID
	}
	return result
}

func (l *Ledger) GetActivePeers(node *Node) []peer.ID {
	l.mu.RLock()
	defer l.mu.RUnlock()

	seen := make(map[peer.ID]bool)
	var active []peer.ID

	for _, participants := range l.Members {
		for _, p := range participants {
			if seen[p.PeerID] {
				continue
			}
			if time.Since(p.LastSeen) <= 10*time.Second || p.PeerID == node.Host.ID() {
				seen[p.PeerID] = true
				active = append(active, p.PeerID)
			}
		}
	}

	return active
}

func (l *Ledger) GetMyLots(node *Node) []*Participant {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.Members[node.Host.ID().String()]
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

			for peerID, participants := range l.Members {
				allOffline := true
				for _, p := range participants {
					if now.Sub(p.LastSeen) <= 30*time.Minute || p.PeerID == node.Host.ID() {
						allOffline = false
						break
					}
				}
				if allOffline && peerID != node.Host.ID().String() {
					delete(l.Members, peerID)
				}
			}
			l.mu.Unlock()
		}
	}
}

func (l *Ledger) UpdateTicks(lotID string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, participants := range l.Members {
    	for _, p := range participants {
    		if p.LotID == lotID {
    			p.T++
    			return
    		}
    	}
	}
}