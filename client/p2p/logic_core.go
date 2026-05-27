package p2p

import (
	"context"
	"fmt"
	"time"
	"sync"
	
	"concord/pb"
	"concord/t2api"
)

type LogicCore struct {
	ledger      *Ledger
	myLotIDs    []string
	volume      int
	value       int
	uom         string
	shooter     *Shooter
	broadcaster *Broadcaster
	dashboard   *Dashboard
	bearer      string
	number      string
	wg          *sync.WaitGroup
}

func InitLogicCore(l *Ledger, myLotIDs []string, vol int, val int, uom string, bearer, number string, node *Node, useMock bool, wg *sync.WaitGroup) *LogicCore {
	myID := node.Host.ID().String()
	shooter := NewShooter(bearer, number, myLotIDs, l, wg)
	broadcaster := NewBroadcaster(l, myID, bearer, number, useMock)
	dashboard := NewDashboard(l, myLotIDs, vol, val, uom)

	return &LogicCore{
		ledger:      l,
		myLotIDs:    myLotIDs,
		volume:      vol,
		value:       val,
		uom:         uom,
		shooter:     shooter,
		broadcaster: broadcaster,
		dashboard:   dashboard,
		bearer:      bearer,
		number:      number,
	    wg:          wg,
	}
}

func (lc *LogicCore) isMyLot(lotID string) bool {
	for _, id := range lc.myLotIDs {
		if id == lotID {
			return true
		}
	}
	return false
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
	} else {
	    lc.ledger.mu.Lock()
        if _, banned := lc.ledger.Blocklist[leaderLotID]; banned {
            fmt.Println("Этот лот находится в бане")
            lc.ledger.mu.Unlock()
            return "", false
        }
        lc.ledger.mu.Unlock()
	}

	myItems := lc.dashboard.GetMyLotsSortedByPriority(node)
	if len(myItems) == 0 {
		return "", false
	}

	myBestLot := myItems[0].LotID

	if leaderLotID == myBestLot {
		fmt.Println("[Brain] Моя очередь!")
		return leaderLotID, true
	}

	return "", false
}

func (lc *LogicCore) ShowDashboard(node *Node, rendezvous string) {
	lc.dashboard.ShowDashboard(node, rendezvous, lc.AmITheShooter)
}

func (lc *LogicCore) checkDutyRole(numDutyNodes int, node *Node) bool {
	return checkDutyRoleInternal(lc.ledger, numDutyNodes, node)
}

func (lc *LogicCore) Run(ctx context.Context, node *Node) {
	go lc.dutyLoop(ctx, node)
	go lc.announceLoop(ctx, node)
}

func (lc *LogicCore) HandleMessage(ctx context.Context, node *Node, m NodeMessage) {
    var senderPID string
    if len(m.SenderPID.String()) > 0 {
        senderPID = m.SenderPID.String()[len(m.SenderPID.String())-8:]
    } else {
        fmt.Println("SenderPID len <= 0")
    }
    
	if m.SenderPID != node.Host.ID() {
		fmt.Printf("[Debug] Пришел тип: %s | Лот: %s | От: %s | AO: %d | NO: %d\n", m.Type, m.LotID, senderPID, m.ActiveOps, m.NetOps)
	}
	lc.ledger.mu.Lock()
	timestamp, banned := lc.ledger.Blocklist[m.LotID]
	if banned {
	    if time.Since(timestamp) > 30*time.Minute {
	        go lc.broadcaster.broadcastUnbanSync(m, node.Topic)
	        delete(lc.ledger.Blocklist, m.LotID)
	    } else {
	        lc.ledger.mu.Unlock()
	        return
	    }
	}
	lc.ledger.mu.Unlock()

	switch m.Type {
	case "ANNOUNCE", "ROCKET":
		if m.NetOps - m.ActiveOps != 0 && !lc.ledger.UseMock {
		    lc.ledger.mu.Lock()
		    lc.ledger.Blocklist[m.LotID] = time.Now()
		    lc.ledger.mu.Unlock()
            go lc.broadcaster.broadcastViolation(m, node.Topic)
        }
		needsCorrection := lc.ledger.Update(m.LotID, m.SenderPID, m.T, m.JoinedAt, m.LastTopTick, m.LastEpoch, m.ActiveOps, m.NetOps)
		if needsCorrection && m.Type == "ANNOUNCE" {
			go lc.broadcaster.broadcastSyncCorrection(m, node.Topic)
		}

	case "TOP":
		lc.ledger.UpdateTicks(m.LotID)
		status := lc.AnalyzeTarget(m.LotID, m.IsBot)

		if status {
			lotID, isMyTurn := lc.AmITheShooter(node)
			if isMyTurn {
				if lc.shooter.TryLock() {
					go lc.shooter.PerformExecution(ctx, node, lotID, lc.broadcaster, lc.wg)
				}
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
				if m.NetOps < p.NetOps {
					p.NetOps = m.NetOps
				}
				if m.LastTopTick > p.LastTopTick {
					p.LastTopTick = m.LastTopTick
				}
			}
		}
		lc.ledger.mu.Unlock()

	case "VIOLATION":
	    lc.ledger.mu.Lock()
	    if m.NetOps - m.ActiveOps > 0 {
            lc.ledger.Blocklist[m.LotID] = time.Now()
		}
		lc.ledger.mu.Unlock()

	case "UNBAN":
	    lc.ledger.mu.Lock()
		var p *Participant
		for _, participants := range lc.ledger.Members {
    		for _, part := range participants {
    			if part.LotID == m.LotID {
    				p = part
    				break
    			}
    		}
		}
		if p != nil && lc.isMyLot(m.LotID) {
		    if p.NetOps - p.ActiveOps > 0 {
			    p.NetOps = p.ActiveOps
			}
		}
		lc.ledger.mu.Unlock()
	}
}

func (lc *LogicCore) dutyLoop(ctx context.Context, node *Node) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	trafficType := lc.uom
	switch lc.uom {
	case "gb":
		trafficType = "data"
	case "min":
		trafficType = "voice"
	case "sms":
		trafficType = "sms"
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			isDuty := lc.checkDutyRole(1, node)
			if isDuty {
				ch := t2api.GetTop4IDsAsync(trafficType, lc.volume, lc.value)
				lc.wg.Add(1)
				go func() {
				    defer lc.wg.Done()
					result := <-ch
					if result.Err != nil {
						fmt.Println(result.Err)
						return
					}
					if len(result.Lots) > 0 {
						lc.broadcaster.broadcastTopStatus(node.Topic, result.Lots[0])
						m := NodeMessage{
						    SenderPID: node.Host.ID(),
							Type:   "TOP",
							LotID:  result.Lots[0].ID,
							IsBot:  result.Lots[0].IsBot,
						}
						lc.HandleMessage(ctx, node, m)
					}
				}()
			}
		}
	}
}

func (lc *LogicCore) announceLoop(ctx context.Context, node *Node) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	var needsActiveOps bool
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			currentEpoch := GetCurrentEpoch()
			lc.ledger.mu.Lock()

			participants := lc.ledger.Members[node.Host.ID().String()]
			var messages []*pb.NodeMessage
			for _, me := range participants {
			    if me.ActiveOps == 0 && !lc.ledger.UseMock {
                    delete(lc.ledger.Blocklist, me.LotID)
                    parts := lc.ledger.Members[node.Host.ID().String()]
                    for i, p := range parts {
                        if p.LotID == me.LotID {
                            copy(parts[i:], parts[i+1:])
                            parts[len(parts)-1] = nil
                            lc.ledger.Members[node.Host.ID().String()] = parts[:len(parts)-1]
                            break
                        }
                    }
                    continue
                }
                needsActiveOps = time.Since(me.OpsCooldown) >= 9*time.Second
				if currentEpoch > me.LastEpoch {
					me.T /= 2
					me.LastEpoch = currentEpoch
				}
				me.LastSeen = time.Now()
				msg := &pb.NodeMessage{
					Type:        "ANNOUNCE",
					LotId:       me.LotID,
					T:           me.T,
					ActiveOps:   me.ActiveOps,
					NetOps:      me.NetOps,
					JoinedAt:    me.JoinedAt,
					LastTopTick: me.LastTopTick,
					LastEpoch:   me.LastEpoch,
				}
				messages = append(messages, msg)
			}
			lc.ledger.mu.Unlock()
            var wg sync.WaitGroup
            for i := range messages {
                wg.Add(1)
                go func(i int) {
                    defer wg.Done()
                    if needsActiveOps && !lc.ledger.UseMock {
                        result, _ := t2api.GetActiveOps(lc.bearer, lc.number, messages[i].LotId)
                        messages[i].ActiveOps = result
                    }
                }(i)
            }
            wg.Wait()
			for _, msg := range messages {
				lc.broadcaster.publish(node.Topic, msg)
			}
		}
	}
}
