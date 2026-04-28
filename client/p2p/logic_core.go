package p2p

import (
	"context"
	"fmt"
	"time"
	"sync"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"market-denet/pb"
	"market-denet/t2api"
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
}

func InitLogicCore(l *Ledger, myLotIDs []string, vol int, val int, uom string, privKey crypto.PrivKey, bearer, number string, node *Node) *LogicCore {
	var myID string
	if node != nil {
		myID = node.Host.ID().String()
	} else {
		pid, _ := peer.IDFromPublicKey(privKey.GetPublic())
		myID = pid.String()
	}
	shooter := NewShooter(bearer, number, myLotIDs, l)
	broadcaster := NewBroadcaster(l, privKey, myID, bearer, number)
	dashboard := NewDashboard(l, myLotIDs, vol, val)

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

func (lc *LogicCore) VerifyIncomingMessage(pm *pb.NodeMessage, senderID peer.ID) bool {
    switch pm.Type {
    case "SYNC", "VIOLATION", "UNBAN":
        return true
    }
	if len(pm.Signature) == 0 {
		fmt.Printf("[SECURITY] Message from %s has no signature\n", senderID)
		return false
	}
    if senderID.String() != pm.PeerId {
        fmt.Printf("[SECURITY] Sender %s != message author %s\n", senderID.String(), pm.PeerId)
        return false
    }
	pID, err := peer.Decode(pm.PeerId)
	if err != nil {
		fmt.Printf("[SECURITY] Cannot decode peer ID from message: %v\n", err)
		return false
	}

	pubKey, err := pID.ExtractPublicKey()
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
        if _, banned := lc.ledger.Blocklist[leaderLotID]; banned {
            fmt.Println("Этот лот находится в бане")
            return "", false
        }
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
	if m.PeerID != node.Host.ID() {
		fmt.Printf("[Debug] Пришел тип: %s | Лот: %s | От: %s | AO: %d | NO: %d\n", m.Type, m.LotID, m.PeerID, m.ActiveOps, m.NetOps)
	}
	timestamp, banned := lc.ledger.Blocklist[m.LotID]
	if banned {
	    if time.Since(timestamp) > 30*time.Minute {
	        go lc.broadcaster.broadcastUnbanSync(m, node.Topic)
	        delete(lc.ledger.Blocklist, m.LotID)
	    } else {
	        return
	    }
	}

	switch m.Type {
	case "ANNOUNCE", "ROCKET":
		pubKey, _ := m.PeerID.ExtractPublicKey()
		if m.NetOps - m.ActiveOps != 0 && !lc.ledger.UseMock {
		    lc.ledger.mu.Lock()
		    lc.ledger.Blocklist[m.LotID] = time.Now()
		    lc.ledger.mu.Unlock()
            go lc.broadcaster.broadcastViolation(m, node.Topic)
        }
		needsCorrection := lc.ledger.Update(m.LotID, m.PeerID, pubKey, m.T, m.JoinedAt, m.LastTopTick, m.LastEpoch, m.ActiveOps, m.NetOps)
		if needsCorrection && m.Type == "ANNOUNCE" {
			go lc.broadcaster.broadcastSyncCorrection(m, node.Topic)
		}

	case "TOP":
		lc.ledger.UpdateTicks(m.LotID, m.PeerID)
		status := lc.AnalyzeTarget(m.LotID, m.IsBot)

		if status {
			lotID, isMyTurn := lc.AmITheShooter(node)
			if isMyTurn {
				if lc.shooter.TryLock() {
					go lc.shooter.PerformExecution(ctx, node, lotID, lc.broadcaster)
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
		participants := lc.ledger.Members[m.PeerID.String()]
		var p *Participant
		for _, part := range participants {
			if part.LotID == m.LotID {
				p = part
				break
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
				go func() {
					result := <-ch
					if result.Err != nil {
						fmt.Println(result.Err)
						return
					}
					if len(result.Lots) > 0 {
						lc.broadcaster.broadcastTopStatus(node.Topic, result.Lots[0])
						m := NodeMessage{
							Type:   "TOP",
							LotID:  result.Lots[0].ID,
							PeerID: node.Host.ID(),
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
                            lc.ledger.Members[node.Host.ID().String()] = append(parts[:i], parts[i+1:]...)
                            break
                        }
                    }
                    return
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
					PeerId:      node.Host.ID().String(),
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
                    if needsActiveOps {
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
