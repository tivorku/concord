package p2p

import (
	"context"
	"math/rand"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"google.golang.org/protobuf/proto"
	"concord/pb"
	"concord/t2api"
)

type Broadcaster struct {
	ledger  *Ledger
	myID    string
	bearer  string
	number  string
	useMock bool
}

func NewBroadcaster(ledger *Ledger, myID, bearer, number string, useMock bool) *Broadcaster {
	return &Broadcaster{
		ledger:  ledger,
		myID:    myID,
		bearer:  bearer,
		number:  number,
		useMock: useMock,
	}
}

func (b *Broadcaster) publish(topic *pubsub.Topic, msg *pb.NodeMessage) {
	raw, err := proto.Marshal(msg)
	if err != nil {
		return
	}
	topic.Publish(context.Background(), raw)
}
func (b *Broadcaster) broadcastViolation(m NodeMessage, topic *pubsub.Topic) {

	report := &pb.NodeMessage{
		Type:        "VIOLATION",
		LotId:       m.LotID,
		ActiveOps:   m.ActiveOps,
		NetOps:      m.NetOps,
	}
	b.publish(topic, report)
}
func (b *Broadcaster) broadcastRocketFired(topic *pubsub.Topic, lotID string) {
	b.ledger.mu.Lock()

	participants := b.ledger.Members[b.myID]
	var me *Participant
	for _, p := range participants {
		if p.LotID == lotID {
			me = p
			break
		}
	}
	if me == nil {
		b.ledger.mu.Unlock()
		return
	}
	// mocking time in top
	if b.useMock {
	    me.T += int64(rand.Intn(3)) + 1
	}
	me.NetOps--
	me.LastTopTick = time.Now().Unix() + NetworkTimeOffset.Load()
	currentEpoch := GetCurrentEpoch()
	msg := &pb.NodeMessage{
		Type:        "ROCKET",
		LotId:       lotID,
		T:           me.T,
		ActiveOps:   me.ActiveOps,
		NetOps:      me.NetOps,
		LastTopTick: me.LastTopTick,
		JoinedAt:    me.JoinedAt,
		LastEpoch:   currentEpoch,
	}
	b.ledger.mu.Unlock()
	result, err := t2api.GetActiveOps(b.bearer, b.number, lotID)
	if err == nil {
	    msg.ActiveOps = result
	    b.ledger.mu.Lock()
	    me.ActiveOps = result
	    b.ledger.mu.Unlock()
	}
	b.publish(topic, msg)
}

func (b *Broadcaster) broadcastTopStatus(topic *pubsub.Topic, info t2api.LotInfo) {
	msg := &pb.NodeMessage{
		Type:   "TOP",
		LotId:  info.ID,
		IsBot:  info.IsBot,
	}
	b.publish(topic, msg)
}

func (b *Broadcaster) broadcastSyncCorrection(m NodeMessage, topic *pubsub.Topic) {
	b.ledger.mu.RLock()
	defer b.ledger.mu.RUnlock()

	participants := b.ledger.Members[m.PeerID.String()]
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
		LotId:       p.LotID,
		PeerId:      m.PeerID.String(),
		T:           p.T,
		ActiveOps:   p.ActiveOps,
		NetOps:      p.NetOps,
		LastEpoch:   p.LastEpoch,
		LastTopTick: p.LastTopTick,
	}
	b.publish(topic, correction)
}
func (b *Broadcaster) broadcastUnbanSync(m NodeMessage, topic *pubsub.Topic) {
    // приравниваем NetOps к ActiveOps для снятия бана
	unban := &pb.NodeMessage{
		Type:        "UNBAN",
		LotId:       m.LotID,
	}
	b.publish(topic, unban)
}