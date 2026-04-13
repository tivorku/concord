package p2p

import (
	"context"
	"math/rand"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"google.golang.org/protobuf/proto"
	"market-denet/pb"
	"market-denet/t2api"
)

type Broadcaster struct {
	ledger  *Ledger
	privKey crypto.PrivKey
	myID    string
}

func NewBroadcaster(ledger *Ledger, privKey crypto.PrivKey, myID string) *Broadcaster {
	return &Broadcaster{
		ledger:  ledger,
		privKey: privKey,
		myID:    myID,
	}
}

func (b *Broadcaster) publish(topic *pubsub.Topic, msg *pb.NodeMessage) {
	sig, err := SignMessage(b.privKey, msg)
	if err == nil {
		msg.Signature = sig
	}
	raw, err := proto.Marshal(msg)
	if err != nil {
		return
	}
	topic.Publish(context.Background(), raw)
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
	me.R++
	me.T = me.T + int64(rand.Intn(3)) + 1
	me.LastTopTick = time.Now().Unix() + NetworkTimeOffset
	currentEpoch := GetCurrentEpoch()
	msg := &pb.NodeMessage{
		Type:        "ROCKET",
		LotId:       lotID,
		PeerId:      b.myID,
		T:           me.T,
		R:           me.R,
		LastTopTick: me.LastTopTick,
		JoinedAt:    me.JoinedAt,
		LastEpoch:   currentEpoch,
	}
	b.ledger.mu.Unlock()
	b.publish(topic, msg)
}

func (b *Broadcaster) broadcastTopStatus(topic *pubsub.Topic, info t2api.LotInfo) {
	msg := &pb.NodeMessage{
		Type:   "TOP",
		LotId:  info.ID,
		PeerId: b.myID,
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
		LotId:       m.LotID,
		PeerId:      m.PeerID.String(),
		T:           p.T,
		R:           p.R,
		LastEpoch:   p.LastEpoch,
		LastTopTick: p.LastTopTick,
	}
	b.publish(topic, correction)
}
