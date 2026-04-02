package p2p

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"
	pb "market-denet/pb"
)

const (
	TestUOM       = "data"
	TestVolume    = 10
	TestValue     = 100
	TestTimeLimit = 20 * time.Second
)

type TestNode struct {
	ID        int
	Host      host.Host
	Ledger    *Ledger
	LogicCore *LogicCore
	Topic     *pubsub.Topic
	Sub       *pubsub.Subscription
	LotID     string
}

func createTestNode(ctx context.Context, id int) (*TestNode, error) {
	rand.Seed(time.Now().UnixNano() + int64(id))

	priv, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		return nil, err
	}

	h, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		return nil, err
	}

	ledger := NewLedger()

	lotID := fmt.Sprintf("test-lot-%d", id)

	now := time.Now().Unix() + NetworkTimeOffset
	ledger.Update(lotID, h.ID(), priv.GetPublic(), 0, 0, now, 0, GetCurrentEpoch())

	lc := InitLogicCore(ledger, []string{lotID}, TestVolume, TestValue, TestUOM, priv, "mock", "123", nil)

	params := pubsub.DefaultGossipSubParams()
	params.HeartbeatInterval = 500 * time.Millisecond
	ps, err := pubsub.NewGossipSub(ctx, h,
		pubsub.WithPeerExchange(true),
		pubsub.WithFloodPublish(true),
		pubsub.WithGossipSubParams(params),
	)
	if err != nil {
		h.Close()
		return nil, err
	}

	topic, err := ps.Join(fmt.Sprintf("test-segment-%s-%d-%d", TestUOM, TestVolume, TestValue))
	if err != nil {
		h.Close()
		return nil, err
	}

	sub, err := topic.Subscribe()
	if err != nil {
		topic.Close()
		h.Close()
		return nil, err
	}

	return &TestNode{
		ID:        id,
		Host:      h,
		Ledger:    ledger,
		LogicCore: lc,
		Topic:     topic,
		Sub:       sub,
		LotID:     lotID,
	}, nil
}

func connectNodes(h1, h2 host.Host) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	addrInfo := peer.AddrInfo{
		ID:    h2.ID(),
		Addrs: h2.Addrs(),
	}
	return h1.Connect(ctx, addrInfo)
}

func startMessageHandler(ctx context.Context, tn *TestNode, lc *LogicCore) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				msg, err := tn.Sub.Next(ctx)
				if err != nil {
					continue
				}

				senderPID := msg.ReceivedFrom
				if senderPID == tn.Host.ID() {
					continue
				}

				var pm pb.NodeMessage
				if err := proto.Unmarshal(msg.Data, &pm); err != nil {
					continue
				}

				pID, _ := peer.Decode(pm.PeerId)

				if !lc.VerifyIncomingMessage(&pm, pID) {
					continue
				}

				m := NodeMessage{
					Type:        pm.Type,
					LotID:       pm.LotId,
					PeerID:      pID,
					T:           pm.T,
					R:           pm.R,
					JoinedAt:    pm.JoinedAt,
					LastEpoch:   pm.LastEpoch,
					LastTopTick: pm.LastTopTick,
					IsBot:       pm.IsBot,
					Signature:   pm.Signature,
				}
				lc.HandleMessage(ctx, &Node{
					Host:   tn.Host,
					Topic:  tn.Topic,
					Ledger: tn.Ledger,
					Ctx:    ctx,
				}, m)
			}
		}
	}()
}

func TestIntegration_FourNodes(t *testing.T) {
	t.Log("=== Интеграционный тест: 4 ноды ===")
	t.Logf("Сегмент: Volume=%d, Value=%d", TestVolume, TestValue)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodes := make([]*TestNode, 4)

	t.Log("Создание 4 нод...")

	for i := 0; i < 4; i++ {
		node, err := createTestNode(ctx, i)
		if err != nil {
			t.Fatalf("Failed to create node %d: %v", i, err)
		}
		nodes[i] = node
		t.Logf("Node %d: %s", i, node.Host.ID().String()[:16])
	}

	for i := 0; i < 4; i++ {
		defer func(idx int) {
			nodes[idx].Topic.Close()
			nodes[idx].Host.Close()
		}(i)
	}

	t.Log("Соединение нод (полный граф)...")

	connectedPairs := 0
	for i := 0; i < 4; i++ {
		for j := i + 1; j < 4; j++ {
			if err := connectNodes(nodes[i].Host, nodes[j].Host); err != nil {
				t.Logf("Failed to connect Node %d <-> Node %d: %v", i, j, err)
			} else {
				connectedPairs++
				t.Logf("Node %d <-> Node %d: connected", i, j)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	t.Logf("Соединено пар: %d/6", connectedPairs)

	t.Log("Ожидание установления соединений...")
	time.Sleep(2 * time.Second)

	t.Log("Запуск логики и обработчиков сообщений на всех нодах...")

	for i, n := range nodes {
		node := &Node{
			Host:   n.Host,
			Topic:  n.Topic,
			Ledger: n.Ledger,
			Ctx:    ctx,
		}
		go n.LogicCore.Run(ctx, node)
		startMessageHandler(ctx, n, n.LogicCore)
		t.Logf("Node %d: запущен (лот: %s)", i, n.LotID)
	}

	t.Logf("Ожидание %v для обмена сообщениями...", TestTimeLimit)

	time.Sleep(TestTimeLimit)

	t.Log("=== Результаты ===")

	allPassed := true

	for i, n := range nodes {
		n.Ledger.mu.RLock()
		memberCount := len(n.Ledger.Members)
		totalLots := 0
		for _, parts := range n.Ledger.Members {
			totalLots += len(parts)
		}
		n.Ledger.mu.RUnlock()

		t.Logf("Node %d: %d peers, %d lots", i, memberCount, totalLots)

		if memberCount < 4 {
			t.Logf("FAIL: Node %d видит только %d нод из 4", i, memberCount)
			allPassed = false
		}

		if totalLots < 4 {
			t.Logf("FAIL: Node %d видит только %d лотов из 4", i, totalLots)
			allPassed = false
		}
	}

	t.Log("=== Тест завершён ===")

	if !allPassed {
		t.Error("Не все ноды видят друг друга")
	}
}
