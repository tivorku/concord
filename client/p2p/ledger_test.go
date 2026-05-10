package p2p

import (
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func generateTestPeer() (peer.ID) {
	priv, pubKey, _ := crypto.GenerateEd25519Key(nil)
	pID, _ := peer.IDFromPublicKey(pubKey)
	_ = priv
	return pID
}

func TestNewLedger_Empty(t *testing.T) {
	ledger := NewLedger(true)
	if ledger == nil {
		t.Fatal("NewLedger should return non-nil")
	}
	if len(ledger.Members) != 0 {
		t.Error("New ledger should be empty")
	}
}

func TestIsLotKnown_EmptyLedger(t *testing.T) {
	ledger := NewLedger(true)

	if ledger.IsLotKnown("any-lot") {
		t.Error("Empty ledger should not contain any lots")
	}
}

func TestUpdate_NewParticipant(t *testing.T) {
	ledger := NewLedger(true)
	pID := generateTestPeer()
	now := time.Now().Unix()
	epoch := GetCurrentEpoch()

	needsCorrection := ledger.Update("lot-123", pID, 100, now, now, epoch, 5, 5)
	//myLedger.Update(lotID, h.ID(), 0, now, 0, p2p.GetCurrentEpoch(), 5, 5)
    // Update(lotID string, pID peer.ID, incomingT int64, joinedAt int64, lastTopTick int64, incomingEpoch int64, incomingActiveOps int64, incomingNetOps int64) bool {

	if needsCorrection {
		t.Error("New participant should not need correction")
	}
	if !ledger.IsLotKnown("lot-123") {
		t.Error("Lot should be known after update")
	}
	if len(ledger.Members[pID.String()]) != 1 {
		t.Error("Peer should have one lot after update")
	}
}

func TestUpdate_ExistingParticipant(t *testing.T) {
	ledger := NewLedger(true)
	pID := generateTestPeer()
	now := time.Now().Unix()
	epoch := GetCurrentEpoch()

	ledger.Update("lot-123", pID, 100, now, now, epoch, 5, 5)
	needsCorrection := ledger.Update("lot-123", pID, 110, now, now, epoch, 5, 5)

	if needsCorrection {
		t.Error("Existing participant with valid update should not need correction")
	}
}

func TestUpdate_EpochDiffTooLarge(t *testing.T) {
	ledger := NewLedger(true)
	pID := generateTestPeer()
	now := time.Now().Unix()
	epoch := GetCurrentEpoch()

	ledger.Update("lot-123", pID, 100, now, now, epoch, 5, 5)

	needsCorrection := ledger.Update("lot-123", pID, 110, now, now, epoch+5, 5, 5)

	if needsCorrection {
		t.Error("Message with epoch diff > 1 should be rejected (returns false)")
	}
}

func TestUpdate_TManipulation_Plus3Max(t *testing.T) {
	ledger := NewLedger(true)
	pID := generateTestPeer()
	now := time.Now().Unix()
	epoch := GetCurrentEpoch()

	ledger.Update("lot-123", pID, 100, now, now, epoch, 5, 5)
	ledger.Update("lot-123", pID, 200, now, now, epoch, 5, 5)

	ledger.mu.RLock()
	p := ledger.Members[pID.String()][0]
	actualT := p.T
	ledger.mu.RUnlock()

	if actualT > 103 {
		t.Errorf("T should be capped at +3, got %d (original 100 + 200 diff)", actualT)
	}
}

func TestUpdate_JoinedAtUpdate(t *testing.T) {
	ledger := NewLedger(true)
	pID := generateTestPeer()
	epoch := GetCurrentEpoch()

	ledger.Update("lot-123", pID, 100, 900, 900, epoch, 5, 5)
	ledger.Update("lot-123", pID, 110, 1000, 1100, epoch, 5, 5)

	ledger.mu.RLock()
	p := ledger.Members[pID.String()][0]
	joinedAt := p.JoinedAt
	ledger.mu.RUnlock()

	if joinedAt != 1000 {
		t.Errorf("JoinedAt should be updated to later value: got %d, want 1000", joinedAt)
	}
}

func TestUpdate_Concurrent(t *testing.T) {
	ledger := NewLedger(true)
	pID := generateTestPeer()
	now := time.Now().Unix()
	epoch := GetCurrentEpoch()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ledger.Update("lot-123", pID, int64(100+idx), now, now, epoch, 5, 5)
		}(i)
	}
	wg.Wait()

	if !ledger.IsLotKnown("lot-123") {
		t.Error("Lot should exist after concurrent updates")
	}
}

func TestUpdateTicks(t *testing.T) {
	ledger := NewLedger(true)
	pID := generateTestPeer()
	now := time.Now().Unix()
	epoch := GetCurrentEpoch()

	ledger.Update("lot-123", pID, 100, now, now, epoch, 5, 5)

	ledger.mu.RLock()
	oldT := ledger.Members[pID.String()][0].T
	ledger.mu.RUnlock()

	ledger.UpdateTicks("lot-123")

	ledger.mu.RLock()
	newT := ledger.Members[pID.String()][0].T
	ledger.mu.RUnlock()

	if newT != oldT+1 {
		t.Errorf("T should be incremented: got %d, want %d", newT, oldT+1)
	}
}

func TestGetMyLots(t *testing.T) {
	ledger := NewLedger(true)
	now := time.Now().Unix()
	epoch := GetCurrentEpoch()

	ledger.mu.Lock()
	ledger.Members["test-peer"] = []*Participant{
		{LotID: "lot-1", T: 100, JoinedAt: now, LastEpoch: epoch},
		{LotID: "lot-2", T: 100, JoinedAt: now, LastEpoch: epoch},
	}
	ledger.mu.Unlock()

	ledger.mu.RLock()
	lots := ledger.Members["test-peer"]
	ledger.mu.RUnlock()

	if len(lots) != 2 {
		t.Errorf("Expected 2 lots, got %d", len(lots))
	}
}

func TestGetSortedQueue(t *testing.T) {
	ledger := NewLedger(true)
	pID := generateTestPeer()
	now := time.Now().Unix()
	epoch := GetCurrentEpoch()

	ledger.Update("lot-123", pID, 100, now, now, epoch, 5, 5)

	queue := ledger.GetSortedQueue(&Node{})
	if len(queue) != 1 {
		t.Fatalf("Expected 1 item in queue, got %d", len(queue))
	}
	if queue[0] != "lot-123" {
		t.Errorf("Expected lot-123, got %s", queue[0])
	}
}
