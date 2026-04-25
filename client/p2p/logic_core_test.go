package p2p

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
)

func TestAnalyzeTarget_IsBot(t *testing.T) {
	ledger := NewLedger()
	priv, _, _ := crypto.GenerateEd25519Key(nil)
	lc := InitLogicCore(ledger, []string{"lot-1"}, 10, 150, "gb", priv, "bearer", "number", nil)

	result := lc.AnalyzeTarget("any-lot", true)
	if result {
		t.Error("AnalyzeTarget should return false for bots")
	}
}

func TestAnalyzeTarget_KnownLot(t *testing.T) {
	ledger := NewLedger()
	priv, _, _ := crypto.GenerateEd25519Key(nil)
	lc := InitLogicCore(ledger, []string{"lot-1"}, 10, 150, "gb", priv, "bearer", "number", nil)

	pID, pubKey := generateTestPeer()
	now := time.Now().Unix()
	epoch := GetCurrentEpoch()
	ledger.Update("lot-1", pID, pubKey, 100, 5, now, now, epoch)

	result := lc.AnalyzeTarget("lot-1", false)
	if result {
		t.Error("AnalyzeTarget should return false for known lots")
	}
}

func TestAnalyzeTarget_UnknownLot(t *testing.T) {
	ledger := NewLedger()
	priv, _, _ := crypto.GenerateEd25519Key(nil)
	lc := InitLogicCore(ledger, []string{"lot-1"}, 10, 150, "gb", priv, "bearer", "number", nil)

	result := lc.AnalyzeTarget("unknown-lot", false)
	if !result {
		t.Error("AnalyzeTarget should return true for unknown lots")
	}
}

func TestShooter_CanShoot(t *testing.T) {
	ledger := NewLedger()
	s := NewShooter("bearer", "number", []string{"lot-1"}, ledger)

	if !s.CanShoot("lot-1") {
		t.Error("First shot should be allowed")
	}

	if s.CanShoot("lot-1") {
		t.Error("Second shot within 5s should be blocked")
	}
}

func TestShooter_TryLock(t *testing.T) {
	s := NewShooter("bearer", "number", []string{"lot-1"}, nil)

	if !s.TryLock() {
		t.Error("First lock should succeed")
	}

	if s.TryLock() {
		t.Error("Second lock while executing should fail")
	}

	s.Unlock()

	if !s.TryLock() {
		t.Error("Lock after unlock should succeed")
	}
}

func TestBroadcaster_New(t *testing.T) {
	ledger := NewLedger()
	priv, _, _ := crypto.GenerateEd25519Key(nil)
	b := NewBroadcaster(ledger, priv, "test-peer")

	if b == nil {
		t.Error("NewBroadcaster should not return nil")
	}
	if b.myID != "test-peer" {
		t.Errorf("Expected myID 'test-peer', got '%s'", b.myID)
	}
}

func TestDashboard_IsMyLot(t *testing.T) {
	ledger := NewLedger()
	d := NewDashboard(ledger, []string{"lot-1", "lot-2"}, 10, 150)

	if !d.isMyLot("lot-1") {
		t.Error("lot-1 should be my lot")
	}
	if !d.isMyLot("lot-2") {
		t.Error("lot-2 should be my lot")
	}
	if d.isMyLot("lot-3") {
		t.Error("lot-3 should not be my lot")
	}
}
