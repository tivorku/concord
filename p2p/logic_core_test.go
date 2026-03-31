package p2p

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"market-denet/t2api"
)

func TestAnalyzeTarget_IsBot(t *testing.T) {
	ledger := NewLedger()
	priv, _, _ := crypto.GenerateEd25519Key(nil)
	lc := InitLogicCore(ledger, []string{"lot-1"}, 10, 100, priv, []*t2api.Account{}, nil)

	result := lc.AnalyzeTarget("any-lot", true)
	if result {
		t.Error("AnalyzeTarget should return false for bots")
	}
}

func TestAnalyzeTarget_KnownLot(t *testing.T) {
	ledger := NewLedger()
	priv, _, _ := crypto.GenerateEd25519Key(nil)
	lc := InitLogicCore(ledger, []string{"lot-1"}, 10, 100, priv, []*t2api.Account{}, nil)

	pID, pubKey := generateTestPeer()
	now := time.Now().Unix()
	epoch := GetCurrentEpoch()
	ledger.Update("lot-1", pID, pubKey, 100, 5, now, now, epoch, "test-acc")

	result := lc.AnalyzeTarget("lot-1", false)
	if result {
		t.Error("AnalyzeTarget should return false for known lots")
	}
}

func TestAnalyzeTarget_UnknownLot(t *testing.T) {
	ledger := NewLedger()
	priv, _, _ := crypto.GenerateEd25519Key(nil)
	lc := InitLogicCore(ledger, []string{"lot-1"}, 10, 100, priv, []*t2api.Account{}, nil)

	result := lc.AnalyzeTarget("unknown-lot", false)
	if !result {
		t.Error("AnalyzeTarget should return true for unknown lots")
	}
}
