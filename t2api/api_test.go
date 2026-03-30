package t2api

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestGetTop4IDsAsync_CallbackCalled(t *testing.T) {
	var called int32
	var wg sync.WaitGroup
	wg.Add(1)

	GetTop4IDsAsync(10, 100, func(lots []LotInfo, err error) {
		atomic.AddInt32(&called, 1)
		wg.Done()
	})

	wg.Wait()

	if called != 1 {
		t.Errorf("Callback should be called exactly once, got %d", called)
	}
}

func TestGetTop4IDsAsync_MultipleCalls(t *testing.T) {
	var callCount int32

	for i := 0; i < 3; i++ {
		GetTop4IDsAsync(10, 100, func(lots []LotInfo, err error) {
			atomic.AddInt32(&callCount, 1)
		})
	}
}

func TestLotInfo_Structure(t *testing.T) {
	info := LotInfo{
		ID:    "test-lot-id",
		IsBot: true,
	}

	if info.ID != "test-lot-id" {
		t.Errorf("Expected ID 'test-lot-id', got %s", info.ID)
	}

	if !info.IsBot {
		t.Error("Expected IsBot to be true")
	}
}
