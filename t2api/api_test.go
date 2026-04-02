package t2api

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestGetTop4IDsAsync_ChannelReturns(t *testing.T) {
	ch := GetTop4IDsAsync("data", 10, 100)

	result := <-ch
	if result.Err != nil {
		t.Logf("Expected error in test environment (no real API): %v", result.Err)
	}
}

func TestGetTop4IDsAsync_MultipleCalls(t *testing.T) {
	var callCount int32

	for i := 0; i < 3; i++ {
		ch := GetTop4IDsAsync("data", 10, 100)
		go func() {
			<-ch
			atomic.AddInt32(&callCount, 1)
		}()
	}

	var wg sync.WaitGroup
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func() {
			ch := GetTop4IDsAsync("data", 10, 100)
			<-ch
			atomic.AddInt32(&callCount, 1)
			wg.Done()
		}()
	}
	wg.Wait()
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

func TestGetTop4Result_Structure(t *testing.T) {
	result := GetTop4Result{
		Lots: []LotInfo{{ID: "lot-1", IsBot: false}},
		Err:  nil,
	}

	if len(result.Lots) != 1 {
		t.Errorf("Expected 1 lot, got %d", len(result.Lots))
	}
	if result.Err != nil {
		t.Error("Expected nil error")
	}
}

func TestUOMDisplayName(t *testing.T) {
	tests := []struct {
		uom      string
		expected string
	}{
		{"gb", "ГБ"},
		{"min", "Минуты"},
		{"sms", "SMS"},
		{"unknown", "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.uom, func(t *testing.T) {
			result := UOMDisplayName(tt.uom)
			if result != tt.expected {
				t.Errorf("UOMDisplayName(%q) = %q, want %q", tt.uom, result, tt.expected)
			}
		})
	}
}
