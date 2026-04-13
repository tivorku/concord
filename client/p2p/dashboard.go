package p2p

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
)

type Dashboard struct {
	ledger   *Ledger
	myLotIDs []string
	volume   int
	value    int
}

func NewDashboard(ledger *Ledger, myLotIDs []string, volume, value int) *Dashboard {
	return &Dashboard{
		ledger:   ledger,
		myLotIDs: myLotIDs,
		volume:   volume,
		value:    value,
	}
}

func ClearScreen() {
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.Command("cmd", "/c", "cls")
	} else {
		cmd = exec.Command("clear")
	}
	cmd.Stdout = os.Stdout
	cmd.Run()
}

func (d *Dashboard) isMyLot(lotID string) bool {
	for _, id := range d.myLotIDs {
		if id == lotID {
			return true
		}
	}
	return false
}

func (d *Dashboard) ShowDashboard(node *Node, rendezvous string, amITheShooter func(node *Node) (string, bool)) {
	doubleDelim := "=================================================================="
	delim := "------------------------------------------------------------------"
	ClearScreen()

	fmt.Println(doubleDelim)
	fmt.Printf("          СЕГМЕНТ: %d ГБ / %d РУБ | %s\n", d.volume, d.value, rendezvous)
	fmt.Println(doubleDelim)
	fmt.Printf("%-6s | %-12s | %-4s | %-4s | %-4s | %-6s | %-4s\n", "#", "PeerID", "T", "R", "W", "P", "Trust")
	fmt.Println(delim)

	items := d.ledger.GetQueueWithMetrics(node)

	peerTrust := make(map[string]float64)
	for _, item := range items {
		if item.Trust > peerTrust[item.PeerID] {
			peerTrust[item.PeerID] = item.Trust
		}
	}

	dutyLotID, _ := amITheShooter(node)

	for i, item := range items {
		prefix := "  "
		suffix := "   "
		if d.isMyLot(item.LotID) {
			prefix = ">>"
			if item.LotID == dutyLotID {
				suffix = " * "
			}
		}
		shortPID := item.PeerID
		if len(shortPID) > 8 {
			shortPID = shortPID[len(shortPID)-8:]
		}

		trust := peerTrust[item.PeerID]
		fmt.Printf("%s%-1d%s | %-12s | %-4d | %-4d | %-4d | %-6.2f | %-4.0f\n",
			prefix, i+1, suffix, shortPID, item.T, item.R, item.WaitTime, item.Priority, trust)
	}
	fmt.Println(doubleDelim)
}

func (d *Dashboard) GetMyLotsSortedByPriority(node *Node) []Item {
	items := d.ledger.GetQueueWithMetrics(node)
	var myItems []Item
	for _, item := range items {
		if d.isMyLot(item.LotID) {
			myItems = append(myItems, item)
		}
	}
	return myItems
}
