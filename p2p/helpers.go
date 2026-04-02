package p2p

import (
	"math/rand"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

func checkDutyRoleInternal(ledger *Ledger, numDutyNodes int, node *Node) bool {
	activePeers := ledger.GetActivePeers(node)
	if len(activePeers) == 0 {
		return true
	}

	if len(activePeers) <= numDutyNodes {
		return true
	}

	peerTrust := make(map[peer.ID]float64)
	ledger.mu.RLock()
	for _, participants := range ledger.Members {
		for _, p := range participants {
			peerTrust[p.PeerID] += p.TrustScore()
		}
	}
	ledger.mu.RUnlock()

	type weightedPeer struct {
		peer  peer.ID
		trust float64
	}
	var weighted []weightedPeer
	for _, pid := range activePeers {
		trust := peerTrust[pid]
		if trust < 1.0 {
			trust = 1.0
		}
		for i := 0; i < int(trust*10); i++ {
			weighted = append(weighted, weightedPeer{peer: pid, trust: trust})
		}
	}

	seed := (time.Now().Unix() + NetworkTimeOffset) / 300
	rng := rand.New(rand.NewSource(seed))
	rng.Shuffle(len(weighted), func(i, j int) {
		weighted[i], weighted[j] = weighted[j], weighted[i]
	})

	seen := make(map[peer.ID]bool)
	var dutyNodes []peer.ID
	for _, w := range weighted {
		if !seen[w.peer] {
			seen[w.peer] = true
			dutyNodes = append(dutyNodes, w.peer)
			if len(dutyNodes) >= numDutyNodes {
				break
			}
		}
	}

	for _, p := range dutyNodes {
		if p == node.Host.ID() {
			return true
		}
	}
	return false
}
