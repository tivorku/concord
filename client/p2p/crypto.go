package p2p

import (
	"fmt"

	"github.com/libp2p/go-libp2p/core/crypto"
	pb "market-denet/pb"
)

func SignMessage(privKey crypto.PrivKey, msg *pb.NodeMessage) ([]byte, error) {
	data := serializeForSigning(msg)
	return privKey.Sign(data)
}

func VerifySignature(pubKey crypto.PubKey, msg *pb.NodeMessage, signature []byte) bool {
	data := serializeForSigning(msg)
	ok, _ := pubKey.Verify(data, signature)
	return ok
}

func serializeForSigning(msg *pb.NodeMessage) []byte {
	data := fmt.Sprintf("%s|%s|%s|%d|%d|%d|%d|%d|%d|%t",
		msg.Type,
		msg.LotId,
		msg.PeerId,
		msg.T,
		msg.ActiveOps,
		msg.NetOps,
		msg.JoinedAt,
		msg.LastEpoch,
		msg.LastTopTick,
		msg.IsBot,
	)
	return []byte(data)
}