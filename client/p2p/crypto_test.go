package p2p

import (
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"market-denet/pb"
)

func TestSignMessage_CreatesSignature(t *testing.T) {
	priv, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}

	msg := &pb.NodeMessage{
		Type:   "ANNOUNCE",
		LotId:  "test-lot",
		PeerId: "test-peer",
		T:      100,
		R:      5,
	}

	sig, err := SignMessage(priv, msg)
	if err != nil {
		t.Fatalf("SignMessage failed: %v", err)
	}

	if len(sig) == 0 {
		t.Error("Signature should not be empty")
	}
}

func TestSignMessage_DifferentMessages_DifferentSigs(t *testing.T) {
	priv, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}

	msg1 := &pb.NodeMessage{Type: "ANNOUNCE", LotId: "lot-1"}
	msg2 := &pb.NodeMessage{Type: "ANNOUNCE", LotId: "lot-2"}

	sig1, _ := SignMessage(priv, msg1)
	sig2, _ := SignMessage(priv, msg2)

	if string(sig1) == string(sig2) {
		t.Error("Different messages should produce different signatures")
	}
}

func TestVerifySignature_Valid(t *testing.T) {
	priv, pubKey, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}

	pID, _ := peer.IDFromPublicKey(pubKey)

	msg := &pb.NodeMessage{
		Type:   "ANNOUNCE",
		LotId:  "test-lot",
		PeerId: pID.String(),
		T:      100,
		R:      5,
	}

	sig, _ := SignMessage(priv, msg)

	valid := VerifySignature(pubKey, msg, sig)
	if !valid {
		t.Error("Valid signature should pass verification")
	}
}

func TestVerifySignature_InvalidSignature(t *testing.T) {
	_, pubKey, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}

	pID, _ := peer.IDFromPublicKey(pubKey)

	msg := &pb.NodeMessage{
		Type:   "ANNOUNCE",
		LotId:  "test-lot",
		PeerId: pID.String(),
	}

	invalidSig := []byte("invalid-signature")

	valid := VerifySignature(pubKey, msg, invalidSig)
	if valid {
		t.Error("Invalid signature should fail verification")
	}
}

func TestVerifySignature_WrongKey(t *testing.T) {
	priv1, pubKey1, _ := crypto.GenerateEd25519Key(nil)
	_, pubKey2, _ := crypto.GenerateEd25519Key(nil)

	pID, _ := peer.IDFromPublicKey(pubKey1)

	msg := &pb.NodeMessage{
		Type:   "ANNOUNCE",
		LotId:  "test-lot",
		PeerId: pID.String(),
	}

	sig, _ := SignMessage(priv1, msg)

	valid := VerifySignature(pubKey2, msg, sig)
	if valid {
		t.Error("Signature with wrong key should fail verification")
	}
}

func TestVerifySignature_ModifiedMessage(t *testing.T) {
	priv, pubKey, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}

	pID, _ := peer.IDFromPublicKey(pubKey)

	msg := &pb.NodeMessage{
		Type:   "ANNOUNCE",
		LotId:  "test-lot",
		PeerId: pID.String(),
		T:      100,
	}

	sig, _ := SignMessage(priv, msg)

	msg.T = 999
	valid := VerifySignature(pubKey, msg, sig)
	if valid {
		t.Error("Modified message should fail verification")
	}
}
