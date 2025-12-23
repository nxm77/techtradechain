package libp2ppeer

import (
	"techtradechain.com/techtradechain/common/v2/helper/libp2pcrypto"
	b58 "github.com/mr-tron/base58/base58"
	mh "github.com/multiformats/go-multihash"
)

var AdvancedEnableInlining = true

const maxInlineKeyLength = 42

type ID string

// Pretty returns a base58-encoded string representation of the ID.
func (id ID) Pretty() string {
	return IDB58Encode(id)
}

// IDB58Encode returns the base58-encoded multihash representation of the ID.
//
// Deprecated: Use Encode.
func IDB58Encode(id ID) string {
	return b58.Encode([]byte(id))
}

// IDFromPublicKey returns the Peer ID corresponding to the public key pk.
// nolint: staticcheck
func IDFromPublicKey(pk libp2pcrypto.PubKey) (ID, error) {
	b, err := pk.Bytes()
	if err != nil {
		return "", err
	}
	var alg uint64 = mh.SHA2_256
	if AdvancedEnableInlining && len(b) <= maxInlineKeyLength {
		alg = mh.ID
	}
	hash, _ := mh.Sum(b, alg, -1)
	return ID(hash), nil
}
