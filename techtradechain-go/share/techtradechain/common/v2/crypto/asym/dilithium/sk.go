// nolint
package dilithium

import (
	"crypto"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"techtradechain.com/techtradechain/common/v2/crypto/hash"

	bccrypto "techtradechain.com/techtradechain/common/v2/crypto"
	dilithium "github.com/kudelskisecurity/crystals-go/crystals-dilithium"
)

var _ bccrypto.PrivateKey = (*PrivateKey)(nil)

type PrivateKey struct {
	SK  []byte //PackSK
	PK  []byte //PackPk
	Dlt *dilithium.Dilithium
}

func (key *PrivateKey) Bytes() ([]byte, error) {
	if key.SK == nil || key.Dlt == nil {
		return nil, errors.New("PrivateKey is invalid")
	}
	return key.SK, nil
}

func (key *PrivateKey) Type() bccrypto.KeyType {
	return bccrypto.DILITHIUM2
}

func (key *PrivateKey) String() (string, error) {
	keyBytes, err := key.Bytes()
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(keyBytes), nil
}

func (key *PrivateKey) Sign(data []byte) ([]byte, error) {
	return key.Dlt.Sign(key.SK, data), nil
}

func (key *PrivateKey) SignWithOpts(data []byte, opts *bccrypto.SignOpts) ([]byte, error) {
	if opts == nil {
		return key.Sign(data)
	}
	msg, err := hash.Get(opts.Hash, data)
	if err != nil {
		return nil, err
	}
	return key.Sign(msg)
}

func (key *PrivateKey) PublicKey() bccrypto.PublicKey {
	return &PublicKey{
		K:   key.PK,
		Dlt: key.Dlt,
	}
}

func (key *PrivateKey) ToStandardKey() crypto.PrivateKey {
	return &signer{PrivateKey: *key}
}

func New(keyType bccrypto.KeyType) (bccrypto.PrivateKey, error) {
	if keyType != bccrypto.DILITHIUM2 {
		return nil, fmt.Errorf("Invalid keyType, got %s, want %s",
			bccrypto.KeyType2NameMap[keyType], bccrypto.KeyType2NameMap[bccrypto.DILITHIUM2])
	}

	dlt := dilithium.NewDilithium2()
	pk, sk := dlt.KeyGen(nil)

	return &PrivateKey{
		SK:  sk,
		PK:  pk,
		Dlt: dlt,
	}, nil
}

//signer implements crypto.Signer
type signer struct {
	PrivateKey
}

func (s *signer) Public() crypto.PublicKey {
	return s.PublicKey()
}

func (s *signer) Sign(rand io.Reader, msg []byte, opts crypto.SignerOpts) ([]byte, error) {
	return s.PrivateKey.SignWithOpts(msg, nil)
}
