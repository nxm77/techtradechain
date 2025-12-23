// nolint
package dilithium

import (
	"crypto"
	"encoding/hex"
	"errors"

	"techtradechain.com/techtradechain/common/v2/crypto/hash"

	bccrypto "techtradechain.com/techtradechain/common/v2/crypto"
	dilithium "github.com/kudelskisecurity/crystals-go/crystals-dilithium"
)

var _ bccrypto.PublicKey = (*PublicKey)(nil)

// PublicKey is pqc public key
type PublicKey struct {
	K   []byte //PackPK
	Dlt *dilithium.Dilithium
}

func (p *PublicKey) Bytes() ([]byte, error) {
	if p.Dlt == nil || p.K == nil {
		return nil, errors.New("PublicKey is invalid")
	}
	return p.K, nil
}

func (p *PublicKey) Type() bccrypto.KeyType {
	return bccrypto.DILITHIUM2
}

func (p *PublicKey) String() (string, error) {
	pkBytes, err := p.Bytes()
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(pkBytes), nil
}

func (p *PublicKey) Verify(data []byte, sig []byte) (bool, error) {
	return p.Dlt.Verify(p.K, data, sig), nil
}

func (p *PublicKey) VerifyWithOpts(data []byte, sig []byte, opts *bccrypto.SignOpts) (bool, error) {
	if opts == nil {
		return p.Verify(data, sig)
	}
	msg, err := hash.Get(opts.Hash, data)
	if err != nil {
		return false, err
	}
	return p.Verify(msg, sig)
}

func (p *PublicKey) ToStandardKey() crypto.PublicKey {
	return p
}
