package dilithium

import (
	"crypto/x509/pkix"
	"encoding/asn1"
	"fmt"

	bccrypto "techtradechain.com/techtradechain/common/v2/crypto"
	dilithium "github.com/kudelskisecurity/crystals-go/crystals-dilithium"
)

type pkixPublicKey struct {
	Algo      pkix.AlgorithmIdentifier
	BitString asn1.BitString
}

// ParsePubKey is used parse pqc public key
func ParsePubKey(der []byte) (bccrypto.PublicKey, error) {
	pkixPub := pkixPublicKey{}
	var packPK []byte
	if _, err := asn1.Unmarshal(der, &pkixPub); err == nil {
		asn1Data := pkixPub.BitString.RightAlign()
		packPK = asn1Data
	} else {
		packPK = der
	}
	if len(packPK) != dilithium.Dilithium2SizePK {
		return nil, fmt.Errorf("invalid dilithium2 PK size, must be %d",
			dilithium.Dilithium2SizePK)
	}

	//fmt.Println("ParsePubKey:", hex.EncodeToString(packPK))

	return &PublicKey{Dlt: dilithium.NewDilithium2(), K: packPK}, nil
}

// ParsePrivateKey is used to parse pqc private key
func ParsePrivateKey(der []byte) (bccrypto.PrivateKey, error) {
	if len(der) != dilithium.Dilihtium2SizeSK {
		return nil, fmt.Errorf("invalid dilithium2 SK size, must be %d",
			dilithium.Dilihtium2SizeSK)
	}

	//fmt.Println("ParsePrivateKey:", hex.EncodeToString(der))

	return &PrivateKey{Dlt: dilithium.NewDilithium2(), SK: der}, nil
}
