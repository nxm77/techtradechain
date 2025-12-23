package crypto

import (
	bccrypto "techtradechain.com/techtradechain/common/v2/crypto"
	asymSM2 "techtradechain.com/techtradechain/common/v2/crypto/asym/sm2"
	"fmt"
	"github.com/libp2p/go-libp2p-core/crypto/pb"
	"github.com/tjfoc/gmsm/x509"
)

var _ PrivKey = (*KMSPrivateKey)(nil)

// KMSPrivateKey is an implementation of a kms private key
type KMSPrivateKey struct {
	Priv bccrypto.PrivateKey
}

var _ PubKey = (*KMSPublicKey)(nil)

// KMSPublicKey is an implementation of an SM2 public key
type KMSPublicKey struct {
	Pub bccrypto.PublicKey
}

// UnmarshalKMSPublicKey returns the public key from x509 bytes
func UnmarshalKMSPublicKey(data []byte) (PubKey, error) {
	pub, err := x509.ParseSm2PublicKey(data)
	if err != nil {
		return nil, err
	}
	return &KMSPublicKey{Pub: &asymSM2.PublicKey{K: pub}}, nil
}

// Bytes returns the private key as protobuf bytes
func (tPriv *KMSPrivateKey) Bytes() ([]byte, error) {
	return MarshalPrivateKey(tPriv)
}

// Type returns the key type
func (tPriv *KMSPrivateKey) Type() pb.KeyType {
	return pb.KeyType_KMS
}

// Raw returns x509 bytes from a private key
func (tPriv *KMSPrivateKey) Raw() ([]byte, error) {
	return tPriv.Priv.Bytes()
}

// Equals compares two private keys
func (tPriv *KMSPrivateKey) Equals(o Key) bool {
	return basicEquals(tPriv, o)
}

// Sign returns the signature of the input data
func (tPriv *KMSPrivateKey) Sign(data []byte) ([]byte, error) {
	return tPriv.Priv.SignWithOpts(data, &bccrypto.SignOpts{
		Hash: bccrypto.HASH_TYPE_SM3,
		UID:  bccrypto.CRYPTO_DEFAULT_UID,
	})
}

// GetPublic returns a public key
func (tPriv *KMSPrivateKey) GetPublic() PubKey {
	pub := tPriv.Priv.PublicKey()
	pk, ok := pub.(*asymSM2.PublicKey)
	if !ok {
		return nil
	}

	return &SM2PublicKey{pk.K}
}

// Bytes returns the public key as protobuf bytes
func (ePub *KMSPublicKey) Bytes() ([]byte, error) {
	return MarshalPublicKey(ePub)
}

// Type returns the key type
func (ePub *KMSPublicKey) Type() pb.KeyType {
	return pb.KeyType_KMS
}

// Raw returns x509 bytes from a public key
func (ePub *KMSPublicKey) Raw() ([]byte, error) {
	pk, ok := ePub.Pub.(*asymSM2.PublicKey)
	if !ok {
		return nil, fmt.Errorf("invaild pubkey type:%T", ePub.Pub)
	}

	return x509.MarshalSm2PublicKey(pk.K)
}

// Equals compares to public keys
func (ePub *KMSPublicKey) Equals(o Key) bool {
	return basicEquals(ePub, o)
}

// Verify compares data to a signature
func (ePub *KMSPublicKey) Verify(data, sigBytes []byte) (bool, error) {
	//return ePub.pub.Verify(data, sigBytes)
	return ePub.Pub.VerifyWithOpts(data, sigBytes, &bccrypto.SignOpts{
		Hash: bccrypto.HASH_TYPE_SM3,
		UID:  bccrypto.CRYPTO_DEFAULT_UID,
	})
}
