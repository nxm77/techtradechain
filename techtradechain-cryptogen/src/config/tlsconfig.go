package config

import "techtradechain.com/techtradechain/common/v2/crypto"

const (
	TLS_Single_Cert_Mode = 0
	TLS_Double_Cert_Mode = 1
)

func IsTlsDoubleCertMode(tlsMode int, keyType crypto.KeyType) bool {
	//tls double cert mode only used for GMTLS
	if tlsMode == TLS_Double_Cert_Mode && keyType == crypto.SM2 {
		return true
	}
	return false
}
