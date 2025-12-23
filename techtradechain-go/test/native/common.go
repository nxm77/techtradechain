package native_test1

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"

	"techtradechain.com/techtradechain-go/module/accesscontrol"
	"techtradechain.com/techtradechain/common/v2/ca"
	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/asym"
	"techtradechain.com/techtradechain/common/v2/helper"
	acPb "techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
	apiPb "techtradechain.com/techtradechain/pb-go/v2/api"
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/protocol/v2"
	"google.golang.org/grpc"
)

const (
	CHAIN1        = "chain1"
	IP            = "localhost"
	Port          = 12301
	logTempSendTx = "send tx resp: code:%d, msg:%s, payload:%+v\n"
)

var (
	isTls           = true
	WasmPath        = ""
	WasmUpgradePath = ""
	contractName    = ""
	runtimeType     commonPb.RuntimeType
	caPaths         = []string{certPathPrefix + "/crypto-config/wx-org1.techtradechain.com/ca"}
	//userKeyPath         = certPathPrefix + "/crypto-config/wx-org1.techtradechain.com/user/client1/client1.tls.key"
	userKeyPath = certPathPrefix + "/crypto-config/wx-org1.techtradechain.com/user/client1/client1.sign.key"
	//userCrtPath         = certPathPrefix + "/crypto-config/wx-org1.techtradechain.com/user/client1/client1.tls.crt"
	userCrtPath         = certPathPrefix + "/crypto-config/wx-org1.techtradechain.com/user/client1/client1.sign.crt"
	prePathFmt          = certPathPrefix + "/crypto-config/wx-org%s.techtradechain.com/user/admin1/"
	OrgIdFormat         = "wx-org%s.techtradechain.com"
	conn                *grpc.ClientConn
	client              apiPb.RpcNodeClient
	sk3                 crypto.PrivateKey
	err                 error
	txId                string
	UserKeyPathFmt      = certPathPrefix + "/crypto-config/wx-org%s.techtradechain.com/user/client1/client1.tls.key"
	UserCrtPathFmt      = certPathPrefix + "/crypto-config/wx-org%s.techtradechain.com/user/client1/client1.tls.crt"
	UserSignKeyPathFmt  = certPathPrefix + "/crypto-config/wx-org%s.techtradechain.com/user/client1/client1.sign.key"
	UserSignCrtPathFmt  = certPathPrefix + "/crypto-config/wx-org%s.techtradechain.com/user/client1/client1.sign.crt"
	AdminSignKeyPathFmt = certPathPrefix + "/crypto-config/wx-org%s.techtradechain.com/user/admin1/admin1.sign.key"
	AdminSignCrtPathFmt = certPathPrefix + "/crypto-config/wx-org%s.techtradechain.com/user/admin1/admin1.sign.crt"

	DefaultUserKeyPath = fmt.Sprintf(UserKeyPathFmt, "1")
	DefaultUserCrtPath = fmt.Sprintf(UserCrtPathFmt, "1")
	DefaultOrgId       = fmt.Sprintf(OrgIdFormat, "1")
)

const certPathPrefix = "../../config"

func init() {
	// init
	conn, err = initGRPCConnect(isTls)
	if err != nil {
		fmt.Println(err)
		return
	}
	//defer conn.Close()

	client = apiPb.NewRpcNodeClient(conn)

	file, err := ioutil.ReadFile(userKeyPath)
	if err != nil {
		panic(err)
	}

	sk3, err = asym.PrivateKeyFromPEM(file, nil)
	if err != nil {
		panic(err)
	}

	WasmPath = "../wasm/rust-func-verify-2.0.0.wasm"
	WasmUpgradePath = WasmPath
	contractName = "contract101"
	runtimeType = commonPb.RuntimeType_WASMER
}

func initGRPCConnect(useTLS bool) (*grpc.ClientConn, error) {
	url := fmt.Sprintf("%s:%d", IP, Port)
	if useTLS {
		tlsClient := ca.CAClient{
			ServerName: "techtradechain.com",
			CaPaths:    caPaths,
			CertFile:   userCrtPath,
			KeyFile:    userKeyPath,
		}

		c, err := tlsClient.GetCredentialsByCA()
		if err != nil {
			log.Fatalf("GetTLSCredentialsByCA err: %v", err)
			return nil, err
		}
		return grpc.Dial(url, grpc.WithTransportCredentials(*c))
	} else {
		return grpc.Dial(url, grpc.WithInsecure())
	}
}
func AclSignOne(bytes []byte, index int) (*commonPb.EndorsementEntry, error) {
	signers := make([]protocol.SigningMember, 0)
	sk, member := GetAdminTlsSK(index)
	signer := getSigner(sk, member)
	signers = append(signers, signer)
	return signWith(bytes, signer, crypto.CRYPTO_ALGO_SHA256)
}

// 获取admin的私钥
func GetAdminSK(index int) (crypto.PrivateKey, *acPb.Member) {
	numStr := strconv.Itoa(index)

	path := fmt.Sprintf(prePathFmt, numStr) + "admin1.sign.key"
	file, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}
	sk3, err := asym.PrivateKeyFromPEM(file, nil)
	if err != nil {
		panic(err)
	}

	userCrtPath := fmt.Sprintf(prePathFmt, numStr) + "admin1.sign.crt"
	file2, err := ioutil.ReadFile(userCrtPath)
	//fmt.Println("node", numStr, "crt", string(file2))
	if err != nil {
		panic(err)
	}

	// 获取peerId
	peerId, err := helper.GetLibp2pPeerIdFromCert(file2)
	fmt.Println("node", numStr, "peerId", peerId)

	// 构造Sender
	sender := &acPb.Member{
		OrgId:      fmt.Sprintf(OrgIdFormat, numStr),
		MemberInfo: file2,
		////IsFullCert: true,
	}

	return sk3, sender
}

// 获取admin的私钥
func GetAdminTlsSK(index int) (crypto.PrivateKey, *acPb.Member) {
	numStr := strconv.Itoa(index)

	path := fmt.Sprintf(prePathFmt, numStr) + "admin1.tls.key"
	file, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}
	sk3, err := asym.PrivateKeyFromPEM(file, nil)
	if err != nil {
		panic(err)
	}

	userCrtPath := fmt.Sprintf(prePathFmt, numStr) + "admin1.tls.crt"
	file2, err := ioutil.ReadFile(userCrtPath)
	//fmt.Println("node", numStr, "crt", string(file2))
	if err != nil {
		panic(err)
	}

	// 获取peerId
	peerId, err := helper.GetLibp2pPeerIdFromCert(file2)
	fmt.Println("node", numStr, "peerId", peerId)

	// 构造Sender
	sender := &acPb.Member{
		OrgId:      fmt.Sprintf(OrgIdFormat, numStr),
		MemberType: 0,
		MemberInfo: file2,
	}

	return sk3, sender
}

func getSigner(sk3 crypto.PrivateKey, sender *acPb.Member) protocol.SigningMember {
	skPEM, err := sk3.String()
	if err != nil {
		log.Fatalf("get sk PEM failed, %s", err.Error())
	}

	signer, err := accesscontrol.NewCertSigningMember("", sender, skPEM, "")
	if err != nil {
		panic(err)
	}
	return signer
}

func signWith(msg []byte, signer protocol.SigningMember, hashType string) (*commonPb.EndorsementEntry, error) {
	sig, err := signer.Sign(hashType, msg)
	if err != nil {
		return nil, err
	}
	signerSerial, err := signer.GetMember()
	if err != nil {
		return nil, err
	}
	return &commonPb.EndorsementEntry{
		Signer:    signerSerial,
		Signature: sig,
	}, nil
}

// 获取用户私钥
func GetUserSK(index int) (crypto.PrivateKey, *acPb.Member) {
	numStr := strconv.Itoa(index)

	keyPath := fmt.Sprintf(UserSignKeyPathFmt, numStr)
	file, err := ioutil.ReadFile(keyPath)
	if err != nil {
		panic(err)
	}
	sk3, err := asym.PrivateKeyFromPEM(file, nil)
	if err != nil {
		panic(err)
	}
	certPath := fmt.Sprintf(UserSignCrtPathFmt, numStr)
	file2, err := ioutil.ReadFile(certPath)
	if err != nil {
		panic(err)
	}

	sender := &acPb.Member{
		OrgId:      fmt.Sprintf(OrgIdFormat, numStr),
		MemberInfo: file2,
		////IsFullCert: true,
	}

	return sk3, sender
}

// 获取用户私钥
func GetUserTlsSK(index int) (crypto.PrivateKey, *acPb.Member) {
	numStr := strconv.Itoa(index)

	keyPath := fmt.Sprintf(UserKeyPathFmt, numStr)
	file, err := ioutil.ReadFile(keyPath)
	if err != nil {
		panic(err)
	}
	sk3, err := asym.PrivateKeyFromPEM(file, nil)
	if err != nil {
		panic(err)
	}
	certPath := fmt.Sprintf(UserCrtPathFmt, numStr)
	file2, err := ioutil.ReadFile(certPath)
	if err != nil {
		panic(err)
	}

	sender := &acPb.Member{
		OrgId:      fmt.Sprintf(OrgIdFormat, numStr),
		MemberInfo: file2,
		////IsFullCert: true,
	}

	return sk3, sender
}

func InitGRPCConnect(useTLS bool) (*grpc.ClientConn, error) {
	url := fmt.Sprintf("%s:%d", IP, Port)

	if useTLS {
		tlsClient := ca.CAClient{
			ServerName: "techtradechain.com",
			CaPaths:    caPaths,
			CertFile:   DefaultUserCrtPath,
			KeyFile:    DefaultUserKeyPath,
		}

		c, err := tlsClient.GetCredentialsByCA()
		if err != nil {
			log.Fatalf("GetTLSCredentialsByCA err: %v", err)
			return nil, err
		}
		return grpc.Dial(url, grpc.WithTransportCredentials(*c))
	} else {
		return grpc.Dial(url, grpc.WithInsecure())
	}
}
