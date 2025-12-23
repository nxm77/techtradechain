package libp2pnet

import (
	"encoding/json"
	"time"
)

const TOPIC_CERTID = "certID"

type certIdList struct {
	CertId     string //兼容底链234到236，net-libp2p的125到127
	PeerId     string //兼容底链234到236，net-libp2p的125到127
	CertIdList []*certId
}

type certId struct {
	CertId string
	PeerId string
}

// checkCertId
// @Description: 本地certId周期广播给其他节点,收到广播消息后存到本地
// @receiver ln
// @param chainId
// @return ${return_type}
func (ln *LibP2pNet) checkCertId(chainId string) {
	err := ln.SubscribeWithChainId(chainId, TOPIC_CERTID, func(publisherPeerId string, msgData []byte) error {
		msg := &certIdList{}
		err := json.Unmarshal(msgData, msg)
		if err != nil {
			ln.log.Error("json err:", err)
			return nil
		}
		for _, v := range msg.CertIdList {
			_, err = ln.libP2pHost.certPeerIdMapper.FindPeerIdByCertId(v.CertId)
			if err != nil {
				ln.log.Info("certPeerIdMapper.Add:", v.PeerId)
				ln.libP2pHost.certPeerIdMapper.Add(v.CertId, v.PeerId)
			}
		}
		//兼容底链234到236，net-libp2p的125到127
		if len(msg.PeerId) > 0 && len(msg.CertId) > 0 {
			_, err = ln.libP2pHost.certPeerIdMapper.FindPeerIdByCertId(msg.CertId)
			if err != nil {
				ln.log.Info("certPeerIdMapper.Add old:", msg.PeerId)
				ln.libP2pHost.certPeerIdMapper.Add(msg.CertId, msg.PeerId)
			}
		}
		return nil
	})
	if err != nil {
		ln.log.Error("SubscribeWithChainId err:", err)
		return
	}
	ticker := time.NewTicker(refreshCertIdTickerTime)

	for {
		select {
		case <-ln.ctx.Done():
			ln.log.Info("checkCertId return")
			return
		case <-ticker.C:
		}
		msgList := &certIdList{}
		tmpCertIdList := ln.libP2pHost.certPeerIdMapper.GetAll()
		if len(tmpCertIdList) == 0 {
			continue
		}
		if len(tmpCertIdList)%2 != 0 {
			ln.log.Error("certPeerIdMapper return err,", tmpCertIdList)
			continue
		}
		for i := 0; i < len(tmpCertIdList); i += 2 {
			msg := &certId{
				CertId: tmpCertIdList[i],
				PeerId: tmpCertIdList[i+1],
			}
			msgList.CertIdList = append(msgList.CertIdList, msg)
		}

		raw, err := json.Marshal(msgList)
		if err != nil {
			ln.log.Error("checkCertId json err:", err)
			continue
		}
		err = ln.BroadcastWithChainId(chainId, TOPIC_CERTID, raw)
		if err != nil {
			if err.Error() == ErrorPubSubNotExist.Error() {
				//正常退出
				ln.log.Info("checkCertId return")
				return
			}
			ln.log.Warn("BroadcastWithChainId err:", err)
		}

	}

}
