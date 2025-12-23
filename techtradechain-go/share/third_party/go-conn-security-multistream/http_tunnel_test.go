package csms

import (
	"net/http"
	"os"
	"testing"
)

func TestGetEnv(t *testing.T) {
	pid1 := "QmeyNRs2DwWjcHTpcVHoUSaDAAif4VQZ2wQDQNDP33gH"
	pid2 := "QmXf6mnQDBR9aHauRmViKzSuZgpumkn7x6rNxw1oqqRr45"
	pid3 := "QmRRWXJpAVdhFsFtd9ah5F4LDQWFFBDVKpECAF8hssqj6H"
	pid4 := "QmPpx5dTZ4A1GQ9a4nsSoMJ72AtT3VDgcX2EVKAFxJUHb1"
	_ = os.Setenv(HttpTunnelTargetAddressList, "gH/http://example.org1,45/127.0.0.1:8000,6H/example.org1:8000")
	_ = os.Setenv(HttpTunnelWhiteList, "gH,45,b1")
	_ = os.Setenv(HttpTunnelDefaultTargetAddress, "example.org")
	//test1
	res := getSendHttpTunnelTargetAddress(pid1)
	if res != "http://example.org1" {
		t.Error(res)
	}
	_, err := http.NewRequest("CONNECT", res, nil)
	if err != nil {
		t.Error(err)
	}
	//test2
	res = getSendHttpTunnelTargetAddress(pid2)
	if res != "//127.0.0.1:8000" {
		t.Error(res)
	}
	_, err = http.NewRequest("CONNECT", res, nil)
	if err != nil {
		t.Error(err)
	}
	//test3
	res = getSendHttpTunnelTargetAddress(pid3)
	if res != "example.org1:8000" {
		t.Error(res)
	}
	_, err = http.NewRequest("CONNECT", res, nil)
	if err != nil {
		t.Error(err)
	}
	//test4
	res = getSendHttpTunnelTargetAddress(pid4)
	if res != "example.org" {
		t.Error(res)
	}
	_, err = http.NewRequest("CONNECT", res, nil)
	if err != nil {
		t.Error(err)
	}
}

func TestGetEnv2(t *testing.T) {
	pid1 := "QmeyNRs2DwWjcHTpcVHoUSaDAAif4VQZ2wQDQNDP33gH"
	//test1
	_ = os.Setenv(HttpTunnelTargetAddressList, "")
	_ = os.Setenv(HttpTunnelWhiteList, "gH,45,6H")
	_ = os.Setenv(HttpTunnelDefaultTargetAddress, "example.org")
	res := getSendHttpTunnelTargetAddress(pid1)
	if len(res) == 0 {
		t.Error(pid1)
	}
	//test2
	_ = os.Setenv(HttpTunnelTargetAddressList, "gH/127.0.0.1:8000,45/127.0.0.1:8001,33/example.org1")
	_ = os.Setenv(HttpTunnelWhiteList, "")
	_ = os.Setenv(HttpTunnelDefaultTargetAddress, "example.org")
	_ = getSendHttpTunnelTargetAddress(pid1)
	//test3
	_ = os.Setenv(HttpTunnelTargetAddressList, "")
	_ = os.Setenv(HttpTunnelWhiteList, "gH,45,6H")
	_ = os.Setenv(HttpTunnelDefaultTargetAddress, "")
	res = getSendHttpTunnelTargetAddress(pid1)
	if len(res) != 0 {
		t.Error(pid1)
	}
}
