/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package chainconfigmgr

import (
	"fmt"

	"techtradechain.com/techtradechain/common/v2/sortedmap"
	acPb "techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
	configPb "techtradechain.com/techtradechain/pb-go/v2/config"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/vm-native/v2/common"
	"github.com/gogo/protobuf/proto"
)

// ChainPermissionRuntime 关于Permission的配置操作
type ChainPermissionRuntime struct {
	log protocol.Logger
}

// ResourcePolicyAdd add permission
func (r *ChainPermissionRuntime) ResourcePolicyAdd(txSimContext protocol.TxSimContext, params map[string][]byte) (
	result []byte, err error) {
	// [start]
	chainConfig, err := common.GetChainConfig(txSimContext)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}

	// verify params
	changed := false
	resourcePolicies := chainConfig.ResourcePolicies
	if resourcePolicies == nil {
		resourcePolicies = make([]*configPb.ResourcePolicy, 0)
	}

	resourceMap := make(map[string]interface{})
	for _, p := range resourcePolicies {
		resourceMap[p.ResourceName] = struct{}{}
	}

	sortedParams := sortedmap.NewStringKeySortedMapWithBytesData(params)
	var parseParamErr error
	sortedParams.Range(func(key string, val interface{}) (isContinue bool) {
		value, ok := val.([]byte)
		if !ok {
			r.log.Error("value not a string")
		}
		_, ok = resourceMap[key]
		if ok {
			parseParamErr = fmt.Errorf("permission resource_name[%s] is exist", key)
			r.log.Errorf(parseParamErr.Error())
			return false
		}

		policy := &acPb.Policy{}
		err1 := proto.Unmarshal([]byte(value), policy)
		if err1 != nil {
			parseParamErr = fmt.Errorf("policy Unmarshal err:%s", err1)
			r.log.Errorf(parseParamErr.Error())
			return false
		}

		resourcePolicy := &configPb.ResourcePolicy{
			ResourceName: key,
			Policy:       policy,
		}

		ac, err1 := txSimContext.GetAccessControl()
		if err1 != nil {
			parseParamErr = fmt.Errorf("add resource policy GetAccessControl err:%s", err1)
			r.log.Errorf(parseParamErr.Error())
			return false
		}

		b := ac.ValidateResourcePolicy(resourcePolicy)
		if !b {
			parseParamErr = fmt.Errorf(
				"add resource policy failed this resourcePolicy is restricted CheckPrincipleValidity"+
					" err resourcePolicy[%s]", resourcePolicy)
			r.log.Errorf(parseParamErr.Error())
			return false
		}
		resourcePolicies = append(resourcePolicies, resourcePolicy)

		chainConfig.ResourcePolicies = resourcePolicies
		changed = true
		return true
	})

	if parseParamErr != nil {
		return nil, parseParamErr
	}
	if !changed {
		r.log.Error(common.ErrParams)
		return nil, common.ErrParams
	}
	// [end]
	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Errorf("resource policy add fail, %s, params %+v", err.Error(), params)
	} else {
		r.log.Infof("resource policy add success. params %+v", params)
	}
	return result, err
}

// ResourcePolicyUpdate update resource policy
func (r *ChainPermissionRuntime) ResourcePolicyUpdate(txSimContext protocol.TxSimContext, params map[string][]byte) (
	result []byte, err error) {
	// [start]
	chainConfig, err := common.GetChainConfig(txSimContext)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}

	// verify params
	changed := false
	resourcePolicies := chainConfig.ResourcePolicies
	if resourcePolicies == nil {
		resourcePolicies = make([]*configPb.ResourcePolicy, 0)
	}

	resourceMap := make(map[string]interface{})
	for _, p := range resourcePolicies {
		resourceMap[p.ResourceName] = struct{}{}
	}

	sortedParams := sortedmap.NewStringKeySortedMapWithBytesData(params)
	var parseParamErr error
	sortedParams.Range(func(key string, val interface{}) (isContinue bool) {
		value, ok := val.([]byte)
		if !ok {
			r.log.Error("value not a string")
		}
		_, ok = resourceMap[key]
		if !ok {
			parseParamErr = fmt.Errorf("permission resource name does not exist resource_name[%s]", key)
			r.log.Errorf(parseParamErr.Error())
			return false
		}
		policy := &acPb.Policy{}
		err1 := proto.Unmarshal(value, policy)
		if err1 != nil {
			parseParamErr = fmt.Errorf("policy Unmarshal err:%s", err1)
			r.log.Errorf(parseParamErr.Error())
			return false
		}
		for i, resourcePolicy := range resourcePolicies {
			if resourcePolicy.ResourceName != key {
				continue
			}
			rp := &configPb.ResourcePolicy{
				ResourceName: key,
				Policy:       policy,
			}
			ac, err1 := txSimContext.GetAccessControl()
			if err1 != nil {
				parseParamErr = fmt.Errorf("GetAccessControl, err:%s", err1)
				r.log.Errorf(parseParamErr.Error())
				return false
			}
			b := ac.ValidateResourcePolicy(rp)
			if !b {
				parseParamErr = fmt.Errorf(
					"update resource policy this resourcePolicy is restricted. CheckPrincipleValidity"+
						" err resourcePolicy %+v", rp)
				r.log.Errorf(parseParamErr.Error())
				return false
			}
			resourcePolicies[i] = rp
			chainConfig.ResourcePolicies = resourcePolicies
			changed = true
		}
		return true
	})

	if parseParamErr != nil {
		return nil, parseParamErr
	}

	if !changed {
		r.log.Error(common.ErrParams)
		return nil, common.ErrParams
	}
	// [end]
	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Errorf("resource policy update fail, %s, params %+v", err.Error(), params)
	} else {
		r.log.Infof("resource policy update success. params %+v", params)
	}
	return result, err
}

// ResourcePolicyDelete delete permission
func (r *ChainPermissionRuntime) ResourcePolicyDelete(txSimContext protocol.TxSimContext, params map[string][]byte) (
	result []byte, err error) {
	// [start]
	chainConfig, err := common.GetChainConfig(txSimContext)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}

	// verify params
	changed := false
	resourcePolicies := chainConfig.ResourcePolicies
	if resourcePolicies == nil {
		resourcePolicies = make([]*configPb.ResourcePolicy, 0)
	}

	resourceMap := make(map[string]interface{})
	for _, p := range resourcePolicies {
		resourceMap[p.ResourceName] = struct{}{}
	}

	// map is out of order, in order to ensure that each execution sequence is consistent, we need to sort
	sortedParams := sortedmap.NewStringKeySortedMapWithBytesData(params)
	var parseParamErr error
	sortedParams.Range(func(key string, val interface{}) (isContinue bool) {
		_, ok := resourceMap[key]
		if !ok {
			parseParamErr = fmt.Errorf("permission resource name does not exist resource_name[%s]", key)
			r.log.Error(parseParamErr.Error())
			return false
		}
		resourcePolicy := &configPb.ResourcePolicy{
			ResourceName: key,
			Policy: &acPb.Policy{
				Rule:     string(protocol.RuleDelete),
				OrgList:  nil,
				RoleList: nil,
			},
		}
		ac, err1 := txSimContext.GetAccessControl()
		if err1 != nil {
			parseParamErr = fmt.Errorf("delete resource policy GetAccessControl err:%s", err1)
			r.log.Error(parseParamErr.Error())
			return false
		}
		b := ac.ValidateResourcePolicy(resourcePolicy)
		if !b {
			parseParamErr = fmt.Errorf("delete resource policy this resourcePolicy is restricted,"+
				" CheckPrincipleValidity err resourcePolicy %+v", resourcePolicy)
			r.log.Error(parseParamErr.Error())
			return false
		}

		for i, rp := range resourcePolicies {
			if rp.ResourceName == key {
				resourcePolicies = append(resourcePolicies[:i], resourcePolicies[i+1:]...)
				chainConfig.ResourcePolicies = resourcePolicies
				changed = true
				break
			}
		}
		return true
	})
	if parseParamErr != nil {
		return nil, parseParamErr
	}

	if !changed {
		r.log.Error(common.ErrParams)
		return nil, common.ErrParams
	}
	// [end]
	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Errorf("resource policy delete fail, %s, params %+v", err.Error(), params)
	} else {
		r.log.Infof("resource policy delete success. params %+v", params)
	}
	return result, err
}

// ResourcePolicyList get newest resource policy list
func (r *ChainPermissionRuntime) ResourcePolicyList(txSimContext protocol.TxSimContext, params map[string][]byte) (
	result []byte, err error) {
	chainConfig, err := common.GetChainConfig(txSimContext)
	if err != nil {
		return nil, err
	}
	var policyMap map[string]*acPb.Policy
	var cfg = &configPb.ChainConfig{}
	//get default resource policies from ac, and merge it with the chain config resource policies
	if ac, err1 := txSimContext.GetAccessControl(); err1 == nil {
		if policyMap, err1 = ac.GetAllPolicy(); err1 == nil {
			for _, policy := range chainConfig.ResourcePolicies {
				policyMap[policy.ResourceName] = policy.Policy
			}
		}
	}
	//convert map to slice
	if len(policyMap) == 0 {
		cfg.ResourcePolicies = chainConfig.ResourcePolicies
	} else {
		for k, v := range policyMap {
			cfg.ResourcePolicies = append(cfg.ResourcePolicies, &configPb.ResourcePolicy{
				ResourceName: k,
				Policy:       v,
			})
		}
	}
	//we use chainconfig, not ResourcePolicy slice which is not a proto Message
	bytes, err := proto.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("proto marshal chain config failed, err: %s", err.Error())
	}
	return bytes, nil
}
