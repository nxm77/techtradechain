/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package option

type Interface interface {
	Name() string
	Value() interface{}
}

type Option struct {
	name  string
	value interface{}
}

func New(name string, value interface{}) *Option {
	return &Option{
		name:  name,
		value: value,
	}
}

func (o *Option) Name() string {
	return o.name
}
func (o *Option) Value() interface{} {
	return o.value
}
