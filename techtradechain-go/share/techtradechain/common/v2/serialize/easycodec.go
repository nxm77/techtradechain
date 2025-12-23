/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

/*
techtradechain contract serialization of data interaction
serialization format :
    magicNum + ecVersion + reserved + itemCount + (keyType + keyLen + key + valType + valLen + val)*

 	magicNum: 	byte[4], is the identity easycodec serialized, is []byte("cmec") []byte{99, 109, 101, 99},
		"cmec" mean techtradechain easycodec,
 	ecVersion: 	byte[4]	easycodec version, []byte("v1.0") []byte{118, 49, 46, 48}
	reserved:  	byte[4]	reserved field, 8 byte, []byte{255, 255, 255, 255,255, 255, 255, 255}
	itemCount:  byte[4] number of kvPair, le int32
	keyType:  	byte[4], le int32
	keyLen:  	byte[4], le int32
	keyType:  	byte[keyLen]
	valType:  	byte[4], le int32
	valLen:  	byte[4], le int32
	val:  		byte[valLen]
*/

package serialize

import (
	"bytes"
	"encoding/base64"
	"errors"
	"sort"
	"strconv"
	"strings"
)

var ecMagicNum = []byte{99, 109, 101, 99} // "cmec"
var ecVersion = []byte{118, 49, 46, 48}   // "v1.0"
var ecReserved = []byte{255, 255, 255, 255, 255, 255, 255, 255}

// var ecHeader = []byte{99, 109, 101, 99, 118, 49, 46, 48, 255, 255, 255, 255, 255, 255, 255, 255}

type EasyKeyType int32
type EasyValueType int32

const (
	EasyKeyType_SYSTEM EasyKeyType = 0
	EasyKeyType_USER   EasyKeyType = 1

	EasyValueType_INT32  EasyValueType = 0
	EasyValueType_STRING EasyValueType = 1
	EasyValueType_BYTES  EasyValueType = 2

	MAX_KEY_COUNT    = 128
	MAX_KEY_LEN      = 64
	MAX_VALUE_LEN    = 1024 * 1024
	MIN_LEN          = 20
	EC_MAGIC_NUM_LEN = 4
	EC_VERSION_LEN   = 4
	EC_RESERVED_LEN  = 8
)

type EasyCodec struct {
	items []*EasyCodecItem
}

func NewEasyCodec() *EasyCodec {
	items := make([]*EasyCodecItem, 0)
	return &EasyCodec{items}
}

func NewEasyCodecWithMap(value map[string][]byte) *EasyCodec {
	items := ParamsMapToEasyCodecItem(value)
	return &EasyCodec{items}
}

func NewEasyCodecWithBytes(value []byte) *EasyCodec {
	return &EasyCodec{EasyUnmarshal(value)}
}

func NewEasyCodecWithItems(items []*EasyCodecItem) *EasyCodec {
	return &EasyCodec{items: items}
}

func (e *EasyCodec) AddInt32(key string, value int32) {
	e.items = append(e.items, newEasyCodecItemWithInt32(key, value))
}

func (e *EasyCodec) AddString(key string, value string) {
	e.items = append(e.items, newEasyCodecItemWithString(key, value))
}

func (e *EasyCodec) AddBytes(key string, value []byte) {
	e.items = append(e.items, newEasyCodecItemWithBytes(key, value))
}

func (e *EasyCodec) AddMap(value map[string][]byte) {
	items := ParamsMapToEasyCodecItem(value)
	e.items = append(e.items, items...)
}
func (e *EasyCodec) AddValue(keyType EasyKeyType, key string, valueType EasyValueType, value interface{}) {
	e.items = append(e.items, newEasyCodecItem(keyType, key, valueType, value))
}

func (e *EasyCodec) AddItem(item *EasyCodecItem) {
	e.items = append(e.items, item)
}

func (e *EasyCodec) RemoveKey(key string) {
	for i, item := range e.items {
		if item.Key == key {
			e.items = append(e.items[:i], e.items[i+1:]...)
			return
		}
	}
}

// ToJson simple json, no nesting, rule: int32->strconv.itoa(val) []byte->string([]byte)
func (e *EasyCodec) ToJson() string {
	return EasyCodecItemToJsonStr(e.items)
}

func (e *EasyCodec) ToMap() map[string][]byte {
	return EasyCodecItemToParamsMap(e.items)
}

func (e *EasyCodec) GetItems() []*EasyCodecItem {
	return e.items
}

func (e *EasyCodec) GetItem(key string, keyType EasyKeyType) (*EasyCodecItem, error) {
	for _, item := range e.items {
		if item.Key == key && item.KeyType == keyType {
			return item, nil
		}
	}
	return nil, errors.New("not found key with keyType")
}

func (e *EasyCodec) GetValue(key string, keyType EasyKeyType) (interface{}, error) {
	for _, item := range e.items {
		if item.Key == key && item.KeyType == keyType {
			return item.Value, nil
		}
	}
	return nil, errors.New("not found key with keyType")
}

func (e *EasyCodec) GetInt32(key string) (int32, error) {
	item, err := e.GetItem(key, EasyKeyType_USER)
	if err == nil && item.ValueType == EasyValueType_INT32 {
		return item.Value.(int32), nil
	}
	return 0, errors.New("not found key or value type not int32")
}

func (e *EasyCodec) GetString(key string) (string, error) {
	item, err := e.GetItem(key, EasyKeyType_USER)
	if err == nil && item.ValueType == EasyValueType_STRING {
		return item.Value.(string), nil
	}
	return "", errors.New("not found key or value type not string")
}

func (e *EasyCodec) GetBytes(key string) ([]byte, error) {
	item, err := e.GetItem(key, EasyKeyType_USER)
	if err == nil && item.ValueType == EasyValueType_BYTES {
		return item.Value.([]byte), nil
	}
	return nil, errors.New("not found key or value type not bytes")
}

func (e *EasyCodec) Marshal() []byte {
	return EasyMarshal(e.items)
}

type EasyCodecItem struct {
	KeyType EasyKeyType
	Key     string

	ValueType EasyValueType
	Value     interface{}
}

func newEasyCodecItem(keyType EasyKeyType, key string, valueType EasyValueType, value interface{}) *EasyCodecItem {
	return &EasyCodecItem{
		KeyType:   keyType,
		Key:       key,
		ValueType: valueType,
		Value:     value,
	}
}

func newEasyCodecItemWithInt32(key string, value int32) *EasyCodecItem {
	return newEasyCodecItem(EasyKeyType_USER, key, EasyValueType_INT32, value)
}

func newEasyCodecItemWithString(key string, value string) *EasyCodecItem {
	return newEasyCodecItem(EasyKeyType_USER, key, EasyValueType_STRING, value)
}

func newEasyCodecItemWithBytes(key string, value []byte) *EasyCodecItem {
	return newEasyCodecItem(EasyKeyType_USER, key, EasyValueType_BYTES, value)
}

// ParamsMapToEasyCodecItem Params map converter
func ParamsMapToEasyCodecItem(params map[string][]byte) []*EasyCodecItem {
	items := make([]*EasyCodecItem, 0)
	if len(params) == 0 {
		return items
	}
	keys := make([]string, 0)
	for key := range params {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		items = append(items, newEasyCodecItemWithBytes(key, params[key]))
	}
	return items
}

// EasyCodecItemToParamsMap easyCodecItem converter
func EasyCodecItemToParamsMap(items []*EasyCodecItem) map[string][]byte {
	params := make(map[string][]byte)
	for _, item := range items {
		switch item.ValueType {
		case EasyValueType_BYTES:
			params[item.Key], _ = item.Value.([]byte)
		case EasyValueType_INT32:
			params[item.Key] = []byte(strconv.Itoa(int(item.Value.(int32))))
		case EasyValueType_STRING:
			params[item.Key] = []byte(item.Value.(string))
		}
	}
	return params
}

// EasyCodecItemToJsonStr simple json, no nesting, rule: int32->strconv.itoa(val) []byte->string([]byte)
func EasyCodecItemToJsonStr(items []*EasyCodecItem) string {
	if items == nil {
		return "{}"
	}
	var build strings.Builder
	build.WriteString("{")
	total := len(items)
	for i, item := range items {
		key := item.Key
		build.WriteString("\"")
		build.WriteString(key)
		build.WriteString("\":")
		var val string
		switch item.ValueType {
		case EasyValueType_INT32:
			val = strconv.Itoa(int(item.Value.(int32)))
			build.WriteString(val)
		case EasyValueType_STRING:
			val, _ = item.Value.(string)
			val = strings.ReplaceAll(val, "\"", "\\\"")
			build.WriteString("\"")
			build.WriteString(val)
			build.WriteString("\"")
		case EasyValueType_BYTES:
			val = base64.StdEncoding.EncodeToString(item.Value.([]byte))
			build.WriteString("\"")
			build.WriteString(val)
			build.WriteString("\"")
		}
		if i != total-1 {
			build.WriteString(",")
		}
	}
	build.WriteString("}")
	return build.String()
}

// GetValue get value from item
func (e *EasyCodecItem) GetValue(key string, keyType EasyKeyType) (interface{}, bool) {
	if e.KeyType == keyType && e.Key == key {
		return e.Value, true
	}
	return "", false
}

// EasyMarshal serialize item into binary
func EasyMarshal(items []*EasyCodecItem) []byte {
	buf := new(bytes.Buffer)
	uint32DataBytes := make([]byte, 4)

	//buf.Write(ecMagicNum)
	//buf.Write(ecVersion)
	//buf.Write(ecReserved)

	binaryUint32Marshal(buf, uint32(len(items)), uint32DataBytes)

	for _, item := range items {

		if item.KeyType != EasyKeyType_SYSTEM && item.KeyType != EasyKeyType_USER {
			continue
		}

		binaryUint32Marshal(buf, uint32(item.KeyType), uint32DataBytes)
		binaryUint32Marshal(buf, uint32(len(item.Key)), uint32DataBytes)
		buf.Write([]byte(item.Key))

		switch item.ValueType {

		case EasyValueType_INT32:

			binaryUint32Marshal(buf, uint32(item.ValueType), uint32DataBytes)
			binaryUint32Marshal(buf, uint32(4), uint32DataBytes)
			binaryUint32Marshal(buf, uint32(item.Value.(int32)), uint32DataBytes)

		case EasyValueType_STRING:

			binaryUint32Marshal(buf, uint32(item.ValueType), uint32DataBytes)
			binaryUint32Marshal(buf, uint32(len(item.Value.(string))), uint32DataBytes)
			buf.Write([]byte(item.Value.(string)))

		case EasyValueType_BYTES:

			binaryUint32Marshal(buf, uint32(item.ValueType), uint32DataBytes)
			binaryUint32Marshal(buf, uint32(len(item.Value.([]byte))), uint32DataBytes)
			buf.Write(item.Value.([]byte))

		}
	}

	return buf.Bytes()
}

// EasyUnmarshal Deserialized from binary to item
func EasyUnmarshal(data []byte) []*EasyCodecItem {
	var (
		items         []*EasyCodecItem
		easyKeyType   EasyKeyType
		keyLength     int32
		keyContent    []byte
		easyValueType EasyValueType
		valueLength   int32
	)

	if len(data) <= MIN_LEN {
		return items
	}
	buf := bytes.NewBuffer(data)
	uint32DataBytes := make([]byte, 4)

	var count uint32
	magicNum := make([]byte, EC_MAGIC_NUM_LEN)
	_, _ = buf.Read(magicNum)
	if bytes.Equal(magicNum, ecMagicNum) {
		version := make([]byte, EC_VERSION_LEN)
		reserved := make([]byte, EC_RESERVED_LEN)
		_, _ = buf.Read(version)
		_, _ = buf.Read(reserved)
		if !(bytes.Equal(magicNum, ecMagicNum) && bytes.Equal(version, ecVersion) && bytes.Equal(reserved, ecReserved)) {
			return items
		}
		count = binaryUint32Unmarshal(buf, uint32DataBytes)
	} else {
		count = binaryUint32Unmarshal(bytes.NewBuffer(magicNum), uint32DataBytes)
	}

	if count > MAX_KEY_COUNT {
		return nil
	}

	for i := 0; i < int(count); i++ {
		// Key Part
		easyKeyType = EasyKeyType(binaryUint32Unmarshal(buf, uint32DataBytes))

		keyLength = int32(binaryUint32Unmarshal(buf, uint32DataBytes))
		if keyLength > MAX_KEY_LEN {
			return items
		}
		keyContent = make([]byte, keyLength)
		_, _ = buf.Read(keyContent)

		// Value Part
		easyValueType = EasyValueType((binaryUint32Unmarshal(buf, uint32DataBytes)))

		valueLength = int32(binaryUint32Unmarshal(buf, uint32DataBytes))
		// move 'length verify' from sdk to vm
		//if valueLength > MAX_VALUE_LEN {
		//	return items
		//}

		var easyCodecItem EasyCodecItem

		switch easyValueType {
		case EasyValueType_INT32:
			valueContent := int32(binaryUint32Unmarshal(buf, uint32DataBytes))
			easyCodecItem.Value = valueContent
		case EasyValueType_STRING:
			valueContent := make([]byte, valueLength)
			_, _ = buf.Read(valueContent)
			easyCodecItem.Value = string(valueContent)
		case EasyValueType_BYTES:
			valueContent := make([]byte, valueLength)
			_, _ = buf.Read(valueContent)
			easyCodecItem.Value = valueContent
		}

		easyCodecItem.KeyType = easyKeyType
		easyCodecItem.Key = string(keyContent)
		easyCodecItem.ValueType = easyValueType
		items = append(items, &easyCodecItem)
	}
	return items
}

func binaryUint32Marshal(buf *bytes.Buffer, data uint32, dataBytes []byte) {
	_ = dataBytes[3]
	dataBytes[0] = byte(data)
	dataBytes[1] = byte(data >> 8)
	dataBytes[2] = byte(data >> 16)
	dataBytes[3] = byte(data >> 24)
	buf.Write(dataBytes)
}

func binaryUint32Unmarshal(buf *bytes.Buffer, bs []byte) uint32 {
	_, _ = buf.Read(bs)
	_ = bs[3]
	return uint32(bs[0]) | uint32(bs[1])<<8 | uint32(bs[2])<<16 | uint32(bs[3])<<24
}
