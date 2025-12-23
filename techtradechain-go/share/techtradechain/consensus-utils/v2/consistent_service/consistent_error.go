/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consistent_service

import "errors"

var (
	// ErrorInvalidParameter invalid parameter
	ErrorInvalidParameter = errors.New("invalid parameter")
	// ErrorBroadcasterExist broadcaster exist
	ErrorBroadcasterExist = errors.New("broadcaster exist")
	// ErrorDecoderExist decoder exist
	ErrorDecoderExist = errors.New("decoder exist")
	// ErrorInterceptorExist interceptor exist
	ErrorInterceptorExist = errors.New("interceptor exist")
	// ErrorRunRepeatedly run repeatedly
	ErrorRunRepeatedly = errors.New("run repeatedly")
	// ErrorNotRunning not running
	ErrorNotRunning = errors.New("not running")
	// ErrorRemoterExist remoter exist
	ErrorRemoterExist = errors.New("remoter exist")
	// ErrorRemoterNotExist remoter not exist
	ErrorRemoterNotExist = errors.New("remoter not exist")
	// ErrorRemoterEqualLocal remoter equal local
	ErrorRemoterEqualLocal = errors.New("remoter equal local")
)
