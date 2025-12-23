/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package rotatelogs

func (h HandlerFunc) Handle(e Event) {
	h(e)
}

func (e *FileRotatedEvent) Type() EventType {
	return FileRotatedEventType
}

func (e *FileRotatedEvent) PreviousFile() string {
	return e.prev
}

func (e *FileRotatedEvent) CurrentFile() string {
	return e.current
}
