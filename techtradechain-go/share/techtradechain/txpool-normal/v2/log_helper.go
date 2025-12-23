/*
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package normal

import (
	"strings"
	"time"

	"techtradechain.com/techtradechain/protocol/v2"
)

const (
	printCap = 1024
)

// LogHelper Auxiliary printing
type LogHelper struct {
	log       protocol.Logger
	ticker    *time.Ticker
	printC    chan string
	finishC   chan struct{}
	logFormat string
	max       int
}

// newLogHelper create logHelper
func newLogHelper(duration time.Duration, max int, log protocol.Logger, logFormat string) *LogHelper {
	return &LogHelper{
		log:       log,
		ticker:    time.NewTicker(duration),
		max:       max,
		printC:    make(chan string, printCap),
		finishC:   make(chan struct{}),
		logFormat: logFormat,
	}
}

func (l *LogHelper) start() {
	go func() {
		var builder strings.Builder
		var writeCounter = 0
		for {
			select {
			case c, ok := <-l.printC:
				if !ok {
					l.ticker.Stop()
					l.finishC <- struct{}{}
					return
				}
				writeCounter++
				if builder.Len() > 0 {
					builder.WriteString(",")
				}
				builder.WriteString(c)
				if writeCounter%l.max == 0 {
					l.log.Warnf(l.logFormat, builder.String())
					builder.Reset()
					writeCounter = 0
				}
			case <-l.ticker.C:
				if builder.Len() > 0 {
					l.log.Warnf(l.logFormat, builder.String())
					builder.Reset()
					writeCounter = 0
				}
			}
		}
	}()
}

func (l *LogHelper) append(x string) {
	l.printC <- x
}

func (l *LogHelper) stop() {
	close(l.printC)
	<-l.finishC
}
