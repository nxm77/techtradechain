/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package bloom

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"techtradechain.com/techtradechain/protocol/v2"
)

type logLevel int

const (
	logLevelDebug logLevel = iota
	logLevelInfo
	logLevelWarn
	logLevelError
	logLevelFatal
	logLevelPanic
)

var (
	_              protocol.Logger = (*logger)(nil)
	logLevelLabels                 = []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL", "PANIC"}
)

func newLogger() *logger {
	return &logger{
		Logger: log.New(os.Stdout, "", log.Lshortfile|log.LstdFlags),
	}
}

type logger struct {
	*log.Logger
}

// Debug implements protocol.Logger.
func (l *logger) Debug(args ...interface{}) {
	l.printL(logLevelDebug, args...)
}

// DebugDynamic implements protocol.Logger.
func (*logger) DebugDynamic(getStr func() string) {
	panic("unimplemented")
}

// Debugw implements protocol.Logger.
func (l *logger) Debugw(msg string, keysAndValues ...interface{}) {
	// panic("unimplemented")
	l.printwL(logLevelDebug, msg, keysAndValues...)
}

// Error implements protocol.Logger.
func (l *logger) Error(args ...interface{}) {
	l.printL(logLevelError, args...)
}

// Errorf implements protocol.Logger.
func (l *logger) Errorf(format string, args ...interface{}) {
	l.printfL(logLevelError, format, args...)
}

// Errorw implements protocol.Logger.
func (l *logger) Errorw(msg string, keysAndValues ...interface{}) {
	l.printwL(logLevelError, msg, keysAndValues...)
}

// Fatal implements protocol.Logger.
func (l *logger) Fatal(args ...interface{}) {
	l.fatalL(logLevelFatal, args...)
}

// Fatalf implements protocol.Logger.
func (l *logger) Fatalf(format string, args ...interface{}) {
	l.fatalfL(logLevelFatal, format, args...)
}

// Fatalw implements protocol.Logger.
func (l *logger) Fatalw(msg string, keysAndValues ...interface{}) {
	l.fatalwL(logLevelFatal, msg, keysAndValues...)
}

// Info implements protocol.Logger.
func (l *logger) Info(args ...interface{}) {
	l.printL(logLevelInfo, args...)
}

// InfoDynamic implements protocol.Logger.
func (*logger) InfoDynamic(getStr func() string) {
	panic("unimplemented")
}

// Infof implements protocol.Logger.
func (l *logger) Infof(format string, args ...interface{}) {
	l.printfL(logLevelInfo, format, args...)
}

// Infow implements protocol.Logger.
func (l *logger) Infow(msg string, keysAndValues ...interface{}) {
	l.printwL(logLevelInfo, msg, keysAndValues...)
}

// Panic implements protocol.Logger.
func (l *logger) Panic(args ...interface{}) {
	l.panicL(logLevelPanic, args...)
}

// Panicf implements protocol.Logger.
func (l *logger) Panicf(format string, args ...interface{}) {
	l.panicfL(logLevelPanic, format, args...)
}

// Panicw implements protocol.Logger.
func (l *logger) Panicw(msg string, keysAndValues ...interface{}) {
	l.panicwL(logLevelPanic, msg, keysAndValues...)
}

// Warn implements protocol.Logger.
func (l *logger) Warn(args ...interface{}) {
	l.printL(logLevelWarn, args...)
}

// Warnf implements protocol.Logger.
func (l *logger) Warnf(format string, args ...interface{}) {
	l.printfL(logLevelWarn, format, args...)
}

// Warnw implements protocol.Logger.
func (*logger) Warnw(msg string, keysAndValues ...interface{}) {
	panic("unimplemented")
}

func (l *logger) Debugf(format string, args ...interface{}) {
	l.printfL(logLevelDebug, format, args...)
}

func (l *logger) printL(level logLevel, args ...interface{}) {
	_ = l.Output(3, "["+logLevelLabels[level]+"] "+fmt.Sprint(args...))
}

func (l *logger) printfL(level logLevel, format string, args ...interface{}) {
	_ = l.Output(3, "["+logLevelLabels[level]+"] "+fmt.Sprintf(format, args...))
}
func (l *logger) printwL(level logLevel, msg string, keysAndValues ...interface{}) {
	_ = l.Output(3, "["+logLevelLabels[level]+"] "+msg+" "+getWMessage(keysAndValues...))
}

func (l *logger) fatalL(level logLevel, args ...interface{}) {
	l.printL(level, args...)
	os.Exit(1)
}

func (l *logger) fatalfL(level logLevel, format string, args ...interface{}) {
	l.printfL(level, format, args...)
	os.Exit(1)
}
func (l *logger) fatalwL(level logLevel, msg string, keysAndValues ...interface{}) {
	l.printwL(level, msg, keysAndValues...)
	os.Exit(1)
}

func (l *logger) panicL(level logLevel, args ...interface{}) {
	s := fmt.Sprint(args...)
	_ = l.Output(3, "["+logLevelLabels[level]+"] "+s)
	panic(s)
}
func (l *logger) panicfL(level logLevel, format string, args ...interface{}) {
	s := fmt.Sprintf(format, args...)
	_ = l.Output(3, "["+logLevelLabels[level]+"] "+s)
	panic(s)
}

func (l *logger) panicwL(level logLevel, msg string, keysAndValues ...interface{}) {
	s := msg + " " + getWMessage(keysAndValues...)
	_ = l.Output(3, "["+logLevelLabels[level]+"] "+s)
	panic(s)
}

func getWMessage(keysAndValues ...interface{}) string {
	if len(keysAndValues) == 0 {
		return ""
	}
	if len(keysAndValues)%2 != 0 {
		return "<<logger error>> invalid number of arguments"
	}

	data := make(map[string]interface{})
	for i := 0; i < len(keysAndValues); i += 2 {
		key := fmt.Sprintf("%v", keysAndValues[i])
		value := keysAndValues[i+1]
		data[key] = value
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return "<<" + err.Error() + ">>"
	}
	return string(jsonData)

}
