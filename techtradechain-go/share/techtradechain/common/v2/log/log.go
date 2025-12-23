/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package log

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	rotatelogs "techtradechain.com/techtradechain/common/v2/log/file-rotatelogs"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// LOG_LEVEL 日志级别，int类型，内部接口使用常量
type LOG_LEVEL int

// 日志级别
const (
	LEVEL_DEBUG LOG_LEVEL = iota
	LEVEL_INFO
	LEVEL_WARN
	LEVEL_ERROR
)

// 日志级别，配置文件定义的常量
const (
	DEBUG = "DEBUG"
	INFO  = "INFO"
	WARN  = "WARN"
	ERROR = "ERROR"
)

// GetLogLevel 根据字符串型的日志级别，返回枚举型日志级别
// @param lvl
// @return LOG_LEVEL
func GetLogLevel(lvl string) LOG_LEVEL {
	switch strings.ToUpper(lvl) {
	case ERROR:
		return LEVEL_ERROR
	case WARN:
		return LEVEL_WARN
	case INFO:
		return LEVEL_INFO
	case DEBUG:
		return LEVEL_DEBUG
	}

	return LEVEL_INFO
}

func getZapLevel(lvl string) (*zapcore.Level, error) {
	var zapLevel zapcore.Level
	switch strings.ToUpper(lvl) {
	case ERROR:
		zapLevel = zap.ErrorLevel
	case WARN:
		zapLevel = zap.WarnLevel
	case INFO:
		zapLevel = zap.InfoLevel
	case DEBUG:
		zapLevel = zap.DebugLevel
	default:
		return nil, errors.New("invalid log level")
	}
	return &zapLevel, nil
}

// 日志切割默认配置
const (
	DEFAULT_MAX_AGE       = 365 // 日志最长保存时间，单位：天
	DEFAULT_ROTATION_TIME = 6   // 日志滚动间隔，单位：小时
	DEFAULT_ROTATION_SIZE = 100 // 默认的日志滚动大小，单位：MB
)

// 日志滚动单位
const (
	ROTATION_SIZE_MB = 1024 * 1024
)

type color int

// 常用颜色
const (
	ColorBlack color = iota + 30
	ColorRed
	ColorGreen
	ColorYellow
	ColorBlue
	ColorMagenta
	ColorCyan
	ColorWhite
)

var colorList = [...]color{ColorRed, ColorGreen, ColorYellow, ColorBlue, ColorMagenta}

type FilteredLogger struct {
	logger *zap.SugaredLogger

	sensitiveDataFiltering     bool   //启用敏感数据过滤
	sensitiveDataEncryptionKey string //启用敏感数据加密（密钥)
}

func NewFilteredLogger(logger *zap.SugaredLogger,
	sensitiveDataFiltering bool, //启用敏感数据过滤
	sensitiveDataEncryptionKey string, //启用敏感数据加密（密钥)
) *FilteredLogger {
	return &FilteredLogger{logger: logger, sensitiveDataFiltering: sensitiveDataFiltering,
		sensitiveDataEncryptionKey: sensitiveDataEncryptionKey}
}

func (fl *FilteredLogger) filterSensitiveData(msg string) string {
	if fl.sensitiveDataFiltering {
		re := regexp.MustCompile(`\[\[(.*?)\]\]`)
		matches := re.FindAllStringSubmatch(msg, -1)

		for _, match := range matches {
			// 过滤敏感数据
			msg = strings.ReplaceAll(msg, match[0], "[[***]]")
		}

		return msg
	} else if fl.sensitiveDataEncryptionKey != "" {
		// 使用 sha256 哈希函数将普通密钥转换为 32 字节的密钥
		key := sha256.Sum256([]byte(fl.sensitiveDataEncryptionKey))

		// 创建一个新的 AES 密码块
		block, err := aes.NewCipher(key[:])
		if err != nil {
			return err.Error() + "failed to create AES cipher"
		}

		// 创建一个新的 GCM
		gcm, err := cipher.NewGCM(block)
		if err != nil {
			return err.Error() + "failed to create GCM"
		}

		// 创建一个随机 nonce
		nonce := make([]byte, gcm.NonceSize())
		if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
			return err.Error() + "failed to generate nonce"
		}

		// 识别并替换手机号
		re := regexp.MustCompile(`\[\[(.*?)\]\]`)
		msg = ciphertext(re, gcm, nonce, msg, "[[", "]]")

		return msg

	}
	return msg
}

func ciphertext(re *regexp.Regexp, gcm cipher.AEAD, nonce []byte, msg string, before string, after string) string {
	matches := re.FindAllString(msg, -1)
	for _, match := range matches {
		// 加密数据
		ciphertext := gcm.Seal(nonce, nonce, []byte(match), nil)

		// 将加密后的数据转换为 base64 字符串
		encryptedData := base64.StdEncoding.EncodeToString(ciphertext)
		// 替换敏感数据为加密后的数据
		msg = strings.ReplaceAll(msg, match, before+encryptedData+after)
	}
	return msg
}

func (fl *FilteredLogger) Filterf(format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	filteredMsg := fl.filterSensitiveData(msg)
	// 将过滤后的消息写入日志
	fl.logger.Info(filteredMsg)
}

func (fl *FilteredLogger) Filter(msg string) {
	filteredMsg := fl.filterSensitiveData(msg)
	// 将过滤后的消息写入日志
	fl.logger.Info(filteredMsg)
}

func (fl *FilteredLogger) Info(args ...interface{}) {
	fl.logger.Info(args...)
}

func (fl *FilteredLogger) Infof(template string, args ...interface{}) {
	fl.logger.Infof(template, args...)
}

func (fl *FilteredLogger) Warn(args ...interface{}) {
	fl.logger.Warn(args...)
}

func (fl *FilteredLogger) Warnf(template string, args ...interface{}) {
	fl.logger.Warnf(template, args...)
}

func (fl *FilteredLogger) Debug(args ...interface{}) {
	fl.logger.Debug(args...)
}

func (fl *FilteredLogger) Debugf(template string, args ...interface{}) {
	fl.logger.Debugf(template, args...)
}

func (fl *FilteredLogger) Error(args ...interface{}) {
	fl.logger.Error(args...)
}

func (fl *FilteredLogger) Errorf(template string, args ...interface{}) {
	fl.logger.Errorf(template, args...)
}

type LogConfig struct {
	Module       string    // module: module name
	ChainId      string    // chainId: chain id
	LogPath      string    // logPath: log file save path
	LogLevel     LOG_LEVEL // logLevel: log level
	MaxAge       int       // maxAge: the maximum number of days to retain old log files
	RotationTime int       // RotationTime: rotation time
	RotationSize int64     // RotationSize: rotation size Mb
	JsonFormat   bool      // jsonFormat: log file use json format
	ShowLine     bool      // showLine: show filename and line number
	LogInConsole bool      // logInConsole: show logs in console at the same time
	ShowColor    bool      // if true, show color log
	IsBrief      bool      // if true, only show log, won't print log level、caller func and line

	// StackTraceLevel record a stack trace for all messages at or above a given level.
	// Empty string or invalid level will not open stack trace.
	StackTraceLevel string

	IsCompress      bool   //启用压缩
	NoCompressCount int    //最近n个文件不压缩
	HmacKey         string //密钥
	ArchivePath     string //日志归档路径

	SensitiveDataFiltering     bool   //启用敏感数据过滤
	SensitiveDataEncryptionKey string //启用敏感数据加密（密钥)

	MultipleLogFiles bool //多日志文件，true：不同级别日志写入不同文件夹
}

// InitSugarLogger 基于配置初始化一个zap的SugaredLogger
// @param logConfig
// @param writer
// @return *zap.SugaredLogger
// @return zap.AtomicLevel
func InitSugarLogger(logConfig *LogConfig, writer ...io.Writer) (*zap.SugaredLogger, zap.AtomicLevel) {
	var level zapcore.Level
	switch logConfig.LogLevel {
	case LEVEL_DEBUG:
		level = zap.DebugLevel
	case LEVEL_INFO:
		level = zap.InfoLevel
	case LEVEL_WARN:
		level = zap.WarnLevel
	case LEVEL_ERROR:
		level = zap.ErrorLevel
	default:
		level = zap.InfoLevel
	}

	var sugaredLogger *zap.SugaredLogger
	aLevel := zap.NewAtomicLevel()
	aLevel.SetLevel(level)
	multipleLogFiles := logConfig.MultipleLogFiles
	if multipleLogFiles {
		sugaredLogger = newLoggerMultiLogDirs(logConfig, aLevel, writer...).Sugar()
	} else {
		sugaredLogger = newLogger(logConfig, aLevel, writer...).Sugar()
	}
	return sugaredLogger, aLevel
}

func InitFilterLogger(logConfig *LogConfig, writer ...io.Writer) (*FilteredLogger, zap.AtomicLevel) {
	var level zapcore.Level
	switch logConfig.LogLevel {
	case LEVEL_DEBUG:
		level = zap.DebugLevel
	case LEVEL_INFO:
		level = zap.InfoLevel
	case LEVEL_WARN:
		level = zap.WarnLevel
	case LEVEL_ERROR:
		level = zap.ErrorLevel
	default:
		level = zap.InfoLevel
	}

	var sugaredLogger *zap.SugaredLogger
	aLevel := zap.NewAtomicLevel()
	aLevel.SetLevel(level)
	multipleLogFiles := logConfig.MultipleLogFiles
	if multipleLogFiles {
		sugaredLogger = newLoggerMultiLogDirs(logConfig, aLevel, writer...).Sugar()
	} else {
		sugaredLogger = newLogger(logConfig, aLevel, writer...).Sugar()
	}
	return NewFilteredLogger(sugaredLogger, logConfig.SensitiveDataFiltering, logConfig.SensitiveDataEncryptionKey), aLevel
}

// newLogger 根据给定的 LogConfig 和 AtomicLevel 创建一个新的 zap.Logger。
// 可以选择传入一个或多个 io.Writer 作为附加的日志输出。
func newLogger(logConfig *LogConfig, level zap.AtomicLevel, writer ...io.Writer) *zap.Logger {
	// 获取文件钩子，用于日志轮转和文件输出
	hook, err := getHook(logConfig.LogPath, logConfig.MaxAge, logConfig.RotationTime, logConfig.RotationSize,
		logConfig.IsCompress, logConfig.NoCompressCount, logConfig.HmacKey, logConfig.ArchivePath)
	if err != nil {
		log.Fatalf("new logger get hook failed, %s", err)
	}

	// 创建一个 WriteSyncer，将日志输出到指定的文件钩子和其他 io.Writer（如 os.Stdout）
	var syncer zapcore.WriteSyncer
	syncers := []zapcore.WriteSyncer{zapcore.AddSync(hook)}
	if logConfig.LogInConsole {
		syncers = append(syncers, zapcore.AddSync(os.Stdout))
	}
	for _, outSyncer := range writer {
		syncers = append(syncers, zapcore.AddSync(outSyncer))
	}

	syncer = zapcore.NewMultiWriteSyncer(syncers...)

	// 根据 LogConfig 创建一个 EncoderConfig
	var encoderConfig zapcore.EncoderConfig
	if logConfig.IsBrief {
		encoderConfig = zapcore.EncoderConfig{
			TimeKey:    "time",
			MessageKey: "msg",
			EncodeTime: CustomTimeEncoder,
			LineEnding: zapcore.DefaultLineEnding,
		}
	} else {
		encoderConfig = zapcore.EncoderConfig{
			TimeKey:        "time",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "line",
			MessageKey:     "msg",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    CustomLevelEncoder,
			EncodeTime:     CustomTimeEncoder,
			EncodeDuration: zapcore.SecondsDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
			EncodeName:     zapcore.FullNameEncoder,
		}
	}

	// 根据 EncoderConfig 创建一个 Encoder
	var encoder zapcore.Encoder
	if logConfig.JsonFormat {
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	} else {
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	}

	// 使用 Encoder 和 WriteSyncer 创建一个新的 Core
	core := zapcore.NewCore(
		encoder,
		syncer,
		level,
	)

	// 根据日志配置设置链 ID
	chainId := fmt.Sprintf("@%s", logConfig.ChainId)
	if logConfig.ShowColor {
		chainId = getColorChainId(chainId)
	}

	// 设置日志的名称，包括模块名称和链 ID
	var name string
	if logConfig.ChainId != "" {
		name = fmt.Sprintf("%s %s", logConfig.Module, chainId)
	} else {
		name = logConfig.Module
	}

	// 创建一个新的 zap.Logger
	logger := zap.New(core).Named(name)

	// 确保在函数退出时同步日志
	defer func(logger *zap.Logger) {
		_ = logger.Sync()
	}(logger)

	// 如果配置中指定了显示行号，则添加调用者信息
	if logConfig.ShowLine {
		logger = logger.WithOptions(zap.AddCaller())
	}
	// 如果配置中指定了堆栈跟踪级别，则添加堆栈跟踪
	if lvl, err := getZapLevel(logConfig.StackTraceLevel); err == nil {
		logger = logger.WithOptions(zap.AddStacktrace(lvl))
	}
	// 跳过调用者栈帧，以便日志消息显示正确的函数名
	logger = logger.WithOptions(zap.AddCallerSkip(1))
	return logger
}

// newLogger 根据给定的 LogConfig 和 AtomicLevel 创建一个新的 zap.Logger。
// 可以选择传入一个或多个 io.Writer 作为附加的日志输出。
func newLoggerMultiLogDirs(logConfig *LogConfig, level zap.AtomicLevel, writer ...io.Writer) *zap.Logger {

	// 创建一个 map，键为日志级别，值为对应的 WriteSyncer
	levelWriters := make(map[zapcore.Level]zapcore.WriteSyncer)
	for _, lvl := range []zapcore.Level{zapcore.DebugLevel, zapcore.InfoLevel, zapcore.WarnLevel, zapcore.ErrorLevel} {

		filename := filepath.Join(filepath.Join(filepath.Dir(logConfig.LogPath), lvl.String()), "system.log."+lvl.String())
		hook, err := getHook(filename, logConfig.MaxAge, logConfig.RotationTime, logConfig.RotationSize,
			logConfig.IsCompress, logConfig.NoCompressCount, logConfig.HmacKey, logConfig.ArchivePath)
		if err != nil {
			log.Fatalf("new logger get hook failed, %s", err)
		}
		levelWriters[lvl] = zapcore.AddSync(hook)
	}

	//// 获取文件钩子，用于日志轮转和文件输出
	//hook, err := getHook(logConfig.LogPath, logConfig.MaxAge, logConfig.RotationTime, logConfig.RotationSize)
	//if err != nil {
	//	log.Fatalf("new logger get hook failed, %s", err)
	//}

	// 创建一个 WriteSyncer，将日志输出到指定的文件钩子和其他 io.Writer（如 os.Stdout）
	//var syncer zapcore.WriteSyncer
	//syncers := []zapcore.WriteSyncer{zapcore.AddSync(hook)}
	//if logConfig.LogInConsole {
	//	syncers = append(syncers, zapcore.AddSync(os.Stdout))
	//}
	//for _, outSyncer := range writer {
	//	syncers = append(syncers, zapcore.AddSync(outSyncer))
	//}
	//
	//syncer = zapcore.NewMultiWriteSyncer(syncers...)

	// 根据 LogConfig 创建一个 EncoderConfig
	var encoderConfig zapcore.EncoderConfig
	if logConfig.IsBrief {
		encoderConfig = zapcore.EncoderConfig{
			TimeKey:    "time",
			MessageKey: "msg",
			EncodeTime: CustomTimeEncoder,
			LineEnding: zapcore.DefaultLineEnding,
		}
	} else {
		encoderConfig = zapcore.EncoderConfig{
			TimeKey:        "time",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "line",
			MessageKey:     "msg",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    CustomLevelEncoder,
			EncodeTime:     CustomTimeEncoder,
			EncodeDuration: zapcore.SecondsDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
			EncodeName:     zapcore.FullNameEncoder,
		}
	}

	// 根据 EncoderConfig 创建一个 Encoder
	var encoder zapcore.Encoder
	if logConfig.JsonFormat {
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	} else {
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	}

	// 创建一个 Core，该 Core 可以将不同级别的日志写入到不同的目录中
	cores := []zapcore.Core{}
	for _, lvl := range []zapcore.Level{zapcore.DebugLevel, zapcore.InfoLevel, zapcore.WarnLevel, zapcore.ErrorLevel} {
		tLvl := lvl
		core := zapcore.NewCore(
			encoder,
			levelWriters[lvl],
			zap.LevelEnablerFunc(func(l zapcore.Level) bool {
				return l == tLvl
			}),
		)
		cores = append(cores, core)
	}
	combinedCore := zapcore.NewTee(cores...)
	// 使用 Encoder 和 WriteSyncer 创建一个新的 Core
	//core := zapcore.NewCore(
	//	encoder,
	//	syncer,
	//	level,
	//)

	// 根据日志配置设置链 ID
	chainId := fmt.Sprintf("@%s", logConfig.ChainId)
	if logConfig.ShowColor {
		chainId = getColorChainId(chainId)
	}

	// 设置日志的名称，包括模块名称和链 ID
	var name string
	if logConfig.ChainId != "" {
		name = fmt.Sprintf("%s %s", logConfig.Module, chainId)
	} else {
		name = logConfig.Module
	}

	// 创建一个新的 zap.Logger
	logger := zap.New(combinedCore).Named(name)
	//logger := zap.New(core).Named(name)

	// 确保在函数退出时同步日志
	defer func(logger *zap.Logger) {
		_ = logger.Sync()
	}(logger)

	// 如果配置中指定了显示行号，则添加调用者信息
	if logConfig.ShowLine {
		logger = logger.WithOptions(zap.AddCaller())
	}
	// 如果配置中指定了堆栈跟踪级别，则添加堆栈跟踪
	if lvl, err := getZapLevel(logConfig.StackTraceLevel); err == nil {
		logger = logger.WithOptions(zap.AddStacktrace(lvl))
	}
	// 跳过调用者栈帧，以便日志消息显示正确的函数名
	logger = logger.WithOptions(zap.AddCallerSkip(1))
	return logger
}

func getHook(filename string, maxAge, rotationTime int, rotationSize int64,
	isCompress bool, noCompressCount int, hmacKey string, archivePath string) (io.Writer, error) {

	hook, err := rotatelogs.New(
		filename+".%Y%m%d%H",
		rotatelogs.WithRotationTime(time.Hour*time.Duration(rotationTime)),
		//filename+".%Y%m%d%H%M",
		//rotatelogs.WithRotationSize(rotationSize*ROTATION_SIZE_MB),
		rotatelogs.WithLinkName(filename),
		rotatelogs.WithMaxAge(time.Hour*24*time.Duration(maxAge)),
		rotatelogs.WithCompress(isCompress),
		rotatelogs.WithNoCompressCount(noCompressCount),
		rotatelogs.WithHmacKey(hmacKey),
		rotatelogs.WithArchivePath(archivePath),
	)

	if err != nil {
		return nil, err
	}

	return hook, nil
}

// nolint: deadcode, unused
func recognizeLogLevel(l string) LOG_LEVEL {
	logLevel := strings.ToUpper(l)
	var level LOG_LEVEL
	switch logLevel {
	case DEBUG:
		level = LEVEL_DEBUG
	case INFO:
		level = LEVEL_INFO
	case WARN:
		level = LEVEL_WARN
	case ERROR:
		level = LEVEL_ERROR
	default:
		level = LEVEL_INFO
	}
	return level
}

// CustomLevelEncoder 自定义日志级别的输出格式
// @param level
// @param enc
func CustomLevelEncoder(level zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString("[" + level.CapitalString() + "]")
}

// CustomTimeEncoder 自定义时间转字符串的编码方法
// @param t
// @param enc
func CustomTimeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format("2006-01-02 15:04:05.000"))
}

// nolint: deadcode, unused
func showColor(color color, msg string) string {
	return fmt.Sprintf("\033[%dm%s\033[0m", int(color), msg)
}

func showColorBold(color color, msg string) string {
	return fmt.Sprintf("\033[%d;1m%s\033[0m", int(color), msg)
}

func getColorChainId(chainId string) string {
	c := crc32.ChecksumIEEE([]byte(chainId))
	color := colorList[int(c)%len(colorList)]
	return showColorBold(color, chainId)
}
