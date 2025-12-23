// package rotatelogs is a port of File-RotateLogs from Perl
// (https://metacpan.org/release/File-RotateLogs), and it allows
// you to automatically rotate output files when you write to them
// according to the filename pattern that you can specify.

package rotatelogs

// nolint:staticcheck
import (
	"compress/gzip"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	strftime "github.com/lestrrat-go/strftime"
	"github.com/pkg/errors"
)

const (
	GZ  = ".gz"
	ZIP = ".zip"
)

func (c clockFn) Now() time.Time {
	return c()
}

// New creates a new RotateLogs object. A log filename pattern
// must be passed. Optional `Option` parameters may be passed
func New(p string, options ...Option) (*RotateLogs, error) {
	globPattern := p
	for _, re := range patternConversionRegexps {
		globPattern = re.ReplaceAllString(globPattern, "*")
	}

	pattern, err := strftime.New(p)
	if err != nil {
		return nil, errors.Wrap(err, `invalid strftime pattern`)
	}

	rl := &RotateLogs{}
	rl.apply(globPattern, pattern, options...)
	if rl.maxAge > 0 && rl.rotationCount > 0 {
		return nil, errors.New("options MaxAge and RotationCount cannot be both set")
	}
	if rl.maxAge == 0 && rl.rotationCount == 0 {
		// if both are 0, give maxAge a sane default
		rl.maxAge = 7 * 24 * time.Hour
	}

	// 首次启动时执行压缩和归档
	rl.compressArchiveAsync()
	return rl, nil
}

func (rl *RotateLogs) apply(globPattern string, pattern *strftime.Strftime, options ...Option) {
	var (
		rotationSize  int64
		rotationCount uint
		linkName      string
		maxAge        time.Duration
		handler       Handler
		forceNewFile  bool
		clock         Clock = Local

		rotationTime    = 24 * time.Hour
		isCompress      = false
		noCompressCount = 1
		hmacKey         = ""
		archivePath     = ""
	)

	for _, o := range options {
		switch o.Name() {
		case optkeyClock:
			clock, _ = o.Value().(Clock)
		case optkeyLinkName:
			linkName, _ = o.Value().(string)
		case optkeyMaxAge:
			maxAge, _ = o.Value().(time.Duration)
			if maxAge < 0 {
				maxAge = 0
			}
		case optkeyRotationTime:
			rotationTime, _ = o.Value().(time.Duration)
			if rotationTime < 0 {
				rotationTime = 0
			}
		case optkeyCompress:
			isCompress, _ = o.Value().(bool)
		case optkeyNoCompressCount:
			noCompressCount, _ = o.Value().(int)
			if noCompressCount <= 1 {
				noCompressCount = 1
			}
		case optkeyHmacKey:
			hmacKey, _ = o.Value().(string)
		case optkeyArchivePath:
			archivePath, _ = o.Value().(string)
			if !validateArchivePath(archivePath) {
				archivePath = ""
			}
		case optkeyRotationSize:
			rotationSize, _ = o.Value().(int64)
			if rotationSize < 0 {
				rotationSize = 0
			}
		case optkeyRotationCount:
			rotationCount, _ = o.Value().(uint)
		case optkeyHandler:
			handler, _ = o.Value().(Handler)
		case optkeyForceNewFile:
			forceNewFile = true
		}
	}

	rl.clock = clock
	rl.eventHandler = handler
	rl.globPattern = globPattern
	rl.linkName = linkName
	rl.maxAge = maxAge
	rl.pattern = pattern
	rl.rotationTime = rotationTime
	rl.rotationSize = rotationSize
	rl.rotationCount = rotationCount
	rl.forceNewFile = forceNewFile
	rl.hmacKey = hmacKey
	rl.archivePath = archivePath

	rl.isCompress = isCompress
	rl.noCompressCount = noCompressCount
}

func (rl *RotateLogs) genFilename() string {
	now := rl.clock.Now()

	// XXX HACK: Truncate only happens in UTC semantics, apparently.
	// observed values for truncating given time with 86400 secs:
	//
	// before truncation: 2018/06/01 03:54:54 2018-06-01T03:18:00+09:00
	// after  truncation: 2018/06/01 03:54:54 2018-05-31T09:00:00+09:00
	//
	// This is really annoying when we want to truncate in local time
	// so we hack: we take the apparent local time in the local zone,
	// and pretend that it's in UTC. do our math, and put it back to
	// the local zone
	var base time.Time
	if now.Location() != time.UTC {
		base = time.Date(
			now.Year(),
			now.Month(),
			now.Day(),
			now.Hour(),
			now.Minute(),
			now.Second(),
			now.Nanosecond(),
			time.UTC,
		)
		base = base.Truncate(time.Duration(rl.rotationTime))
		base = time.Date(
			base.Year(),
			base.Month(),
			base.Day(),
			base.Hour(),
			base.Minute(),
			base.Second(),
			base.Nanosecond(),
			base.Location(),
		)
	} else {
		base = now.Truncate(time.Duration(rl.rotationTime))
	}
	return rl.pattern.FormatString(base)
}

func hmacLogEntry(p []byte, key string) []byte {
	h := hmac.New(sha512.New, []byte(key))
	h.Write(p)
	return h.Sum(nil)
}

// Write 方法满足 io.Writer 接口。它将数据写入当前正在使用的适当文件句柄。
// 如果已到达旋转时间，则目标文件会自动旋转，并在必要时清除。
func (rl *RotateLogs) Write(p []byte) (n int, err error) {
	// 保护并发写操作
	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	if len(rl.hmacKey) > 0 {
		// 计算日志条目的 HMAC 值
		hmacValue := hmacLogEntry(p[:len(p)-1], rl.hmacKey)
		// 将 HMAC 值附加到日志条目中
		p = append(p[:len(p)-1], []byte(fmt.Sprintf(" [hmac: %s] \n", hex.EncodeToString(hmacValue)))...)
	}

	// 获取当前正在使用的 io.Writer，不加锁
	out, err := rl.getWriterNoLock(false, false)
	if err != nil {
		return 0, errors.Wrap(err, `failed to acquite target io.Writer`)
	}

	// 将数据写入文件
	return out.Write(p)
}

//nolint:deadcode,unused
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

// getWriterNoLock 函数在不加锁的情况下获取用于写入日志的 io.Writer。
// 该函数主要负责在需要时创建新的日志文件，并执行文件旋转操作。
// 必须在操作期间锁定
func (rl *RotateLogs) getWriterNoLock(bailOnRotateFail, useGenerationalNames bool) (io.Writer, error) {

	var (
		// 生成基本文件名
		baseFn = rl.genFilename()
		// 获取当前文件名
		previousFn = rl.curFn
		// 查找下一个文件名和对应的代数
		filename, generation = rl.findNextFile(baseFn, useGenerationalNames)
	)

	// 如果文件名为空，直接返回当前的文件句柄
	if len(filename) == 0 {
		return rl.outFh, nil
	}

	// make sure the dir is existed, eg:
	// ./foo/bar/baz/hello.log must make sure ./foo/bar/baz is existed
	dirname := filepath.Dir(filename)
	if err := os.MkdirAll(dirname, 0755); err != nil {
		return nil, errors.Wrapf(err, "failed to create directory %s", dirname)
	}
	// if we got here, then we need to create a file
	fh, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, errors.Errorf("failed to open file %s: %s", rl.pattern, err)
	}

	// 执行文件旋转操作
	if err := rl.rotateNoLock(filename); err != nil {
		err = errors.Wrap(err, "failed to rotate")
		if bailOnRotateFail {
			// Failure to rotate is a problem, but it's really not a great
			// idea to stop your application just because you couldn't rename
			// your log.
			//
			// We only return this error when explicitly needed (as specified by bailOnRotateFail)
			//
			// However, we *NEED* to close `fh` here
			if fh != nil { // probably can't happen, but being paranoid
				fh.Close()
			}
			return nil, err
		}
	}

	// 关闭当前的文件句柄，并将新的文件句柄赋值给 outFh
	rl.outFh.Close()
	rl.outFh = fh
	rl.curBaseFn = baseFn
	rl.curFn = filename
	rl.generation = generation

	// 如果设置了事件处理器，触发文件旋转事件
	if h := rl.eventHandler; h != nil {
		go h.Handle(&FileRotatedEvent{
			prev:    previousFn,
			current: filename,
		})
	}
	return fh, nil
}

func (rl *RotateLogs) findNextFile(baseFn string, useGenerationalNames bool) (string, int) {
	var (
		forceNewFile bool
		generation   = rl.generation
		filename     = baseFn
		sizeRotation = false
	)

	fi, err := os.Stat(rl.curFn)
	if err == nil && rl.rotationSize > 0 && rl.rotationSize <= fi.Size() {
		forceNewFile = true
		sizeRotation = true
	}

	if baseFn != rl.curBaseFn {
		generation = 0
		// even though this is the first write after calling New(),
		// check if a new file needs to be created
		if rl.forceNewFile {
			forceNewFile = true
		}
	} else {
		if !useGenerationalNames && !sizeRotation {
			// nothing to do
			return "", -1
		}
		forceNewFile = true
		generation++
	}
	if forceNewFile {
		// A new file has been requested. Instead of just using the
		// regular strftime pattern, we create a new file name using
		// generational names such as "foo.1", "foo.2", "foo.3", etc
		var name string
		for {
			if generation == 0 {
				name = filename
			} else {
				name = fmt.Sprintf("%s.%d", filename, generation)
			}
			if _, err := os.Stat(name); err != nil {
				filename = name
				break
			}
			generation++
		}
	}
	return filename, generation
}

// CurrentFileName returns the current file name that
// the RotateLogs object is writing to
func (rl *RotateLogs) CurrentFileName() string {
	rl.mutex.RLock()
	defer rl.mutex.RUnlock()
	return rl.curFn
}

var patternConversionRegexps = []*regexp.Regexp{
	regexp.MustCompile(`%[%+A-Za-z]`),
	regexp.MustCompile(`\*+`),
}

type cleanupGuard struct {
	enable bool
	fn     func()
	mutex  sync.Mutex
}

func (g *cleanupGuard) Enable() {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	g.enable = true
}
func (g *cleanupGuard) Run() {
	g.fn()
}

// Rotate 强制旋转日志文件。如果生成的文件名发生冲突，因为文件已经存在，
// 则形如 ".1", ".2", ".3" 等数字后缀将附加到日志文件的末尾。
//
// 该方法可以与信号处理程序一起使用，以模拟在接收到 SIGHUP 时生成新日志文件的服务器。
func (rl *RotateLogs) Rotate() error {
	// 锁定以防止并发操作
	rl.mutex.Lock()
	defer rl.mutex.Unlock()
	// 获取当前正在使用的 io.Writer，强制旋转且使用代数名称
	if _, err := rl.getWriterNoLock(true, true); err != nil {
		return err
	}
	return nil
}

// rotateNoLock 函数负责在不加锁的情况下执行日志文件的旋转操作。
func (rl *RotateLogs) rotateNoLock(filename string) error {
	// 创建一个锁文件，确保同一时间只有一个进程在执行旋转操作
	lockfn := filename + `_lock`
	fh, err := os.OpenFile(lockfn, os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		// Can't lock, just return
		return err
	}

	// 定义清理锁文件的函数
	var guard = cleanupGuard{
		fn: func() {
			fh.Close()
			os.Remove(lockfn)
		},
	}
	// 在函数执行完成后执行清理操作
	defer guard.Run()

	// 链接文件
	if err = rl.linkFile(filename); err != nil {
		return err
	}

	// 如果 maxAge 和 rotationCount 都设置了值，则抛出错误
	if rl.maxAge <= 0 && rl.rotationCount <= 0 {
		return errors.New("panic: maxAge and rotationCount are both set")
	}

	// 使用 glob 模式匹配文件
	matches, err := filepath.Glob(rl.globPattern)
	if err != nil {
		return err
	}

	//异步执行压缩和归档
	rl.compressArchiveAsync()

	// 计算截止时间
	cutoff := rl.clock.Now().Add(-1 * rl.maxAge)
	// 获取需要解除链接的文件列表
	toUnlink := rl.getUnLinkFiles(matches, cutoff)

	// 如果没有需要解除链接的文件直接返回
	if len(toUnlink) <= 0 {
		return nil
	}

	// 启用清理锁文件的函数
	guard.Enable()
	go func() {
		// unlink files on a separate goroutine
		for _, path := range toUnlink {
			os.Remove(path)
		}
	}()

	return nil
}

func (rl *RotateLogs) linkFile(filename string) error {
	if rl.linkName != "" {
		tmpLinkName := filename + `_symlink`

		// Change how the link name is generated based on where the
		// target location is. if the location is directly underneath
		// the main filename's parent directory, then we create a
		// symlink with a relative path
		var (
			linkDest = filename
			linkDir  = filepath.Dir(rl.linkName)
			baseDir  = filepath.Dir(filename)
		)
		if strings.Contains(rl.linkName, baseDir) {
			tmp, err := filepath.Rel(linkDir, filename)
			if err != nil {
				return errors.Wrapf(err, `failed to evaluate relative path from %#v to %#v`, baseDir, rl.linkName)
			}
			linkDest = tmp
		}
		if err := os.Symlink(linkDest, tmpLinkName); err != nil {
			return errors.Wrap(err, `failed to create new symlink`)
		}
		// the directory where rl.linkName should be created must exist
		if _, err := os.Stat(linkDir); err != nil { // Assume err != nil means the directory doesn't exist
			if err := os.MkdirAll(linkDir, 0755); err != nil {
				return errors.Wrapf(err, `failed to create directory %s`, linkDir)
			}
		}
		if err := os.Rename(tmpLinkName, rl.linkName); err != nil {
			return errors.Wrap(err, `failed to rename new symlink`)
		}
	}
	return nil
}

// getUnLinkFiles 函数的作用是从给定的文件列表matches中筛选出需要删除（unlink）的文件列表。
// 筛选条件基于文件的最大年龄maxAge和文件的旋转计数rotationCount。
func (rl *RotateLogs) getUnLinkFiles(matches []string, cutoff time.Time) []string {
	var toUnlink []string
	//遍历所有文件路径
	for _, path := range matches {
		// 忽略锁文件和符号链接文件
		if strings.HasSuffix(path, "_lock") || strings.HasSuffix(path, "_symlink") {
			continue
		}

		// 获取文件信息
		fi, err := os.Stat(path)
		if err != nil {
			continue
		}

		// 获取文件的模式（以检查它是否是符号链接）
		fl, err := os.Lstat(path)
		if err != nil {
			continue
		}

		// 如果设置了 maxAge 并且文件的修改时间在截止时间之后，则跳过此文件
		if rl.maxAge > 0 && fi.ModTime().After(cutoff) {
			continue
		}

		// 如果设置了 rotationCount 并且文件是符号链接，则跳过此文件
		if rl.rotationCount > 0 && fl.Mode()&os.ModeSymlink == os.ModeSymlink {
			continue
		}

		// 将文件路径添加到要解除链接的文件列表中
		toUnlink = append(toUnlink, path)
	}
	// 如果设置了 rotationCount，则仅在文件数大于 rotationCount 时删除文件。
	// 这是通过切片 toUnlink 列表仅包含最旧文件来完成的。
	if rl.rotationCount > 0 {
		// Only delete if we have more than rotationCount
		if rl.rotationCount >= uint(len(toUnlink)) {
			return nil
		}
		// nolint:gosec
		toUnlink = toUnlink[:len(toUnlink)-int(rl.rotationCount)]
	}

	// 返回要解除链接的文件路径列表
	return toUnlink
}

// getFilesToCompress 返回需要压缩的文件列表。
// 它根据文件的修改时间和文件是否是符号链接来过滤给定的文件路径列表（"matches"）。
// 参数 noCompressCount 表示最近的 n 个文件不进行压缩。
func (rl *RotateLogs) getFilesToCompress(matches []string, noCompressCount int) []string {
	var toCompress []string

	// 遍历所有文件路径
	for _, path := range matches {
		// 忽略锁文件和符号链接文件
		if strings.HasSuffix(path, "_lock") ||
			strings.HasSuffix(path, "_symlink") ||
			strings.HasSuffix(path, GZ) ||
			strings.HasSuffix(path, ZIP) {
			continue
		}

		// 获取文件的模式（以检查它是否是符号链接）
		fl, err := os.Lstat(path)
		if err != nil {
			continue
		}

		// 如果文件是符号链接，则跳过此文件
		if fl.Mode()&os.ModeSymlink == os.ModeSymlink {
			continue
		}

		// 将文件路径添加到要压缩的文件列表中
		toCompress = append(toCompress, path)
	}

	// 根据 noCompressCount 参数确定需要压缩的文件范围
	compressStartIndex := len(toCompress) - noCompressCount
	if compressStartIndex < 0 {
		compressStartIndex = 0
	}

	toCompress = toCompress[:compressStartIndex]

	return toCompress
}

func (rl *RotateLogs) getFilesToArchive(matches []string) []string {
	var toArchive []string

	// 遍历所有文件路径
	for _, path := range matches {
		// 忽略锁文件和符号链接文件
		if strings.HasSuffix(path, GZ) ||
			strings.HasSuffix(path, ZIP) {
			// 将文件路径添加到要归档的文件列表中
			toArchive = append(toArchive, path)
		}
	}
	return toArchive
}

func validateArchivePath(archivePath string) bool {
	// 检查路径是否存在
	fileInfo, err := os.Stat(archivePath)
	if err != nil {

		// 路径不存在，创建目录
		err = os.MkdirAll(archivePath, 0755)
		if err != nil {
			return false
		}
		fileInfo, err = os.Stat(archivePath)
		if err != nil {
			return false
		}
	}

	// 检查路径是否是目录
	if !fileInfo.IsDir() {
		return false
	}

	return true
}

// Close satisfies the io.Closer interface. You must
// call this method if you performed any writes to
// the object.
func (rl *RotateLogs) Close() error {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	if rl.outFh == nil {
		return nil
	}

	rl.outFh.Close()
	rl.outFh = nil
	return nil
}

// compressArchiveAsync 异步压缩/归档日志文件
// 解决因为大文件压缩耗时过长造成流程阻塞问题
func (rl *RotateLogs) compressArchiveAsync() {
	go func() {
		processingMutex.Lock()
		defer processingMutex.Unlock()

		if rl.isCompress {
			matches, err := filepath.Glob(rl.globPattern)
			if err != nil {
				return
			}
			toCompressFiles := rl.getFilesToCompress(matches, rl.noCompressCount)
			for _, filename := range toCompressFiles {
				err := CompressAndDelete(filename, filename+GZ)
				if err != nil {
					rl.handleError(filename, err)
				}
			}
		}

		// 执行归档
		if len(rl.archivePath) > 0 {
			matches, err := filepath.Glob(rl.globPattern)
			if err != nil {
				return
			}
			toArchiveFiles := rl.getFilesToArchive(matches)
			for _, file := range toArchiveFiles {
				archiveGzPath := filepath.Join(rl.archivePath, filepath.Base(file))
				//nolint:errcheck
				if err := os.Rename(file, archiveGzPath); err != nil {
					rl.handleError(file, err)
				}

			}
		}
	}()
}

// CompressAndDelete 压缩文件并在成功后删除原文件
// 参数:
//
//	srcPath: 待压缩的文件路径
//	destPath: 压缩后的文件路径
//
// 返回值:
//
//	error: 如果压缩或删除过程中出现错误，返回错误信息
func CompressAndDelete(srcPath, destPath string) error {
	// 打开源文件
	srcFile, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %v", err)
	}
	defer srcFile.Close()

	// 创建目标文件
	destFile, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %v", err)
	}
	defer destFile.Close()

	// 创建gzip.Writer
	gzWriter := gzip.NewWriter(destFile)
	defer gzWriter.Close()

	// 使用缓冲区逐块读取源文件并写入gzip.Writer
	buf := make([]byte, 1024*1024) // 1MB的缓冲区
	for {
		n, err := srcFile.Read(buf)
		if err != nil && err != io.EOF {
			return fmt.Errorf("error writing compressed data: %v", err)
		}
		if n == 0 {
			break // 文件读取完毕
		}

		// 将读取的数据写入gzip.Writer
		_, err = gzWriter.Write(buf[:n])
		if err != nil {
			return fmt.Errorf("error writing compressed data: %v", err)
		}
	}

	// 确保所有数据都写入目标文件
	if err := gzWriter.Flush(); err != nil {
		return fmt.Errorf("error flushing compressed data: %v", err)
	}

	// 压缩成功后删除源文件
	if err := os.Remove(srcPath); err != nil {
		return fmt.Errorf("failed to delete source file: %v", err)
	}

	return nil
}

// 辅助方法：错误处理
func (rl *RotateLogs) handleError(filename string, err error) {
	// 可根据需要实现重试或日志记录
	fmt.Printf("Error processing %s: %v\n", filename, err)
}
