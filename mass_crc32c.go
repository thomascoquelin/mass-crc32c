package main

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"
)

type MassCRC32C struct {
	wg          sync.WaitGroup
	PathQueueG  chan string
	Interrupted bool

	readSizeG    int
	crc32cTableG *crc32.Table

	startTime           time.Time
	fileCount           uint64
	fileErrorCount      uint64
	directoryErrorCount uint64
	ignoredFilesCount   uint64
	totalDataComputed   uint64

	bufferPool  sync.Pool
	HandlerFunc func(path string) error

	stdin    io.Reader
	StdOut   io.Writer
	ErrOut   io.Writer
	DebugOut io.Writer
}

func (mc *MassCRC32C) printErr(path string, err error) {
	fmt.Fprintf(mc.ErrOut, "error: '%s': %v\n", path, err)
}

func (mc *MassCRC32C) CRCReader(reader io.Reader) (string, uint64, error) {
	checksum := crc32.Checksum([]byte(""), mc.crc32cTableG)
	buf := mc.bufferPool.Get().([]byte)
	defer func() { mc.bufferPool.Put(buf) }()
	fileSize := uint64(0)
	for {
		switch n, err := reader.Read(buf); err {
		case nil:
			checksum = crc32.Update(checksum, mc.crc32cTableG, buf[:n])
			fileSize += uint64(n)
		case io.EOF:
			b := make([]byte, 4)
			binary.BigEndian.PutUint32(b, checksum)
			str := base64.StdEncoding.EncodeToString(b)
			return str, fileSize, nil
		default:
			return "", 0, err
		}
	}
}

func (mc *MassCRC32C) queueHandler(handler func(path string) error) {
	defer mc.wg.Done()
	for path := range mc.PathQueueG { // consume the messages in the queue
		err := handler(path)
		if err != nil {
			break
		}
	}
	return
}

func (mc *MassCRC32C) fileHandler(path string) error {
	err, fileSize, crc := mc.pathToCRC(path)
	if err != nil {
		mc.printErr(path, err)
		atomic.AddUint64(&mc.fileErrorCount, 1)
		return nil
	}
	fmt.Fprintf(mc.StdOut, "%s %d %s\n", crc, fileSize, path)
	atomic.AddUint64(&mc.fileCount, 1)
	atomic.AddUint64(&mc.totalDataComputed, fileSize)
	return nil
}

func (mc *MassCRC32C) pathToCRC(path string) (error, uint64, string) {
	file, err := os.Open(path)
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			mc.printErr(path, err)
		}
	}(file)
	if err != nil {
		return err, 0, ""
	}
	crc, fileSize, err := mc.CRCReader(file)
	return err, fileSize, crc
}

func InitMassCRC32C(
	readSize int,
	queueLength int,
) *MassCRC32C {
	var mc MassCRC32C
	mc.readSizeG = readSize
	mc.crc32cTableG = crc32.MakeTable(crc32.Castagnoli)
	mc.PathQueueG = make(chan string, queueLength) // use a channel with a size to limit the number of list ahead path

	mc.bufferPool = sync.Pool{New: func() any { return make([]byte, 1024*mc.readSizeG) }}

	mc.HandlerFunc = mc.fileHandler

	mc.stdin = os.Stdin
	mc.StdOut = os.Stdout
	mc.ErrOut = os.Stderr
	mc.DebugOut = os.Stderr

	// Notify walk to gracefully stop on a CTRL+C via the 'interrupted' flag
	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt)
	go func() {
		<-interruptChan
		mc.Interrupted = true
	}()
	return &mc
}

func (mc *MassCRC32C) Startup(jobCount int) {
	// create the coroutines
	for i := 0; i < jobCount; i++ {
		mc.wg.Add(1)
		go mc.queueHandler(mc.HandlerFunc)
	}
	mc.startTime = time.Now()

	// Use SIGUSR1 to print summary to debug output
	mc.signalToSummary()
}

func (mc *MassCRC32C) TearDown() {
	close(mc.PathQueueG)
	mc.wg.Wait()
}

func (mc *MassCRC32C) PrintSummary() {
	duration := time.Now().Sub(mc.startTime)
	_, _ = fmt.Fprintf(
		mc.DebugOut,
		"Summary:\n"+
			"Files computed: %d\n"+
			"File errors: %d\n"+
			"Folder errors: %d\n"+
			"Ignored files: %d\n"+
			"Computed data: %dB\n"+
			"Duration: %s\n"+
			"Avg file speed: %d/s\n"+
			"Avg data speed: %dMB/s\n",
		mc.fileCount,
		mc.fileErrorCount,
		mc.directoryErrorCount,
		mc.ignoredFilesCount,
		mc.totalDataComputed,
		duration.String(),
		int(float64(mc.fileCount)/duration.Seconds()),
		int(float64(mc.totalDataComputed)/duration.Seconds()/1024/1024),
	)
}
