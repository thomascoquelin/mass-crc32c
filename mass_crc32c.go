package main

import (
	"bufio"
	"compress/gzip"
	"encoding/base64"
	"encoding/binary"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type MassCRC32C struct {
	wg          sync.WaitGroup
	pathQueueG  chan string
	interrupted bool

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
	stdOut   io.Writer
	errOut   io.Writer
	debugOut io.Writer
}

func (mc *MassCRC32C) printErr(path string, err error) {
	fmt.Fprintf(mc.errOut, "error: '%s': %v\n", path, err)
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
	for path := range mc.pathQueueG { // consume the messages in the queue
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
	fmt.Fprintf(mc.stdOut, "%s %d %s\n", crc, fileSize, path)
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

func (mc *MassCRC32C) walkHandler(path string, dir fs.DirEntry, err error) error {
	if mc.interrupted {
		return io.EOF
	}
	if err != nil {
		if dir.IsDir() {
			fmt.Fprintf(mc.errOut, "dir error: '%s': %v\n", path, err)
			atomic.AddUint64(&mc.directoryErrorCount, 1)
		} else {
			fmt.Fprintf(mc.errOut, "file error: '%s': %v\n", path, err)
			atomic.AddUint64(&mc.fileErrorCount, 1)
		}
		return nil
	}
	if dir.IsDir() {
		fmt.Fprintf(mc.debugOut, "entering dir: %s\n", path)
		return nil
	}
	if !dir.Type().IsRegular() {
		fmt.Fprintf(mc.debugOut, "ignoring: %s\n", path)
		atomic.AddUint64(&mc.ignoredFilesCount, 1)
		return nil
	}
	mc.pathQueueG <- path // add a path message to the queue (blocking when queue is full)
	return nil
}

func printUsage() {
	fmt.Fprintf(
		os.Stderr,
		"Usage of %s: [options] [path ...]\n%s recurses over paths provided as arguments or gets the file list form stdin otherwize\nOptions:\n",
		os.Args[0],
		os.Args[0],
	)
	flag.PrintDefaults()
}

func main() {
	p := flag.Int("p", 1, "# of cpu used")
	jobCountP := flag.Int("j", 1, "# of parallel reads")
	listQueueLength := flag.Int("l", 100, "size of list ahead queue")
	readSizeP := flag.Int("s", 1, "size of reads in kbytes")
	outFile := flag.String("out", "", "write CRC to file")
	outErr := flag.String("errout", "", "write errors to file")
	compress := flag.Bool("c", false, "enable file output compression")
	flag.Usage = printUsage

	flag.Parse()

	runtime.GOMAXPROCS(*p) // limit number of kernel threads (CPUs used)

	mc := initMassCRC32C(*readSizeP, *listQueueLength)
	if *outFile != "" {
		f, err := os.OpenFile(*outFile, os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			os.Exit(2)
		}
		defer f.Close()
		if *compress {
			gzWriter := gzip.NewWriter(f)
			defer func(gzWriter *gzip.Writer) {
				err := gzWriter.Flush()
				if err != nil {
					fmt.Fprintf(mc.debugOut, "Error: failed to flush gzip stream: %v", err)
				}
				err = gzWriter.Close()
				if err != nil {
					fmt.Fprintf(mc.debugOut, "Error: failed to close gzip stream: %v", err)
				}
			}(gzWriter)
			mc.stdOut = gzWriter
		} else {
			mc.stdOut = f
		}
	}
	if *outErr != "" {
		f, err := os.OpenFile(*outErr, os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			os.Exit(2)
		}

		if *compress {
			gzWriter := gzip.NewWriter(f)
			defer func(gzWriter *gzip.Writer) {
				err := gzWriter.Flush()
				if err != nil {
					fmt.Fprintf(mc.debugOut, "Error: failed to flush gzip stream: %v", err)
				}
				err = gzWriter.Close()
				if err != nil {
					fmt.Fprintf(mc.debugOut, "Error: failed to close gzip stream: %v", err)
				}
			}(gzWriter)
			mc.errOut = gzWriter
		} else {
			mc.errOut = f
		}
		defer f.Close()
	}
	mc.startup(*jobCountP)

	// Notify walk to gracefully stop on a CTRL+C via the 'interrupted' flag
	mc.signalToSummary(mc.startTime)

	if flag.NArg() == 0 {
		mc.readFileList()
	} else {
		mc.walkDirectories(mc.walkHandler)
	}
	mc.tearDown()
	mc.printSummary(mc.startTime)
}

func (mc *MassCRC32C) tearDown() {
	close(mc.pathQueueG)
	mc.wg.Wait()
}

func initMassCRC32C(
	readSize int,
	queueLength int,
) *MassCRC32C {
	var mc MassCRC32C
	mc.readSizeG = readSize
	mc.crc32cTableG = crc32.MakeTable(crc32.Castagnoli)
	mc.pathQueueG = make(chan string, queueLength) // use a channel with a size to limit the number of list ahead path

	mc.bufferPool = sync.Pool{New: func() any { return make([]byte, 1024*mc.readSizeG) }}

	mc.HandlerFunc = mc.fileHandler

	mc.stdin = os.Stdin
	mc.stdOut = os.Stdout
	mc.errOut = os.Stderr
	mc.debugOut = os.Stderr

	// Notify walk to gracefully stop on a CTRL+C via the 'interrupted' flag
	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt)
	go func() {
		<-interruptChan
		mc.interrupted = true
	}()
	return &mc
}

func (mc *MassCRC32C) startup(jobCount int) {
	// create the coroutines
	for i := 0; i < jobCount; i++ {
		mc.wg.Add(1)
		go mc.queueHandler(mc.HandlerFunc)
	}
	mc.startTime = time.Now()
}

func (mc *MassCRC32C) printSummary(startTime time.Time) {
	duration := time.Now().Sub(startTime)
	_, _ = fmt.Fprintf(
		mc.debugOut,
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

func (mc *MassCRC32C) walkDirectories(handlerFunc fs.WalkDirFunc) {
	for _, arg := range flag.Args() {
		err := filepath.WalkDir(arg, handlerFunc)
		if err == io.EOF {
			fmt.Fprintln(mc.errOut, "directory walk interrupted")
			break
		} else if err != nil {
			fmt.Fprintf(mc.errOut, "error while walking: %v\n", err)
			break
		}
	}
}

func (mc *MassCRC32C) readFileList() {
	lineScanner := bufio.NewScanner(mc.stdin)
	for lineScanner.Scan() {
		mc.pathQueueG <- lineScanner.Text()
		if err := lineScanner.Err(); err != nil {
			fmt.Fprintf(mc.errOut, "error while reading stdin: %v\n", err)
			break
		}
	}
}
