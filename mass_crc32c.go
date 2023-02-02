package main

import (
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

var wg sync.WaitGroup
var pathQueueG chan string
var interrupted bool

var readSizeG int
var crc32cTableG *crc32.Table

var fileCount uint64
var fileErrorCount uint64
var directoryErrorCount uint64
var ignoredFilesCount uint64
var totalDataComputed uint64

var bufferPool sync.Pool

func printErr(path string, err error) {
	fmt.Fprintf(os.Stderr, "error: '%s': %v\n", path, err)
}

func CRCReader(reader io.Reader) (string, uint64, error) {
	checksum := crc32.Checksum([]byte(""), crc32cTableG)
	buf := bufferPool.Get().([]byte)
	defer func() { bufferPool.Put(buf) }()
	fileSize := uint64(0)
	for {
		switch n, err := reader.Read(buf); err {
		case nil:
			checksum = crc32.Update(checksum, crc32cTableG, buf[:n])
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

func queueHandler(handler func(path string) error) {
	defer wg.Done()
	for path := range pathQueueG { // consume the messages in the queue
		err := handler(path)
		if err != nil {
			break
		}
	}
	return
}

func fileHandler(path string) error {
	err, fileSize, crc := pathToCRC(path)
	if err != nil {
		printErr(path, err)
		atomic.AddUint64(&fileErrorCount, 1)
		return nil
	}
	fmt.Printf("%s %d %s\n", crc, fileSize, path)
	atomic.AddUint64(&fileCount, 1)
	atomic.AddUint64(&totalDataComputed, fileSize)
	return nil
}

func pathToCRC(path string) (error, uint64, string) {
	file, err := os.Open(path)
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			printErr(path, err)
		}
	}(file)
	if err != nil {
		return err, 0, ""
	}
	crc, fileSize, err := CRCReader(file)
	return err, fileSize, crc
}

func walkHandler(path string, dir fs.DirEntry, err error) error {
	if interrupted {
		return io.EOF
	}
	if err != nil {
		if dir.IsDir() {
			fmt.Fprintf(os.Stderr, "dir error: '%s': %v\n", path, err)
			atomic.AddUint64(&directoryErrorCount, 1)
		} else {
			fmt.Fprintf(os.Stderr, "file error: '%s': %v\n", path, err)
			atomic.AddUint64(&fileErrorCount, 1)
		}
		return nil
	}
	if dir.IsDir() {
		fmt.Fprintf(os.Stderr, "entering dir: %s\n", path)
		return nil
	}
	if !dir.Type().IsRegular() {
		fmt.Fprintf(os.Stderr, "ignoring: %s\n", path)
		atomic.AddUint64(&ignoredFilesCount, 1)
		return nil
	}
	pathQueueG <- path // add a path message to the queue (blocking when queue is full)
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
	flag.Usage = printUsage

	flag.Parse()

	runtime.GOMAXPROCS(*p) // limit number of kernel threads (CPUs used)

	setupWorkers(*readSizeP, *jobCountP, *listQueueLength, fileHandler)

	startTime := time.Now()

	// Notify walk to gracefully stop on a CTRL+C via the 'interrupted' flag
	signalToSummary(startTime)

	if flag.NArg() == 0 {
		scanLn := fmt.Scanln
		readFileList(scanLn)
	} else {
		walkDirectories(walkHandler)
	}
	tearDown()
	printSummary(startTime)
}

func tearDown() {
	close(pathQueueG)
	wg.Wait()
}

func setupWorkers(
	readSize int,
	jobCount int,
	queueLength int,
	handler func(path string) error,
) {
	readSizeG = readSize
	crc32cTableG = crc32.MakeTable(crc32.Castagnoli)
	pathQueueG = make(chan string, queueLength) // use a channel with a size to limit the number of list ahead path

	bufferPool = sync.Pool{New: func() any { return make([]byte, 1024*readSizeG) }}

	// create the coroutines
	for i := 0; i < jobCount; i++ {
		wg.Add(1)
		go queueHandler(handler)
	}

	// Notify walk to gracefully stop on a CTRL+C via the 'interrupted' flag
	interuptChan := make(chan os.Signal, 1)
	signal.Notify(interuptChan, os.Interrupt)
	go func() {
		<-interuptChan
		interrupted = true
	}()
}

func printSummary(startTime time.Time) {
	duration := time.Now().Sub(startTime)
	_, _ = fmt.Fprintf(
		os.Stderr,
		"Summary:\n"+
			"Files computed: %d\n"+
			"File errors: %d\n"+
			"Folder errors: %d\n"+
			"Ignored files: %d\n"+
			"Computed data: %dB\n"+
			"Duration: %s\n"+
			"Avg file speed: %d/s\n"+
			"Avg data speed: %dMB/s\n",
		fileCount,
		fileErrorCount,
		directoryErrorCount,
		ignoredFilesCount,
		totalDataComputed,
		duration.String(),
		int(float64(fileCount)/duration.Seconds()),
		int(float64(totalDataComputed)/duration.Seconds()/1024/1024),
	)
}

func walkDirectories(handlerFunc fs.WalkDirFunc) {
	for _, arg := range flag.Args() {
		err := filepath.WalkDir(arg, handlerFunc)
		if err == io.EOF {
			fmt.Fprintln(os.Stderr, "directory walk interrupted")
			break
		} else if err != nil {
			fmt.Fprintf(os.Stderr, "error while walking: %v\n", err)
			break
		}
	}
}

func readFileList(scanLn func(a ...any) (n int, err error)) {
	filePath := ""
	for {
		n, err := scanLn(&filePath)
		if n == 0 || err == io.EOF {
			break
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "error while reading stdin: %v\n", err)
			break
		}
		pathQueueG <- filePath
	}
}
