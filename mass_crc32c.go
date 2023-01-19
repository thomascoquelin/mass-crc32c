package main

import (
	"encoding/base64"
	"encoding/binary"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var wg sync.WaitGroup
var pathQueue chan string
var interrupted bool

var readSize int
var crc32cTable *crc32.Table

var fileCount uint64
var fileErrorCount uint64
var directoryErrorCount uint64
var ignoredFilesCount uint64
var totalDataComputed uint64

func printErr(path string, err error) {
	fmt.Fprintf(os.Stderr, "error: '%s': %v\n", path, err)
}

func CRCReader(reader io.Reader) (string, uint64, error) {
	checksum := crc32.Checksum([]byte(""), crc32cTable)
	buf := make([]byte, 1024*readSize)
	fileSize := uint64(0)
	for {
		switch n, err := reader.Read(buf); err {
		case nil:
			checksum = crc32.Update(checksum, crc32cTable, buf[:n])
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

func fileHandler() {
	defer wg.Done()
	for path := range pathQueue { // consume the messages in the queue
		err, fileSize, crc := pathToCRC(path)
		if err != nil {
			printErr(path, err)
			atomic.AddUint64(&fileErrorCount, 1)
			continue
		}
		fmt.Printf("%s %d %s\n", crc, fileSize, path)
		atomic.AddUint64(&fileCount, 1)
		atomic.AddUint64(&totalDataComputed, fileSize)
	}
	return
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

func walkHandler(path string, info os.FileInfo, err error) error {
	if interrupted {
		return io.EOF
	}
	if err != nil {
		if info.IsDir() {
			fmt.Fprintf(os.Stderr, "dir error: '%s': %v\n", path, err)
			atomic.AddUint64(&directoryErrorCount, 1)
		} else {
			fmt.Fprintf(os.Stderr, "file error: '%s': %v\n", path, err)
			atomic.AddUint64(&fileErrorCount, 1)
		}
		return nil
	}
	if info.IsDir() {
		fmt.Fprintf(os.Stderr, "entering dir: %s\n", path)
		return nil
	}
	if !info.Mode().IsRegular() {
		fmt.Fprintf(os.Stderr, "ignoring: %s\n", path)
		atomic.AddUint64(&ignoredFilesCount, 1)
		return nil
	}
	pathQueue <- path // add a path message to the queue (blocking when queue is full)
	return nil
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage of %s: [options] path [path ...]\n\nOptions:\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	p := flag.Int("p", 1, "# of cpu used")
	j := flag.Int("j", 1, "# of parallel reads")
	l := flag.Int("l", 100, "size of list ahead queue")
	s := flag.Int("s", 1, "size of reads in kbytes")
	flag.Usage = printUsage

	flag.Parse()
	if flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "error: missing paths")
		printUsage()
		os.Exit(1)
	}

	runtime.GOMAXPROCS(*p)            // limit number of kernel threads (CPUs used)
	pathQueue = make(chan string, *l) // use a channel with a size to limit the number of list ahead path
	readSize = *s
	crc32cTable = crc32.MakeTable(crc32.Castagnoli)

	// create the coroutines
	for i := 1; i < *j; i++ {
		wg.Add(1)
		go fileHandler()
	}

	// Notify walk to gracefully stop on a CTRL+C via the 'interrupted' flag
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		interrupted = true
	}()

	startTime := time.Now()
	for _, arg := range flag.Args() {
		err := filepath.Walk(arg, walkHandler)
		if err == io.EOF {
			fmt.Fprintln(os.Stderr, "walk interrupted")
			break
		} else if err != nil {
			fmt.Fprintf(os.Stderr, "error while walking: %v\n", err)
			break
		}
	}
	close(pathQueue)
	wg.Wait()
	duration := time.Now().Sub(startTime)
	fmt.Fprintf(
		os.Stderr,
		"Summary:\n"+
			"Files computed: %d\n"+
			"File errors: %d\n"+
			"Folder errors: %d\n"+
			"Ignored files: %d\n"+
			"Computed data: %dB\n"+
			"Duration: %s\n"+
			"Avg file speed: %d/s\n"+
			"Avg data speed: %dkB/s\n",
		fileCount,
		fileErrorCount,
		directoryErrorCount,
		ignoredFilesCount,
		totalDataComputed,
		duration.String(),
		int(float64(fileCount)/duration.Seconds()),
		int(float64(totalDataComputed)/duration.Seconds()),
	)
}
