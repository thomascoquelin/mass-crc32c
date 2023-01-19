package main

import (
	"encoding/base64"
	"encoding/binary"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

var wg sync.WaitGroup
var pathQueue chan string

var readSize int
var crc32cTable *crc32.Table

func printErr(path string, err error) {
	fmt.Fprintf(os.Stderr, "error: '%s': %v\n", path, err)
}

func CRCReader(reader io.Reader) (string, error) {
	checksum := crc32.Checksum([]byte(""), crc32cTable)
	buf := make([]byte, 1024*readSize)
	for {
		switch n, err := reader.Read(buf); err {
		case nil:
			checksum = crc32.Update(checksum, crc32cTable, buf[:n])
		case io.EOF:
			b := make([]byte, 4)
			binary.BigEndian.PutUint32(b, checksum)
			str := base64.StdEncoding.EncodeToString(b)
			return str, nil
		default:
			return "", err
		}
	}
}

func fileHandler() error {
	wg.Add(1)
	defer wg.Done()
	for path := range pathQueue { // consume the messages in the queue
		file, err := os.Open(path)
		defer func(file *os.File) {
			err := file.Close()
			if err != nil {
				printErr(path, err)
			}
		}(file)
		if err != nil {
			printErr(path, err)
			continue
		}
		crc, err := CRCReader(file)
		if err != nil {
			printErr(path, err)
			continue
		}
		fmt.Printf("%s %s\n", crc, path)
	}
	return nil
}

func walkHandler(path string, info os.FileInfo, err error) error {
	if err != nil {
		nodeType := "file"
		if info.IsDir() {
			nodeType = "dir"
		}
		fmt.Fprintf(os.Stderr, "%s error: '%s': %v\n", nodeType, path, err)
		return nil
	}
	if info.IsDir() {
		fmt.Fprintf(os.Stderr, "entering dir: %s\n", path)
		return nil
	}
	if !info.Mode().IsRegular() {
		fmt.Fprintf(os.Stderr, "ignoring: %s\n", path)
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
		go fileHandler()
	}
	for _, arg := range flag.Args() {
		err := filepath.Walk(arg, walkHandler)
		if err != nil {
			log.Fatal(err)
		}
	}
	close(pathQueue)
	wg.Wait()
}
