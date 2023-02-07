package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"sync/atomic"
)

type FileInput struct {
	mc *MassCRC32C
}

func (fi *FileInput) walkHandler(path string, dir fs.DirEntry, err error) error {
	if fi.mc.Interrupted {
		return io.EOF
	}
	if err != nil {
		if dir.IsDir() {
			fmt.Fprintf(fi.mc.ErrOut, "dir error: '%s': %v\n", path, err)
			atomic.AddUint64(&fi.mc.directoryErrorCount, 1)
		} else {
			fmt.Fprintf(fi.mc.ErrOut, "file error: '%s': %v\n", path, err)
			atomic.AddUint64(&fi.mc.fileErrorCount, 1)
		}
		return nil
	}
	if dir.IsDir() {
		fmt.Fprintf(fi.mc.DebugOut, "entering dir: %s\n", path)
		return nil
	}
	if !dir.Type().IsRegular() {
		fmt.Fprintf(fi.mc.DebugOut, "ignoring: %s\n", path)
		atomic.AddUint64(&fi.mc.ignoredFilesCount, 1)
		return nil
	}
	fi.mc.PathQueueG <- path // add a path message to the queue (blocking when queue is full)
	return nil
}

func (fi *FileInput) WalkDirectories() {
	for _, arg := range flag.Args() {
		err := filepath.WalkDir(arg, fi.walkHandler)
		if err == io.EOF {
			fmt.Fprintln(fi.mc.DebugOut, "directory walk interrupted")
			break
		} else if err != nil {
			fmt.Fprintf(fi.mc.ErrOut, "error while walking: %v\n", err)
			break
		}
	}
}

func (fi *FileInput) ReadFileList() {
	lineScanner := bufio.NewScanner(fi.mc.stdin)
	for lineScanner.Scan() {
		if fi.mc.Interrupted {
			fmt.Fprintln(fi.mc.DebugOut, "directory walk interrupted")
			break
		}
		fi.mc.PathQueueG <- lineScanner.Text()
		if err := lineScanner.Err(); err != nil {
			fmt.Fprintf(fi.mc.ErrOut, "error while reading stdin: %v\n", err)
			break
		}
	}
}
