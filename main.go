package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"os"
	"runtime"
)

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

	mc := InitMassCRC32C(*readSizeP, *listQueueLength)
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
					fmt.Fprintf(mc.DebugOut, "Error: failed to flush gzip stream: %v", err)
				}
				err = gzWriter.Close()
				if err != nil {
					fmt.Fprintf(mc.DebugOut, "Error: failed to close gzip stream: %v", err)
				}
			}(gzWriter)
			mc.StdOut = gzWriter
		} else {
			mc.StdOut = f
		}
	}
	if *outErr != "" {
		f, err := os.OpenFile(*outErr, os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			os.Exit(2)
		}
		defer f.Close()
		if *compress {
			gzWriter := gzip.NewWriter(f)
			defer func(gzWriter *gzip.Writer) {
				err := gzWriter.Flush()
				if err != nil {
					fmt.Fprintf(mc.DebugOut, "Error: failed to flush gzip stream: %v", err)
				}
				err = gzWriter.Close()
				if err != nil {
					fmt.Fprintf(mc.DebugOut, "Error: failed to close gzip stream: %v", err)
				}
			}(gzWriter)
			mc.ErrOut = gzWriter
		} else {
			mc.ErrOut = f
		}
	}
	mc.Startup(*jobCountP)
	fi := FileInput{mc: mc}

	if flag.NArg() == 0 {
		fi.ReadFileList()
	} else {
		fi.WalkDirectories()
	}
	mc.TearDown()
	mc.PrintSummary()
}
