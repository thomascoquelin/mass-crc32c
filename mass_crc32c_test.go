package main

import (
	"fmt"
	"io"
	"math"
	"os"
	"testing"
)

// This replaces the default test Main
func TestMain(m *testing.M) {
	// set global variables
	setupWorkers(1, 1, 5, testHandler)

	// run tests and report final status
	code := m.Run()
	os.Exit(code)
}

// implements `io.Reader` interface
type dummyFileReader struct {
	payload   string
	fp        *int
	readCount *int
}

func (dfr dummyFileReader) Read(p []byte) (n int, err error) {
	if *dfr.fp == len(dfr.payload) {
		return 0, io.EOF
	}
	n = copy(p, dfr.payload[*dfr.fp:])
	*dfr.fp = *dfr.fp + n
	*dfr.readCount = *dfr.readCount + 1
	return
}

func makeDummyFileReader(payload string) dummyFileReader {
	fp := 0
	readCount := 0

	return dummyFileReader{
		payload:   payload,
		fp:        &fp,
		readCount: &readCount,
	}
}

func TestCRCReader(t *testing.T) {
	tests := []struct {
		name    string
		crc32c  string
		payload string
	}{
		{"short", "4AmyZA==", "short test data"},
		{"long", "pSk/Tg==", `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Aliquam ut fermentum eros. Aenean mattis
accumsan ante nec auctor. Vivamus finibus congue risus, id scelerisque massa fermentum quis. Praesent purus tortor,
rhoncus quis rhoncus in, posuere in eros. Duis ac congue nunc, non efficitur dolor. Morbi at mauris sed erat
consectetur blandit vitae vel eros. Curabitur sagittis convallis scelerisque. Cras tempor scelerisque velit in
fringilla. Suspendisse potenti. Quisque nec dictum nunc. Sed urna felis, fermentum quis quam ac, lacinia pharetra ex.
Ut velit arcu, ornare at tortor et, pretium aliquet enim. Integer ullamcorper malesuada leo eget blandit.
Suspendisse lobortis auctor justo, sed rhoncus orci bibendum eget. Ut id sapien venenatis, tempus lectus non, tincidunt
sem.\nQuisque blandit velit est, eu hendrerit tellus tincidunt in. Donec vitae malesuada diam. Class aptent taciti
sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Suspendisse potenti. Pellentesque eget dictum
lectus. Etiam sit amet urna eu metus lacinia ornare. Nulla eget elit ultrices, ultricies nunc quis, congue nunc.
Fusce suscipit aliquam magna, eu vehicula tortor eleifend ut. Ut eu dui quis arcu molestie facilisis vel at ante.
Quisque bibendum molestie posuere. Morbi et augue ut magna porttitor bibendum id in massa. Fusce quis elit ligula.
Quisque massa ante, ultrices vitae tellus quis, lacinia ullamcorper quam. Mauris eget orci libero. Morbi ut lacinia
nulla, sit amet semper lorem. Nullam dictum sapien nec mi condimentum accumsan.\nNulla quis sapien ac tortor
pellentesque molestie. Etiam blandit tincidunt quam eget venenatis. Vivamus in bibendum dui. Nam semper risus dolor,
sed interdum metus maximus ac. Aenean eget elementum tortor. Vestibulum tristique diam justo, sit amet elementum justo
elementum suscipit. Nunc nisi lectus, bibendum eget nulla sit amet, pharetra tristique nisl. Aliquam erat volutpat.
Maecenas sed velit eu nulla luctus gravida ac vel nunc. Etiam ullamcorper ornare leo sit amet lobortis. Aenean
consectetur lacus ut erat mollis, sit amet vulputate lectus iaculis.\nVivamus non sollicitudin odio. In non nisi ut
tellus blandit porttitor in at ex. In dapibus molestie ultrices. Suspendisse a efficitur urna. Aliquam convallis,
mauris bibendum varius elementum, nunc libero elementum lectus, sed vulputate massa lectus id odio. Phasellus ut nisl
risus. Vestibulum finibus, nunc ut sodales fringilla, nibh augue posuere nibh, ut iaculis justo lacus finibus leo.
Morbi vulputate erat a velit volutpat volutpat. Aliquam et consectetur urna, ullamcorper imperdiet ex. Ut in leo eu
mauris bibendum rhoncus. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia curae; Cras
tempor diam ligula, sit amet rutrum orci facilisis eget. Maecenas sodales blandit enim quis hendrerit.\nMorbi molestie
mauris id nunc finibus, a ornare eros semper. Sed euismod finibus ante ut laoreet. Aliquam malesuada tellus non dui
placerat, eget volutpat neque scelerisque. Donec porttitor, ante a euismod viverra, sem elit aliquam ex, tempus cursus
arcu nisi vel nisl. Donec posuere convallis semper. Cras quis neque purus. Nulla mattis dictum rutrum. Nunc diam purus,
fermentum sed sapien sed, aliquet rhoncus dolor. Aenean velit enim, porttitor non quam in, cursus efficitur quam. Donec
sagittis nulla sit amet commodo fermentum. Curabitur at egestas magna. Praesent euismod velit quis lectus luctus, nec
fringilla diam maximus. Etiam porttitor tortor id ligula feugiat, in sodales sapien auctor.\n`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := makeDummyFileReader(
				test.payload,
			)

			crc, dataLen, err := CRCReader(data)
			if err != nil {
				t.Errorf("got unexpected error %v", err)
			}
			if crc != test.crc32c {
				t.Errorf("crc32c value error, got %s, expected %s", crc, test.crc32c)
			}
			goodLen := uint64(len(test.payload))
			if dataLen != goodLen {
				t.Errorf("len error, got %d, expected %d", dataLen, goodLen)
			}
			goodReadCount := int(math.Ceil(float64(goodLen) / float64(readSizeG*1024)))
			if *data.readCount != goodReadCount {
				t.Errorf("readCount error, got %d, expected %d\n", *data.readCount, goodReadCount)
			}
		},
		)
	}
}

func TestPathToCRC(t *testing.T) {
	path := "test_data.txt"
	err, fileSize, crc := pathToCRC(path)
	if err != nil {
		t.Errorf("got unexpected error %v", err)
	}
	goodCRC32C := "WaIfQg=="
	if crc != goodCRC32C {
		t.Errorf("crc32c value error, got %s, expected %s", crc, goodCRC32C)
	}
	goodLen := uint64(3538)
	if fileSize != goodLen {
		t.Errorf("len error, got %d, expected %d", fileSize, goodLen)
	}
}

type scanLnMsg struct {
	path string
	err  error
}

var scanLnChIn chan scanLnMsg
var scanLnChOut chan scanLnMsg
var scanLnChErr chan error

func scanLn(a ...any) (n int, err error) {
	msg := <-scanLnChIn
	if msg.err != nil {
		return 0, err
	}
	s, ok := a[0].(*string)
	if !ok {
		return 0, fmt.Errorf("scanLn: failed to convert %v to (*string)", a[0])
	}
	n = len(msg.path)
	*s = msg.path
	return
}

func scanLnSetup(items []scanLnMsg) {
	scanLnChIn = make(chan scanLnMsg, 5)
	scanLnChOut = make(chan scanLnMsg, 5)
	scanLnChErr = make(chan error, 5)
	for _, testItem := range items {
		scanLnChIn <- scanLnMsg{
			path: testItem.path,
			err:  nil,
		}
		scanLnChOut <- testItem
	}
	scanLnChIn <- scanLnMsg{
		path: "",
		err:  io.EOF,
	}
}

func testHandler(path string) (err error) {
	msg := <-scanLnChOut
	if msg.err != nil {
		return err
	}
	if msg.path != path {
		err = fmt.Errorf("got %s, expected %s", path, msg.path)
		scanLnChErr <- err
	}
	return err
}

func TestReadFileList(t *testing.T) {
	scanLnSetup([]scanLnMsg{
		{"path1", nil},
		{"path2", nil},
		{"path3", fmt.Errorf("handled error")}, // should continue despite this error being injected
		{"path4", nil},
	})
	readFileList(scanLn)
	tearDown()
	if len(scanLnChIn) > 0 {
		t.Errorf("input queue isn't empty: %d remaining", len(scanLnChIn))
	}
	if len(scanLnChOut) > 0 {
		t.Errorf("out queue isn't empty: %d remaining", len(scanLnChOut))
	}
	if len(scanLnChErr) > 0 {
		t.Errorf("%v\n", <-scanLnChErr)
	}
}