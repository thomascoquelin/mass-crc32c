package main

import (
	"fmt"
	"io"
	"testing"
)

// Test stdin line reader
type scanLnMsg struct {
	path string
	err  error
}

// implements `io.Reader` interface
type testReader struct {
	scanLnChIn  chan scanLnMsg
	scanLnChOut chan scanLnMsg
	scanLnChErr chan error
}

func (tb *testReader) Read(p []byte) (n int, err error) {
	msg := <-tb.scanLnChIn
	if msg.err != nil {
		return 0, err
	}
	n = copy(p, msg.path)
	return
}

func testBufferSetup(items []scanLnMsg) *testReader {
	tb := testReader{
		scanLnChIn:  make(chan scanLnMsg, 5),
		scanLnChOut: make(chan scanLnMsg, 5),
		scanLnChErr: make(chan error, 5),
	}
	for _, testItem := range items {
		tb.scanLnChIn <- scanLnMsg{
			path: testItem.path + "\n",
			err:  nil,
		}
		tb.scanLnChOut <- testItem
	}
	tb.scanLnChIn <- scanLnMsg{
		path: "",
		err:  io.EOF,
	}
	close(tb.scanLnChIn)
	return &tb
}

func (tb *testReader) testHandler(path string) (err error) {
	msg := <-tb.scanLnChOut
	if msg.err != nil {
		return err
	}
	if msg.path != path {
		err = fmt.Errorf("got %s, expected %s", path, msg.path)
		tb.scanLnChErr <- err
	}
	return err
}

func TestReadFileList(t *testing.T) {
	tb := testBufferSetup([]scanLnMsg{
		{"path1", nil},
		{"path 2", nil},
		{"path3", fmt.Errorf("handled error")}, // should continue despite this error being injected
		{"path/4", nil},
	})
	mc := InitMassCRC32C(1, 1)
	mc.HandlerFunc = tb.testHandler
	mc.stdin = tb
	fi := FileInput{mc: mc}
	mc.Startup(1)
	fi.ReadFileList()
	mc.TearDown()
	if len(tb.scanLnChIn) > 0 {
		t.Errorf("input queue isn't empty: %d remaining", len(tb.scanLnChIn))
	}
	if len(tb.scanLnChOut) > 0 {
		t.Errorf("out queue isn't empty: %d remaining", len(tb.scanLnChOut))
	}
	if len(tb.scanLnChErr) > 0 {
		t.Errorf("%v\n", <-tb.scanLnChErr)
	}
}
