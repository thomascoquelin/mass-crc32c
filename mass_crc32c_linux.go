//go:build linux

package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"
)

func (mc *MassCRC32C) signalToSummary(startTime time.Time) {
	summaryChan := make(chan os.Signal, 1)
	signal.Notify(summaryChan, syscall.SIGUSR1)
	go func() {
		for _ = range summaryChan {
			mc.printSummary(startTime)
		}
	}()
}
