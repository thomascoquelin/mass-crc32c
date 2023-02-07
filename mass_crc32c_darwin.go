//go:build darwin

package main

import (
	"os"
	"os/signal"
	"syscall"
)

func (mc *MassCRC32C) signalToSummary() {
	summaryChan := make(chan os.Signal, 1)
	signal.Notify(summaryChan, syscall.SIGUSR1)
	go func() {
		for _ = range summaryChan {
			mc.PrintSummary()
		}
	}()
}
