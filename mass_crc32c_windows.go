//go:build windows

package main

import (
	"time"
)

func (mc *MassCRC32C) signalToSummary(startTime time.Time) {
	//No signal on windows
}
