package main

import (
	"crypto/rand"
	"fmt"
	"io"
	"strings"
)

// clearLine erases the progress bar before a log line is printed over it.
type clearLine struct{ w io.Writer }

func (c clearLine) Write(p []byte) (int, error) {
	if _, err := io.WriteString(c.w, "\r\033[K"); err != nil {
		return 0, err
	}
	return c.w.Write(p)
}

func newPeerID() ([20]byte, error) {
	var id [20]byte
	copy(id[:], "-MB0001-") // Azureus-style client prefix
	_, err := rand.Read(id[8:])
	return id, err
}

func bar(done, total, width int) string {
	filled := done * width / total
	return "[" + strings.Repeat("█", filled) + strings.Repeat("░", width-filled) + "]"
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
