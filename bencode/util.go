package bencode

import (
	"bufio"
	"io"
)

func ioReadFull(r *bufio.Reader, buf []byte) (int, error) {
	return io.ReadFull(r, buf)
}
