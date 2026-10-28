package bencode

import (
	"bufio"
	"fmt"
	"strconv"
)

// maxStringLen caps a single bencoded string so a malicious length prefix
// can't make us allocate gigabytes. .torrent piece lists stay far below this.
const maxStringLen = 64 << 20

// Decode reads a single bencoded value from r and returns it as a Go value.
// Strings -> string, Integers -> int64, Lists -> []interface{}, Dicts -> map[string]interface{}
func Decode(r *bufio.Reader) (interface{}, error) {
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	switch {
	case b == 'i':
		return decodeInt(r)
	case b == 'l':
		return decodeList(r)
	case b == 'd':
		return decodeDict(r)
	case b >= '0' && b <= '9':
		r.UnreadByte()
		return decodeString(r)
	default:
		return nil, fmt.Errorf("unexpected byte: %q", b)
	}
}

func decodeInt(r *bufio.Reader) (int64, error) {
	s, err := r.ReadString('e')
	if err != nil {
		return 0, err
	}
	s = s[:len(s)-1] // strip trailing 'e'
	if s == "" || s == "-0" || (len(s) > 1 && s[0] == '0') || (len(s) > 2 && s[:2] == "-0") {
		return 0, fmt.Errorf("invalid integer %q", s)
	}
	return strconv.ParseInt(s, 10, 64)
}

func decodeString(r *bufio.Reader) (string, error) {
	lenStr, err := r.ReadString(':')
	if err != nil {
		return "", err
	}
	lenStr = lenStr[:len(lenStr)-1] // strip trailing ':'
	length, err := strconv.Atoi(lenStr)
	if err != nil {
		return "", err
	}
	if length < 0 || length > maxStringLen {
		return "", fmt.Errorf("invalid string length %d", length)
	}

	buf := make([]byte, length)
	_, err = ioReadFull(r, buf)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

func decodeList(r *bufio.Reader) ([]interface{}, error) {
	var list []interface{}
	for {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if b == 'e' {
			return list, nil
		}
		r.UnreadByte()
		val, err := Decode(r)
		if err != nil {
			return nil, err
		}
		list = append(list, val)
	}
}

func decodeDict(r *bufio.Reader) (map[string]interface{}, error) {
	dict := make(map[string]interface{})
	for {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if b == 'e' {
			return dict, nil
		}
		r.UnreadByte()
		key, err := decodeString(r)
		if err != nil {
			return nil, err
		}
		val, err := Decode(r)
		if err != nil {
			return nil, err
		}
		dict[key] = val
	}
}
