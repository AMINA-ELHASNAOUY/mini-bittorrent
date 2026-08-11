package bencode

import (
	"fmt"
	"sort"
	"strconv"
)

// Encode converts a Go value (string, int64, []interface{}, map[string]interface{})
// back into its bencoded byte representation.
func Encode(v interface{}) ([]byte, error) {
	switch val := v.(type) {
	case string:
		return encodeString(val), nil
	case int64:
		return encodeInt(val), nil
	case int:
		return encodeInt(int64(val)), nil
	case []interface{}:
		return encodeList(val)
	case map[string]interface{}:
		return encodeDict(val)
	default:
		return nil, fmt.Errorf("unsupported type for bencode: %T", v)
	}
}

func encodeString(s string) []byte {
	return []byte(strconv.Itoa(len(s)) + ":" + s)
}

func encodeInt(i int64) []byte {
	return []byte("i" + strconv.FormatInt(i, 10) + "e")
}

func encodeList(list []interface{}) ([]byte, error) {
	out := []byte("l")
	for _, item := range list {
		b, err := Encode(item)
		if err != nil {
			return nil, err
		}
		out = append(out, b...)
	}
	out = append(out, 'e')
	return out, nil
}

func encodeDict(dict map[string]interface{}) ([]byte, error) {
	// Bencode spec requires dictionary keys to be sorted
	keys := make([]string, 0, len(dict))
	for k := range dict {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := []byte("d")
	for _, k := range keys {
		out = append(out, encodeString(k)...)
		b, err := Encode(dict[k])
		if err != nil {
			return nil, err
		}
		out = append(out, b...)
	}
	out = append(out, 'e')
	return out, nil
}
