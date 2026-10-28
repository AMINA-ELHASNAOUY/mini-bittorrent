package peer

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"

	"mini-bittorrent/bencode"
)

// MsgExtended carries extension-protocol messages (BEP 10). The first payload
// byte is the extension's message ID (0 = the extension handshake), followed by
// a bencoded dictionary and, for some extensions, raw trailing bytes.
const MsgExtended messageID = 20

// FormatExtended builds an extended message.
func FormatExtended(extID byte, dict map[string]interface{}, trailing []byte) (*Message, error) {
	body, err := bencode.Encode(dict)
	if err != nil {
		return nil, err
	}
	payload := make([]byte, 0, 1+len(body)+len(trailing))
	payload = append(payload, extID)
	payload = append(payload, body...)
	payload = append(payload, trailing...)
	return &Message{ID: MsgExtended, Payload: payload}, nil
}

// ParseExtended splits an extended message into its extension ID,
// its bencoded dictionary and any bytes that follow the dictionary.
func ParseExtended(m *Message) (byte, map[string]interface{}, []byte, error) {
	if m.ID != MsgExtended || len(m.Payload) < 2 {
		return 0, nil, nil, errors.New("not an extended message")
	}
	br := bufio.NewReader(bytes.NewReader(m.Payload[1:]))
	v, err := bencode.Decode(br)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("extended message: %w", err)
	}
	dict, ok := v.(map[string]interface{})
	if !ok {
		return 0, nil, nil, errors.New("extended message body is not a dictionary")
	}
	trailing, err := io.ReadAll(br)
	if err != nil {
		return 0, nil, nil, err
	}
	return m.Payload[0], dict, trailing, nil
}
