package bencode

import (
	"bufio"
	"strings"
	"testing"
)

func TestDecode(t *testing.T) {
	tests := []struct {
		input string
		want  interface{}
	}{
		{"4:spam", "spam"},
		{"i42e", int64(42)},
		{"i-3e", int64(-3)},
	}

	for _, tt := range tests {
		r := bufio.NewReader(strings.NewReader(tt.input))
		got, err := Decode(r)
		if err != nil {
			t.Fatalf("Decode(%q) error: %v", tt.input, err)
		}
		if got != tt.want {
			t.Errorf("Decode(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestDecodeList(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("l4:spami42ee"))
	got, err := Decode(r)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	list, ok := got.([]interface{})
	if !ok || len(list) != 2 {
		t.Fatalf("got %v, want a 2-item list", got)
	}
	t.Logf("Decoded list: %v", list)
}

func TestDecodeDict(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("d3:cow3:mooe"))
	got, err := Decode(r)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	dict, ok := got.(map[string]interface{})
	if !ok || dict["cow"] != "moo" {
		t.Fatalf("got %v, want map[cow:moo]", got)
	}
}

func TestDecodeRejectsMalformed(t *testing.T) {
	for _, in := range []string{"ie", "i03e", "i-0e", "-1:a", "5:abc", "x", "l4:spam"} {
		r := bufio.NewReader(strings.NewReader(in))
		if _, err := Decode(r); err == nil {
			t.Errorf("Decode(%q): expected an error", in)
		}
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	in := map[string]interface{}{
		"b":   []interface{}{"x", int64(-7)},
		"a":   "hello",
		"zzz": map[string]interface{}{"n": int64(0)},
		"bin": string([]byte{0, 255, 'e', ':'}),
	}
	enc, err := Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := Decode(bufio.NewReader(strings.NewReader(string(enc))))
	if err != nil {
		t.Fatal(err)
	}
	again, _ := Encode(out)
	if string(again) != string(enc) {
		t.Errorf("round trip changed bytes:\n%q\n%q", enc, again)
	}
}
