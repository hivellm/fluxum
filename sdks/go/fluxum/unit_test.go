package fluxum

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestMsgpackRoundtrips(t *testing.T) {
	cases := []any{
		nil, true, false,
		int64(0), int64(127), int64(-1), int64(-32), int64(255), int64(65535),
		int64(4294967295), uint64(1) << 63, ^uint64(0),
		int64(-128), int64(-32768), int64(-2147483648),
		3.5, "hello", string(bytes.Repeat([]byte("u"), 40)),
		[]byte{0, 1, 2},
		[]any{int64(1), "two", []any{int64(3), int64(4)}},
	}
	for _, in := range cases {
		enc, err := msgpackEncode(in)
		if err != nil {
			t.Fatalf("encode %v: %v", in, err)
		}
		out, err := msgpackDecode(enc)
		if err != nil {
			t.Fatalf("decode %v: %v", in, err)
		}
		if !deepEqual(in, out) {
			t.Fatalf("roundtrip %#v != %#v", in, out)
		}
	}
}

func TestMsgpackRejectsTrailingBytes(t *testing.T) {
	enc, _ := msgpackEncode(int64(1))
	if _, err := msgpackDecode(append(enc, 0x02)); err == nil {
		t.Fatal("expected a trailing-bytes error")
	}
}

func TestFluxbinReadsEachType(t *testing.T) {
	var buf []byte
	var negFive int32 = -5
	buf = binary.LittleEndian.AppendUint64(buf, 1) // id: U64
	buf = append(buf, 0x01)                        // done: Bool
	buf = binary.LittleEndian.AppendUint32(buf, uint32(negFive))
	buf = binary.LittleEndian.AppendUint32(buf, 2) // Str length
	buf = append(buf, "hi"...)
	row, err := DecodeRow(buf, []Column{
		{"id", "U64"}, {"done", "Bool"}, {"x", "I32"}, {"name", "Str"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if row["id"] != uint64(1) || row["done"] != true || row["x"] != int64(-5) || row["name"] != "hi" {
		t.Fatalf("decoded wrong: %#v", row)
	}
}

func TestFluxbinIdentityHexAndTrailingBytes(t *testing.T) {
	ident := make([]byte, 32)
	for i := range ident {
		ident[i] = byte(i)
	}
	v, err := NewRowReader(ident).Read("Identity")
	if err != nil || v != "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f" {
		t.Fatalf("identity hex wrong: %v %v", v, err)
	}
	if _, err := DecodeRow([]byte{0x01, 0x02}, []Column{{"b", "Bool"}}); err == nil {
		t.Fatal("expected a trailing-byte error")
	}
}

func TestFrameReaderSkipsKeepalivesAndSplitsFrames(t *testing.T) {
	bodyA, _ := msgpackEncode([]any{"A", []any{int64(1)}})
	bodyB, _ := msgpackEncode([]any{"B", []any{int64(2)}})
	keepalive := []byte{0, 0, 0, 0}
	stream := append(append(encodeFrame(bodyA), keepalive...), encodeFrame(bodyB)...)

	r := newFrameReader()
	r.push(stream[:3])
	if _, ok, _ := r.nextBody(); ok {
		t.Fatal("header not yet complete")
	}
	r.push(stream[3:])
	got, ok, _ := r.nextBody()
	if !ok || !bytes.Equal(got, bodyA) {
		t.Fatalf("first body wrong")
	}
	got, ok, _ = r.nextBody()
	if !ok || !bytes.Equal(got, bodyB) {
		t.Fatalf("keep-alive should have been skipped")
	}
	if _, ok, _ := r.nextBody(); ok {
		t.Fatal("expected no more bodies")
	}
}

func TestEnvelopeEncodeDecodeIsPositional(t *testing.T) {
	frame, err := encodeMessage("ReducerCall", []any{int64(7), "add_task", nil, []any{"ship it"}, nil})
	if err != nil {
		t.Fatal(err)
	}
	msg, err := decodeMessage(frame[4:])
	if err != nil {
		t.Fatal(err)
	}
	if msg.tag != "ReducerCall" || toInt(msg.payload[0]) != 7 || msg.payload[1] != "add_task" {
		t.Fatalf("positional decode wrong: %#v", msg)
	}
}

func TestRowListSlicesFixedAndOffsets(t *testing.T) {
	var data []byte
	data = binary.LittleEndian.AppendUint64(data, 1)
	data = binary.LittleEndian.AppendUint64(data, 2)
	rows, err := sliceRowList([]any{int64(2), []any{"Fixed", int64(8)}, data})
	if err != nil || len(rows) != 2 || !bytes.Equal(rows[0], data[:8]) {
		t.Fatalf("fixed slice wrong: %v %v", rows, err)
	}
	packed := []byte("abcde")
	rows, err = sliceRowList([]any{int64(2), []any{"Offsets", []any{int64(0), int64(2)}}, packed})
	if err != nil || string(rows[0]) != "ab" || string(rows[1]) != "cde" {
		t.Fatalf("offsets slice wrong: %v %v", rows, err)
	}
	if _, err := sliceRowList([]any{int64(2), []any{"Fixed", int64(8)}, []byte{0}}); err == nil {
		t.Fatal("expected an inconsistent-layout error")
	}
}

// deepEqual is a tiny structural comparison sufficient for the roundtrip test.
func deepEqual(a, b any) bool {
	switch av := a.(type) {
	case []byte:
		bv, ok := b.([]byte)
		return ok && bytes.Equal(av, bv)
	case []any:
		bv, ok := b.([]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !deepEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	default:
		return a == b
	}
}
