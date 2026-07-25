package fluxum

import (
	"encoding/binary"
	"fmt"
)

// Fluxum message envelopes over the HiveLLM binary wire.
//
// Framing is the family standard: u32 LE length + MessagePack body, with a
// zero-length body as the WIRE-024 keep-alive. On top of it sit Fluxum's own
// pieces — the RPC-011 tagged envelope ([tag, payload]), RowList batches
// (RPC-032), and FluxBIN rows (fluxbin.go).

const frameHeaderLen = 4

// defaultMaxFrameBytes is Fluxum's max_frame_bytes (RPC-061): 16 MB.
const defaultMaxFrameBytes = 16 * 1024 * 1024

// serverMessage is a decoded server envelope: its tag and positional payload.
type serverMessage struct {
	tag     string
	payload []any
}

// encodeFrame frames a message body for the wire.
func encodeFrame(body []byte) []byte {
	out := make([]byte, frameHeaderLen+len(body))
	binary.LittleEndian.PutUint32(out, uint32(len(body)))
	copy(out[frameHeaderLen:], body)
	return out
}

// encodeMessage encodes [tag, payload] and frames it (RPC-011). The payload is
// a positional array: field ORDER is the wire format.
func encodeMessage(tag string, payload []any) ([]byte, error) {
	body, err := msgpackEncode([]any{tag, payload})
	if err != nil {
		return nil, err
	}
	return encodeFrame(body), nil
}

// decodeMessage decodes one envelope body into a serverMessage.
func decodeMessage(body []byte) (serverMessage, error) {
	value, err := msgpackDecode(body)
	if err != nil {
		return serverMessage{}, err
	}
	arr, ok := value.([]any)
	if !ok || len(arr) != 2 {
		return serverMessage{}, fmt.Errorf("protocol: envelope is not a [tag, payload] pair")
	}
	tag, ok := arr[0].(string)
	if !ok {
		return serverMessage{}, fmt.Errorf("protocol: envelope tag is not a string")
	}
	payload, ok := arr[1].([]any)
	if !ok {
		payload = []any{arr[1]}
	}
	return serverMessage{tag: tag, payload: payload}, nil
}

// frameReader accumulates transport bytes and yields complete message bodies,
// skipping keep-alive (zero-length) frames.
type frameReader struct {
	buf []byte
	max int
}

func newFrameReader() *frameReader {
	return &frameReader{max: defaultMaxFrameBytes}
}

func (f *frameReader) push(chunk []byte) {
	f.buf = append(f.buf, chunk...)
}

// nextBody returns the next complete body, or (nil, false) when more bytes are
// needed. Keep-alives are skipped.
func (f *frameReader) nextBody() ([]byte, bool, error) {
	for {
		if len(f.buf) < frameHeaderLen {
			return nil, false, nil
		}
		length := int(binary.LittleEndian.Uint32(f.buf))
		if length > f.max {
			return nil, false, fmt.Errorf("protocol: frame of %d bytes exceeds the %d-byte cap", length, f.max)
		}
		end := frameHeaderLen + length
		if len(f.buf) < end {
			return nil, false, nil
		}
		body := make([]byte, length)
		copy(body, f.buf[frameHeaderLen:end])
		f.buf = f.buf[end:]
		if length == 0 {
			continue // keep-alive
		}
		return body, true, nil
	}
}

// sliceRowList slices a flat RowList into its rows (RPC-032). Wire shape:
// [row_count, size_hint, rows_data], where size_hint is ["Fixed", n] or
// ["Offsets", [start, ...]].
func sliceRowList(value any) ([][]byte, error) {
	arr, ok := value.([]any)
	if !ok || len(arr) < 3 {
		return nil, fmt.Errorf("protocol: RowList is not a 3-field structure")
	}
	count := toInt(arr[0])
	data, ok := arr[2].([]byte)
	if !ok {
		return nil, fmt.Errorf("protocol: RowList.rows_data is not binary")
	}
	hint, ok := arr[1].([]any)
	if !ok || len(hint) == 0 {
		return nil, fmt.Errorf("protocol: RowList.size_hint is not tagged")
	}
	kind, _ := hint[0].(string)

	rows := make([][]byte, 0, count)
	switch kind {
	case "Fixed":
		size := toInt(hint[1])
		if size <= 0 {
			if count != 0 {
				return nil, fmt.Errorf("protocol: Fixed size_hint of 0 with rows present")
			}
			return rows, nil
		}
		if len(data) != count*size {
			return nil, fmt.Errorf("protocol: inconsistent RowList: %d rows x %d != %d", count, size, len(data))
		}
		for i := 0; i < count; i++ {
			rows = append(rows, data[i*size:(i+1)*size])
		}
		return rows, nil
	case "Offsets":
		offs, ok := hint[1].([]any)
		if !ok || len(offs) != count {
			return nil, fmt.Errorf("protocol: inconsistent RowList: offsets length != row_count")
		}
		for i := 0; i < count; i++ {
			start := toInt(offs[i])
			end := len(data)
			if i+1 < count {
				end = toInt(offs[i+1])
			}
			if start > end || end > len(data) {
				return nil, fmt.Errorf("protocol: inconsistent RowList: offset out of range")
			}
			rows = append(rows, data[start:end])
		}
		return rows, nil
	default:
		return nil, fmt.Errorf("protocol: unknown RowList size_hint %q", kind)
	}
}

// tableUpdate decodes one TableUpdate payload entry into its (query_id, table
// name, insert rows, delete rows). Layout: [table_id, table_name, query_id,
// inserts(RowList), deletes(RowList)].
func tableUpdate(entry any) (queryID int, table string, inserts, deletes [][]byte, err error) {
	arr, ok := entry.([]any)
	if !ok || len(arr) < 5 {
		return 0, "", nil, nil, fmt.Errorf("protocol: TableUpdate is not a 5-field structure")
	}
	queryID = toInt(arr[2])
	table, _ = arr[1].(string)
	if inserts, err = sliceRowList(arr[3]); err != nil {
		return
	}
	deletes, err = sliceRowList(arr[4])
	return
}

// toInt coerces a decoded numeric (int64 or uint64) to int.
func toInt(v any) int {
	switch n := v.(type) {
	case int64:
		return int(n)
	case uint64:
		return int(n)
	case int:
		return n
	default:
		return 0
	}
}
