package fluxum

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"unicode/utf8"
)

// FluxBIN — the schema-driven binary row encoding (SPEC-006 RPC-040..042).
//
// No field names and no per-value type tags: the schema supplies the type
// context, so a row is its column values back-to-back in declaration order.
// All integers are little-endian. 64-bit values surface as int64/uint64 (no
// precision loss); Identity/ConnectionId surface as lowercase hex strings.

// Column pairs a name with its FluxType (as /schema spells it).
type Column struct {
	Name string
	Type string
}

// RowReader is a sequential FluxBIN reader over a row buffer.
type RowReader struct {
	data []byte
	off  int
}

// NewRowReader wraps a row's bytes.
func NewRowReader(data []byte) *RowReader {
	return &RowReader{data: data}
}

// Remaining is the number of unread bytes.
func (r *RowReader) Remaining() int { return len(r.data) - r.off }

func (r *RowReader) need(n int) error {
	if r.Remaining() < n {
		return fmt.Errorf("fluxbin: unexpected end of row: needed %d, have %d", n, r.Remaining())
	}
	return nil
}

func (r *RowReader) take(n int) ([]byte, error) {
	if err := r.need(n); err != nil {
		return nil, err
	}
	out := r.data[r.off : r.off+n]
	r.off += n
	return out, nil
}

// Read reads one value of the given FluxType. The concrete Go type is: bool
// for Bool; int64 for I8..I64/Timestamp; uint64 for U8..U64/EntityId; float64
// for F32/F64; string for Str/Identity/ConnectionId; []byte for Bytes.
func (r *RowReader) Read(fluxType string) (any, error) {
	switch fluxType {
	case "Bool":
		b, err := r.take(1)
		if err != nil {
			return nil, err
		}
		if b[0] > 1 {
			return nil, fmt.Errorf("fluxbin: invalid bool byte 0x%02x", b[0])
		}
		return b[0] == 1, nil
	case "I8":
		b, err := r.take(1)
		if err != nil {
			return nil, err
		}
		return int64(int8(b[0])), nil
	case "U8":
		b, err := r.take(1)
		if err != nil {
			return nil, err
		}
		return uint64(b[0]), nil
	case "I16":
		b, err := r.take(2)
		if err != nil {
			return nil, err
		}
		return int64(int16(binary.LittleEndian.Uint16(b))), nil
	case "U16":
		b, err := r.take(2)
		if err != nil {
			return nil, err
		}
		return uint64(binary.LittleEndian.Uint16(b)), nil
	case "I32":
		b, err := r.take(4)
		if err != nil {
			return nil, err
		}
		return int64(int32(binary.LittleEndian.Uint32(b))), nil
	case "U32":
		b, err := r.take(4)
		if err != nil {
			return nil, err
		}
		return uint64(binary.LittleEndian.Uint32(b)), nil
	case "I64", "Timestamp":
		b, err := r.take(8)
		if err != nil {
			return nil, err
		}
		return int64(binary.LittleEndian.Uint64(b)), nil
	case "U64", "EntityId":
		b, err := r.take(8)
		if err != nil {
			return nil, err
		}
		return binary.LittleEndian.Uint64(b), nil
	case "F32":
		b, err := r.take(4)
		if err != nil {
			return nil, err
		}
		return float64(math.Float32frombits(binary.LittleEndian.Uint32(b))), nil
	case "F64":
		b, err := r.take(8)
		if err != nil {
			return nil, err
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(b)), nil
	case "Str":
		raw, err := r.readLenBytes()
		if err != nil {
			return nil, err
		}
		if !utf8.Valid(raw) {
			return nil, fmt.Errorf("fluxbin: string is not valid UTF-8")
		}
		return string(raw), nil
	case "Bytes":
		raw, err := r.readLenBytes()
		if err != nil {
			return nil, err
		}
		out := make([]byte, len(raw))
		copy(out, raw)
		return out, nil
	case "Identity":
		b, err := r.take(32)
		if err != nil {
			return nil, err
		}
		return hex.EncodeToString(b), nil
	case "ConnectionId":
		b, err := r.take(16)
		if err != nil {
			return nil, err
		}
		return hex.EncodeToString(b), nil
	default:
		return nil, fmt.Errorf("fluxbin: unsupported type %s", fluxType)
	}
}

func (r *RowReader) readLenBytes() ([]byte, error) {
	if err := r.need(4); err != nil {
		return nil, err
	}
	n := int(binary.LittleEndian.Uint32(r.data[r.off:]))
	r.off += 4
	return r.take(n)
}

// DecodeRow decodes one row into a map keyed by column name.
func DecodeRow(data []byte, columns []Column) (map[string]any, error) {
	reader := NewRowReader(data)
	row := make(map[string]any, len(columns))
	for _, col := range columns {
		value, err := reader.Read(col.Type)
		if err != nil {
			return nil, err
		}
		row[col.Name] = value
	}
	if reader.Remaining() != 0 {
		return nil, fmt.Errorf("fluxbin: row has %d trailing byte(s): schema mismatch", reader.Remaining())
	}
	return row, nil
}
