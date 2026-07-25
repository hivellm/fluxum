// Package fluxum — a minimal MessagePack codec, just the subset the Fluxum
// wire uses.
//
// The SDK ships no third-party dependencies (SDK-080), so rather than pull in
// an external MessagePack module it carries the small codec it needs: the
// [tag, payload] envelopes (RPC-011) and the values inside them (nil, bool,
// ints of every width, float, str, bin, array, map). Framing and FluxBIN rows
// live elsewhere (protocol.go, fluxbin.go).
package fluxum

import (
	"encoding/binary"
	"fmt"
	"math"
)

// msgpackEncode encodes a value to MessagePack bytes. Supported inputs: nil,
// bool, int/int64/uint64, float64, string, []byte, []any, map[string]any.
func msgpackEncode(value any) ([]byte, error) {
	var out []byte
	out, err := encodeValue(out, value)
	return out, err
}

func encodeValue(out []byte, value any) ([]byte, error) {
	switch v := value.(type) {
	case nil:
		return append(out, 0xC0), nil
	case bool:
		if v {
			return append(out, 0xC3), nil
		}
		return append(out, 0xC2), nil
	case int:
		return encodeInt(out, int64(v)), nil
	case int8:
		return encodeInt(out, int64(v)), nil
	case int16:
		return encodeInt(out, int64(v)), nil
	case int32:
		return encodeInt(out, int64(v)), nil
	case int64:
		return encodeInt(out, v), nil
	case uint:
		return encodeUint(out, uint64(v)), nil
	case uint8:
		return encodeUint(out, uint64(v)), nil
	case uint16:
		return encodeUint(out, uint64(v)), nil
	case uint32:
		return encodeUint(out, uint64(v)), nil
	case uint64:
		return encodeUint(out, v), nil
	case float32:
		out = append(out, 0xCB)
		return binary.BigEndian.AppendUint64(out, math.Float64bits(float64(v))), nil
	case float64:
		out = append(out, 0xCB)
		return binary.BigEndian.AppendUint64(out, math.Float64bits(v)), nil
	case string:
		return encodeStr(out, v), nil
	case []byte:
		return encodeBin(out, v), nil
	case []any:
		return encodeArray(out, v)
	case map[string]any:
		return encodeMap(out, v)
	default:
		return nil, fmt.Errorf("msgpack: cannot encode %T", value)
	}
}

func encodeInt(out []byte, n int64) []byte {
	if n >= 0 {
		return encodeUint(out, uint64(n))
	}
	switch {
	case n >= -32:
		return append(out, byte(n))
	case n >= math.MinInt8:
		return append(out, 0xD0, byte(int8(n)))
	case n >= math.MinInt16:
		out = append(out, 0xD1)
		return binary.BigEndian.AppendUint16(out, uint16(int16(n)))
	case n >= math.MinInt32:
		out = append(out, 0xD2)
		return binary.BigEndian.AppendUint32(out, uint32(int32(n)))
	default:
		out = append(out, 0xD3)
		return binary.BigEndian.AppendUint64(out, uint64(n))
	}
}

func encodeUint(out []byte, n uint64) []byte {
	switch {
	case n <= 0x7F:
		return append(out, byte(n))
	case n <= 0xFF:
		return append(out, 0xCC, byte(n))
	case n <= 0xFFFF:
		out = append(out, 0xCD)
		return binary.BigEndian.AppendUint16(out, uint16(n))
	case n <= 0xFFFFFFFF:
		out = append(out, 0xCE)
		return binary.BigEndian.AppendUint32(out, uint32(n))
	default:
		out = append(out, 0xCF)
		return binary.BigEndian.AppendUint64(out, n)
	}
}

func encodeStr(out []byte, s string) []byte {
	n := len(s)
	switch {
	case n <= 31:
		out = append(out, 0xA0|byte(n))
	case n <= 0xFF:
		out = append(out, 0xD9, byte(n))
	case n <= 0xFFFF:
		out = append(out, 0xDA)
		out = binary.BigEndian.AppendUint16(out, uint16(n))
	default:
		out = append(out, 0xDB)
		out = binary.BigEndian.AppendUint32(out, uint32(n))
	}
	return append(out, s...)
}

func encodeBin(out, data []byte) []byte {
	n := len(data)
	switch {
	case n <= 0xFF:
		out = append(out, 0xC4, byte(n))
	case n <= 0xFFFF:
		out = append(out, 0xC5)
		out = binary.BigEndian.AppendUint16(out, uint16(n))
	default:
		out = append(out, 0xC6)
		out = binary.BigEndian.AppendUint32(out, uint32(n))
	}
	return append(out, data...)
}

func encodeArray(out []byte, items []any) ([]byte, error) {
	n := len(items)
	switch {
	case n <= 15:
		out = append(out, 0x90|byte(n))
	case n <= 0xFFFF:
		out = append(out, 0xDC)
		out = binary.BigEndian.AppendUint16(out, uint16(n))
	default:
		out = append(out, 0xDD)
		out = binary.BigEndian.AppendUint32(out, uint32(n))
	}
	var err error
	for _, item := range items {
		if out, err = encodeValue(out, item); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func encodeMap(out []byte, m map[string]any) ([]byte, error) {
	n := len(m)
	switch {
	case n <= 15:
		out = append(out, 0x80|byte(n))
	default:
		out = append(out, 0xDE)
		out = binary.BigEndian.AppendUint16(out, uint16(n))
	}
	var err error
	for k, val := range m {
		out = encodeStr(out, k)
		if out, err = encodeValue(out, val); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// msgpackDecode decodes a single MessagePack value; the whole buffer must be
// consumed. Ints surface as int64 (or uint64 when above the int64 range),
// binary as []byte, strings as string, arrays as []any, maps as
// map[string]any.
func msgpackDecode(data []byte) (any, error) {
	value, off, err := decodeAt(data, 0)
	if err != nil {
		return nil, err
	}
	if off != len(data) {
		return nil, fmt.Errorf("msgpack: %d trailing byte(s)", len(data)-off)
	}
	return value, nil
}

func decodeAt(data []byte, off int) (any, int, error) {
	if off >= len(data) {
		return nil, 0, fmt.Errorf("msgpack: unexpected end of buffer")
	}
	b := data[off]
	off++

	switch {
	case b <= 0x7F:
		return int64(b), off, nil
	case b >= 0xE0:
		return int64(int8(b)), off, nil
	case b >= 0x80 && b <= 0x8F:
		return decodeMap(data, off, int(b&0x0F))
	case b >= 0x90 && b <= 0x9F:
		return decodeArray(data, off, int(b&0x0F))
	case b >= 0xA0 && b <= 0xBF:
		return decodeStr(data, off, int(b&0x1F))
	}

	switch b {
	case 0xC0:
		return nil, off, nil
	case 0xC2:
		return false, off, nil
	case 0xC3:
		return true, off, nil
	case 0xC4:
		return decodeBin(data, off+1, int(data[off]))
	case 0xC5:
		return decodeBin(data, off+2, int(binary.BigEndian.Uint16(data[off:])))
	case 0xC6:
		return decodeBin(data, off+4, int(binary.BigEndian.Uint32(data[off:])))
	case 0xCA:
		return float64(math.Float32frombits(binary.BigEndian.Uint32(data[off:]))), off + 4, nil
	case 0xCB:
		return math.Float64frombits(binary.BigEndian.Uint64(data[off:])), off + 8, nil
	case 0xCC:
		return int64(data[off]), off + 1, nil
	case 0xCD:
		return int64(binary.BigEndian.Uint16(data[off:])), off + 2, nil
	case 0xCE:
		return int64(binary.BigEndian.Uint32(data[off:])), off + 4, nil
	case 0xCF:
		return binary.BigEndian.Uint64(data[off:]), off + 8, nil
	case 0xD0:
		return int64(int8(data[off])), off + 1, nil
	case 0xD1:
		return int64(int16(binary.BigEndian.Uint16(data[off:]))), off + 2, nil
	case 0xD2:
		return int64(int32(binary.BigEndian.Uint32(data[off:]))), off + 4, nil
	case 0xD3:
		return int64(binary.BigEndian.Uint64(data[off:])), off + 8, nil
	case 0xD9:
		return decodeStr(data, off+1, int(data[off]))
	case 0xDA:
		return decodeStr(data, off+2, int(binary.BigEndian.Uint16(data[off:])))
	case 0xDB:
		return decodeStr(data, off+4, int(binary.BigEndian.Uint32(data[off:])))
	case 0xDC:
		return decodeArray(data, off+2, int(binary.BigEndian.Uint16(data[off:])))
	case 0xDD:
		return decodeArray(data, off+4, int(binary.BigEndian.Uint32(data[off:])))
	case 0xDE:
		return decodeMap(data, off+2, int(binary.BigEndian.Uint16(data[off:])))
	case 0xDF:
		return decodeMap(data, off+4, int(binary.BigEndian.Uint32(data[off:])))
	}
	return nil, 0, fmt.Errorf("msgpack: unsupported byte 0x%02x", b)
}

func decodeBin(data []byte, off, n int) (any, int, error) {
	end := off + n
	if end > len(data) {
		return nil, 0, fmt.Errorf("msgpack: bin length exceeds buffer")
	}
	out := make([]byte, n)
	copy(out, data[off:end])
	return out, end, nil
}

func decodeStr(data []byte, off, n int) (any, int, error) {
	end := off + n
	if end > len(data) {
		return nil, 0, fmt.Errorf("msgpack: str length exceeds buffer")
	}
	return string(data[off:end]), end, nil
}

func decodeArray(data []byte, off, n int) (any, int, error) {
	items := make([]any, n)
	for i := 0; i < n; i++ {
		var err error
		if items[i], off, err = decodeAt(data, off); err != nil {
			return nil, 0, err
		}
	}
	return items, off, nil
}

func decodeMap(data []byte, off, n int) (any, int, error) {
	out := make(map[string]any, n)
	for i := 0; i < n; i++ {
		key, koff, err := decodeAt(data, off)
		if err != nil {
			return nil, 0, err
		}
		val, voff, err := decodeAt(data, koff)
		if err != nil {
			return nil, 0, err
		}
		if ks, ok := key.(string); ok {
			out[ks] = val
		}
		off = voff
	}
	return out, off, nil
}
