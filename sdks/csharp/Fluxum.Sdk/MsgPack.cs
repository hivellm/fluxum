// A minimal MessagePack codec — just the subset the Fluxum wire uses.
//
// The SDK ships no third-party dependencies (SDK-080), so rather than pull in
// an external MessagePack package it carries the small codec it needs: the
// [tag, payload] envelopes (RPC-011) and the values inside them (nil, bool,
// ints of every width, float, str, bin, array, map). Framing and FluxBIN rows
// live elsewhere (Protocol.cs, FluxBin.cs).

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Text;

namespace Fluxum.Sdk;

internal static class MsgPack
{
    public static byte[] Encode(object? value)
    {
        var buf = new List<byte>();
        EncodeInto(value, buf);
        return buf.ToArray();
    }

    private static void EncodeInto(object? value, List<byte> outb)
    {
        switch (value)
        {
            case null:
                outb.Add(0xC0);
                break;
            case bool b:
                outb.Add(b ? (byte)0xC3 : (byte)0xC2);
                break;
            case byte[] bin:
                EncodeBin(bin, outb);
                break;
            case string s:
                EncodeStr(s, outb);
                break;
            case double d:
                outb.Add(0xCB);
                AppendU64(outb, BitConverter.DoubleToUInt64Bits(d));
                break;
            case float f:
                outb.Add(0xCB);
                AppendU64(outb, BitConverter.DoubleToUInt64Bits(f));
                break;
            case ulong ul:
                EncodeUint(ul, outb);
                break;
            case long l:
                EncodeInt(l, outb);
                break;
            case int i:
                EncodeInt(i, outb);
                break;
            case uint ui:
                EncodeUint(ui, outb);
                break;
            case System.Collections.IEnumerable list when value is not string:
                var items = new List<object?>();
                foreach (var it in list) items.Add(it);
                EncodeArray(items, outb);
                break;
            default:
                throw new FormatException($"msgpack: cannot encode {value.GetType().Name}");
        }
    }

    private static void EncodeInt(long n, List<byte> outb)
    {
        if (n >= 0) { EncodeUint((ulong)n, outb); return; }
        if (n >= -32) { outb.Add((byte)n); return; }
        if (n >= sbyte.MinValue) { outb.Add(0xD0); outb.Add((byte)(sbyte)n); return; }
        if (n >= short.MinValue) { outb.Add(0xD1); AppendU16(outb, (ushort)(short)n); return; }
        if (n >= int.MinValue) { outb.Add(0xD2); AppendU32(outb, (uint)(int)n); return; }
        outb.Add(0xD3); AppendU64(outb, (ulong)n);
    }

    private static void EncodeUint(ulong n, List<byte> outb)
    {
        if (n <= 0x7F) { outb.Add((byte)n); return; }
        if (n <= 0xFF) { outb.Add(0xCC); outb.Add((byte)n); return; }
        if (n <= 0xFFFF) { outb.Add(0xCD); AppendU16(outb, (ushort)n); return; }
        if (n <= 0xFFFFFFFF) { outb.Add(0xCE); AppendU32(outb, (uint)n); return; }
        outb.Add(0xCF); AppendU64(outb, n);
    }

    private static void EncodeStr(string s, List<byte> outb)
    {
        var data = Encoding.UTF8.GetBytes(s);
        int n = data.Length;
        if (n <= 31) outb.Add((byte)(0xA0 | n));
        else if (n <= 0xFF) { outb.Add(0xD9); outb.Add((byte)n); }
        else if (n <= 0xFFFF) { outb.Add(0xDA); AppendU16(outb, (ushort)n); }
        else { outb.Add(0xDB); AppendU32(outb, (uint)n); }
        outb.AddRange(data);
    }

    private static void EncodeBin(byte[] data, List<byte> outb)
    {
        int n = data.Length;
        if (n <= 0xFF) { outb.Add(0xC4); outb.Add((byte)n); }
        else if (n <= 0xFFFF) { outb.Add(0xC5); AppendU16(outb, (ushort)n); }
        else { outb.Add(0xC6); AppendU32(outb, (uint)n); }
        outb.AddRange(data);
    }

    private static void EncodeArray(List<object?> items, List<byte> outb)
    {
        int n = items.Count;
        if (n <= 15) outb.Add((byte)(0x90 | n));
        else if (n <= 0xFFFF) { outb.Add(0xDC); AppendU16(outb, (ushort)n); }
        else { outb.Add(0xDD); AppendU32(outb, (uint)n); }
        foreach (var it in items) EncodeInto(it, outb);
    }

    private static void AppendU16(List<byte> outb, ushort v)
    {
        Span<byte> t = stackalloc byte[2];
        BinaryPrimitives.WriteUInt16BigEndian(t, v);
        outb.Add(t[0]); outb.Add(t[1]);
    }

    private static void AppendU32(List<byte> outb, uint v)
    {
        Span<byte> t = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(t, v);
        for (int i = 0; i < 4; i++) outb.Add(t[i]);
    }

    private static void AppendU64(List<byte> outb, ulong v)
    {
        Span<byte> t = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(t, v);
        for (int i = 0; i < 8; i++) outb.Add(t[i]);
    }

    // Decode a single value; the whole buffer must be consumed. Numbers surface
    // as long (or ulong above the long range); binary as byte[]; strings as
    // string; arrays as List<object?>; maps as Dictionary<string, object?>.
    public static object? Decode(byte[] data)
    {
        var (value, off) = DecodeAt(data, 0);
        if (off != data.Length)
            throw new FormatException($"msgpack: {data.Length - off} trailing byte(s)");
        return value;
    }

    private static (object?, int) DecodeAt(byte[] data, int off)
    {
        if (off >= data.Length) throw new FormatException("msgpack: unexpected end of buffer");
        byte b = data[off++];

        if (b <= 0x7F) return ((long)b, off);
        if (b >= 0xE0) return ((long)(sbyte)b, off);
        if (b >= 0x80 && b <= 0x8F) return DecodeMap(data, off, b & 0x0F);
        if (b >= 0x90 && b <= 0x9F) return DecodeArray(data, off, b & 0x0F);
        if (b >= 0xA0 && b <= 0xBF) return DecodeStr(data, off, b & 0x1F);

        switch (b)
        {
            case 0xC0: return (null, off);
            case 0xC2: return (false, off);
            case 0xC3: return (true, off);
            case 0xC4: return DecodeBin(data, off + 1, data[off]);
            case 0xC5: return DecodeBin(data, off + 2, BinaryPrimitives.ReadUInt16BigEndian(data.AsSpan(off)));
            case 0xC6: return DecodeBin(data, off + 4, (int)BinaryPrimitives.ReadUInt32BigEndian(data.AsSpan(off)));
            case 0xCA: return ((double)BitConverter.UInt32BitsToSingle(BinaryPrimitives.ReadUInt32BigEndian(data.AsSpan(off))), off + 4);
            case 0xCB: return (BitConverter.UInt64BitsToDouble(BinaryPrimitives.ReadUInt64BigEndian(data.AsSpan(off))), off + 8);
            case 0xCC: return ((long)data[off], off + 1);
            case 0xCD: return ((long)BinaryPrimitives.ReadUInt16BigEndian(data.AsSpan(off)), off + 2);
            case 0xCE: return ((long)BinaryPrimitives.ReadUInt32BigEndian(data.AsSpan(off)), off + 4);
            case 0xCF:
                ulong u = BinaryPrimitives.ReadUInt64BigEndian(data.AsSpan(off));
                return (u <= long.MaxValue ? (long)u : u, off + 8);
            case 0xD0: return ((long)(sbyte)data[off], off + 1);
            case 0xD1: return ((long)BinaryPrimitives.ReadInt16BigEndian(data.AsSpan(off)), off + 2);
            case 0xD2: return ((long)BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(off)), off + 4);
            case 0xD3: return (BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(off)), off + 8);
            case 0xD9: return DecodeStr(data, off + 1, data[off]);
            case 0xDA: return DecodeStr(data, off + 2, BinaryPrimitives.ReadUInt16BigEndian(data.AsSpan(off)));
            case 0xDB: return DecodeStr(data, off + 4, (int)BinaryPrimitives.ReadUInt32BigEndian(data.AsSpan(off)));
            case 0xDC: return DecodeArray(data, off + 2, BinaryPrimitives.ReadUInt16BigEndian(data.AsSpan(off)));
            case 0xDD: return DecodeArray(data, off + 4, (int)BinaryPrimitives.ReadUInt32BigEndian(data.AsSpan(off)));
            case 0xDE: return DecodeMap(data, off + 2, BinaryPrimitives.ReadUInt16BigEndian(data.AsSpan(off)));
            case 0xDF: return DecodeMap(data, off + 4, (int)BinaryPrimitives.ReadUInt32BigEndian(data.AsSpan(off)));
        }
        throw new FormatException($"msgpack: unsupported byte 0x{b:x2}");
    }

    private static (object?, int) DecodeBin(byte[] data, int off, int n)
    {
        int end = off + n;
        if (end > data.Length) throw new FormatException("msgpack: bin length exceeds buffer");
        var outb = new byte[n];
        Array.Copy(data, off, outb, 0, n);
        return (outb, end);
    }

    private static (object?, int) DecodeStr(byte[] data, int off, int n)
    {
        int end = off + n;
        if (end > data.Length) throw new FormatException("msgpack: str length exceeds buffer");
        return (Encoding.UTF8.GetString(data, off, n), end);
    }

    private static (object?, int) DecodeArray(byte[] data, int off, int n)
    {
        var items = new List<object?>(n);
        for (int i = 0; i < n; i++)
        {
            var (item, next) = DecodeAt(data, off);
            items.Add(item);
            off = next;
        }
        return (items, off);
    }

    private static (object?, int) DecodeMap(byte[] data, int off, int n)
    {
        var outb = new Dictionary<string, object?>(n);
        for (int i = 0; i < n; i++)
        {
            var (key, koff) = DecodeAt(data, off);
            var (val, voff) = DecodeAt(data, koff);
            if (key is string ks) outb[ks] = val;
            off = voff;
        }
        return (outb, off);
    }
}
