// FluxBIN — the schema-driven binary row encoding (SPEC-006 RPC-040..042).
//
// No field names and no per-value type tags: the schema supplies the type
// context, so a row is its column values back-to-back in declaration order.
// All integers are little-endian. 64-bit values surface as long/ulong (no
// precision loss); Identity/ConnectionId surface as lowercase hex strings.

using System;
using System.Buffers.Binary;
using System.Text;

namespace Fluxum.Sdk;

/// <summary>Thrown when bytes do not match the schema being decoded against.</summary>
public sealed class FluxBinException : Exception
{
    public FluxBinException(string message) : base("fluxbin: " + message) { }
}

/// <summary>A column's name and its FluxType (as <c>/schema</c> spells it).</summary>
public readonly record struct Column(string Name, string Type);

/// <summary>Sequential FluxBIN reader over a row buffer.</summary>
public sealed class RowReader
{
    private readonly byte[] _data;
    private int _off;

    public RowReader(byte[] data) { _data = data; }

    public int Remaining => _data.Length - _off;

    private ReadOnlySpan<byte> Take(int n)
    {
        if (Remaining < n)
            throw new FluxBinException($"unexpected end of row: needed {n}, have {Remaining}");
        var span = _data.AsSpan(_off, n);
        _off += n;
        return span;
    }

    /// <summary>Read one value of the given FluxType. Concrete types: bool;
    /// long for I8..I64/Timestamp; ulong for U8..U64/EntityId; double for
    /// F32/F64; string for Str/Identity/ConnectionId; byte[] for Bytes.</summary>
    public object Read(string fluxType)
    {
        switch (fluxType)
        {
            case "Bool":
                byte bb = Take(1)[0];
                if (bb > 1) throw new FluxBinException($"invalid bool byte 0x{bb:x2}");
                return bb == 1;
            case "I8": return (long)(sbyte)Take(1)[0];
            case "U8": return (ulong)Take(1)[0];
            case "I16": return (long)BinaryPrimitives.ReadInt16LittleEndian(Take(2));
            case "U16": return (ulong)BinaryPrimitives.ReadUInt16LittleEndian(Take(2));
            case "I32": return (long)BinaryPrimitives.ReadInt32LittleEndian(Take(4));
            case "U32": return (ulong)BinaryPrimitives.ReadUInt32LittleEndian(Take(4));
            case "I64":
            case "Timestamp": return BinaryPrimitives.ReadInt64LittleEndian(Take(8));
            case "U64":
            case "EntityId": return BinaryPrimitives.ReadUInt64LittleEndian(Take(8));
            case "F32": return (double)BitConverter.UInt32BitsToSingle(BinaryPrimitives.ReadUInt32LittleEndian(Take(4)));
            case "F64": return BitConverter.UInt64BitsToDouble(BinaryPrimitives.ReadUInt64LittleEndian(Take(8)));
            case "Str":
                var sbytes = ReadLenBytes();
                try { return Encoding.UTF8.GetString(sbytes); }
                catch (Exception) { throw new FluxBinException("string is not valid UTF-8"); }
            case "Bytes": return ReadLenBytes().ToArray();
            case "Identity": return Convert.ToHexString(Take(32)).ToLowerInvariant();
            case "ConnectionId": return Convert.ToHexString(Take(16)).ToLowerInvariant();
            default: throw new FluxBinException($"unsupported type {fluxType}");
        }
    }

    private ReadOnlySpan<byte> ReadLenBytes()
    {
        int n = (int)BinaryPrimitives.ReadUInt32LittleEndian(Take(4));
        return Take(n);
    }
}

public static class FluxBin
{
    /// <summary>Decode one row into a dictionary keyed by column name.</summary>
    public static System.Collections.Generic.Dictionary<string, object> DecodeRow(byte[] data, Column[] columns)
    {
        var reader = new RowReader(data);
        var row = new System.Collections.Generic.Dictionary<string, object>(columns.Length);
        foreach (var col in columns)
            row[col.Name] = reader.Read(col.Type);
        if (reader.Remaining != 0)
            throw new FluxBinException($"row has {reader.Remaining} trailing byte(s): schema mismatch");
        return row;
    }
}
