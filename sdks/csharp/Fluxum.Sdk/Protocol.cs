// Fluxum message envelopes over the HiveLLM binary wire.
//
// Framing is the family standard: u32 LE length + MessagePack body, with a
// zero-length body as the WIRE-024 keep-alive. On top of it sit Fluxum's own
// pieces — the RPC-011 tagged envelope ([tag, payload]), RowList batches
// (RPC-032), and FluxBIN rows (FluxBin.cs).

using System;
using System.Buffers.Binary;
using System.Collections.Generic;

namespace Fluxum.Sdk;

internal sealed class ProtocolException : Exception
{
    public int? Code { get; }
    public ProtocolException(string message, int? code = null) : base(message) { Code = code; }
}

internal readonly struct ServerMessage
{
    public string Tag { get; }
    public IReadOnlyList<object?> Payload { get; }
    public ServerMessage(string tag, IReadOnlyList<object?> payload) { Tag = tag; Payload = payload; }
}

internal static class Protocol
{
    private const int FrameHeaderLen = 4;
    private const int DefaultMaxFrameBytes = 16 * 1024 * 1024;

    public static byte[] EncodeFrame(byte[] body)
    {
        var outb = new byte[FrameHeaderLen + body.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(outb, (uint)body.Length);
        Array.Copy(body, 0, outb, FrameHeaderLen, body.Length);
        return outb;
    }

    // Encode [tag, payload] and frame it (RPC-011). The payload is positional.
    public static byte[] EncodeMessage(string tag, List<object?> payload)
    {
        var body = MsgPack.Encode(new List<object?> { tag, payload });
        return EncodeFrame(body);
    }

    public static ServerMessage DecodeMessage(byte[] body)
    {
        var value = MsgPack.Decode(body);
        if (value is not List<object?> arr || arr.Count != 2 || arr[0] is not string tag)
            throw new ProtocolException("envelope is not a [tag, payload] pair", 400);
        var payload = arr[1] as List<object?> ?? new List<object?> { arr[1] };
        return new ServerMessage(tag, payload);
    }

    // Slice a flat RowList into its rows (RPC-032): [row_count, size_hint,
    // rows_data], where size_hint is ["Fixed", n] or ["Offsets", [start, ...]].
    public static List<byte[]> SliceRowList(object? value)
    {
        if (value is not List<object?> arr || arr.Count < 3)
            throw new ProtocolException("RowList is not a 3-field structure", 400);
        int count = ToInt(arr[0]);
        if (arr[2] is not byte[] data)
            throw new ProtocolException("RowList.rows_data is not binary", 400);
        if (arr[1] is not List<object?> hint || hint.Count == 0 || hint[0] is not string kind)
            throw new ProtocolException("RowList.size_hint is not tagged", 400);

        var rows = new List<byte[]>(count);
        if (kind == "Fixed")
        {
            int size = ToInt(hint[1]);
            if (size <= 0)
            {
                if (count != 0) throw new ProtocolException("Fixed size_hint of 0 with rows present", 400);
                return rows;
            }
            if (data.Length != count * size)
                throw new ProtocolException($"inconsistent RowList: {count} rows x {size} != {data.Length}", 400);
            for (int i = 0; i < count; i++) rows.Add(data[(i * size)..((i + 1) * size)]);
            return rows;
        }
        if (kind == "Offsets")
        {
            if (hint[1] is not List<object?> offs || offs.Count != count)
                throw new ProtocolException("inconsistent RowList: offsets length != row_count", 400);
            for (int i = 0; i < count; i++)
            {
                int start = ToInt(offs[i]);
                int end = i + 1 < count ? ToInt(offs[i + 1]) : data.Length;
                if (start > end || end > data.Length)
                    throw new ProtocolException("inconsistent RowList: offset out of range", 400);
                rows.Add(data[start..end]);
            }
            return rows;
        }
        throw new ProtocolException($"unknown RowList size_hint '{kind}'", 400);
    }

    // Decode one TableUpdate entry into (queryId, table, inserts, deletes).
    // Layout: [table_id, table_name, query_id, inserts(RowList), deletes(RowList)].
    public static (int QueryId, string Table, List<byte[]> Inserts, List<byte[]> Deletes) TableUpdate(object? entry)
    {
        if (entry is not List<object?> arr || arr.Count < 5)
            throw new ProtocolException("TableUpdate is not a 5-field structure", 400);
        int queryId = ToInt(arr[2]);
        string table = arr[1] as string ?? "";
        return (queryId, table, SliceRowList(arr[3]), SliceRowList(arr[4]));
    }

    public static int ToInt(object? v) => v switch
    {
        long l => (int)l,
        ulong u => (int)u,
        int i => i,
        _ => 0,
    };

    // Accumulates transport bytes and yields complete message bodies, skipping
    // keep-alive (zero-length) frames.
    public sealed class FrameReader
    {
        private readonly List<byte> _buf = new();

        public void Push(ReadOnlySpan<byte> chunk)
        {
            foreach (var b in chunk) _buf.Add(b);
        }

        public byte[]? NextBody()
        {
            while (true)
            {
                if (_buf.Count < FrameHeaderLen) return null;
                int length = (int)BinaryPrimitives.ReadUInt32LittleEndian(new[] { _buf[0], _buf[1], _buf[2], _buf[3] });
                if (length > DefaultMaxFrameBytes)
                    throw new ProtocolException($"frame of {length} bytes exceeds the cap", 413);
                int end = FrameHeaderLen + length;
                if (_buf.Count < end) return null;
                var body = new byte[length];
                _buf.CopyTo(FrameHeaderLen, body, 0, length);
                _buf.RemoveRange(0, end);
                if (length == 0) continue; // keep-alive
                return body;
            }
        }
    }
}
