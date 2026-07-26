// The async/await Fluxum client (SPEC-011 SDK-060).
//
// One Connection drives a session over FluxRPC/TCP: authenticate, subscribe
// (each query's InitialData lands in a local row cache), call reducers, and
// receive TxUpdate diffs on the same socket. On connection loss the client
// reconnects, re-authenticates, resubscribes every active query and reconciles
// its cache (SDK-047) — the application keeps its handle across the outage.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Fluxum.Sdk;

/// <summary>A table's cache hooks: its name and how to derive a primary key
/// from a full row's bytes and from a delete entry's bytes.</summary>
public sealed class TableSchema
{
    public string Name { get; }
    public Func<byte[], string> PkOfRow { get; }
    public Func<byte[], string> PkOfDelete { get; }

    public TableSchema(string name, Func<byte[], string> pkOfRow, Func<byte[], string> pkOfDelete)
    {
        Name = name;
        PkOfRow = pkOfRow;
        PkOfDelete = pkOfDelete;
    }
}

/// <summary>A server-reported failure. <see cref="Code"/> is the stable
/// SPEC-028 catalog code (the portable assertion); <see cref="Catalog"/> is
/// the SCREAMING_SNAKE name for an Error frame (null for a reducer rejection);
/// <see cref="AppCode"/> is the reducer's optional application code.</summary>
public sealed class FluxumException : Exception
{
    public int Code { get; }
    public string? Catalog { get; }
    public string? AppCode { get; }

    public FluxumException(int code, string message, string? catalog = null, string? appCode = null)
        : base($"fluxum: error {code}: {message}")
    {
        Code = code;
        Catalog = catalog;
        AppCode = appCode;
    }
}

/// <summary>The client's row cache: per table, a pk → row-bytes map,
/// materialized from the rows any active subscription currently holds.</summary>
public sealed class Cache
{
    private readonly object _lock = new();
    private readonly Dictionary<string, Dictionary<string, byte[]>> _rows = new();
    private readonly Dictionary<string, Dictionary<string, HashSet<int>>> _owners = new();
    private readonly HashSet<string> _known = new();

    internal Cache(IEnumerable<TableSchema> tables)
    {
        foreach (var t in tables)
        {
            _rows[t.Name] = new();
            _owners[t.Name] = new();
            _known.Add(t.Name);
        }
    }

    /// <summary>Every currently-cached row of <paramref name="table"/>.</summary>
    public IReadOnlyList<byte[]> Rows(string table)
    {
        lock (_lock)
        {
            return _rows.TryGetValue(table, out var m) ? new List<byte[]>(m.Values) : new List<byte[]>();
        }
    }

    internal void Insert(string table, int queryId, string pk, byte[] row)
    {
        if (!_known.Contains(table)) return;
        _rows[table][pk] = row;
        if (!_owners[table].TryGetValue(pk, out var set)) { set = new(); _owners[table][pk] = set; }
        set.Add(queryId);
    }

    internal void Delete(string table, int queryId, string pk)
    {
        if (!_known.Contains(table)) return;
        if (!_owners[table].TryGetValue(pk, out var set)) return;
        set.Remove(queryId);
        if (set.Count == 0) { _owners[table].Remove(pk); _rows[table].Remove(pk); }
    }

    internal void DropQuery(int queryId)
    {
        lock (_lock)
        {
            foreach (var (table, owners) in _owners)
                foreach (var pk in new List<string>(owners.Keys))
                {
                    owners[pk].Remove(queryId);
                    if (owners[pk].Count == 0) { owners.Remove(pk); _rows[table].Remove(pk); }
                }
        }
    }

    internal void Clear()
    {
        lock (_lock)
        {
            foreach (var t in new List<string>(_rows.Keys)) { _rows[t].Clear(); _owners[t].Clear(); }
        }
    }

    internal object Lock => _lock;
}

public sealed class Connection : IAsyncDisposable
{
    private readonly string _host;
    private readonly int _port;
    private readonly byte[] _token;
    private readonly Dictionary<string, TableSchema> _schemas;
    private readonly Cache _cache;

    private TcpClient? _tcp;
    private NetworkStream? _stream;
    private Protocol.FrameReader _frames = new();
    private int _nextId = 1;
    private readonly ConcurrentDictionary<int, Channel<ServerMessage>> _pending = new();
    private readonly List<(string Sql, int QueryId)> _subs = new();
    private readonly object _sendLock = new();
    private string _identity = new string('0', 64);
    private volatile bool _closed;
    private Task? _readerTask;

    private Connection(string host, int port, byte[] token, IEnumerable<TableSchema> tables)
    {
        _host = host;
        _port = port;
        _token = token;
        _schemas = new();
        foreach (var t in tables) _schemas[t.Name] = t;
        _cache = new Cache(_schemas.Values);
    }

    public Cache Cache => _cache;
    public string Identity { get { lock (_sendLock) return _identity; } }

    /// <summary>Open and authenticate a session. <paramref name="url"/> is
    /// <c>fluxum://host:port</c> or a bare <c>host:port</c> (TCP).</summary>
    public static async Task<Connection> ConnectAsync(string url, byte[] token, IEnumerable<TableSchema> tables, CancellationToken ct = default)
    {
        var (host, port) = ParseUrl(url);
        var conn = new Connection(host, port, token, tables);
        await conn.EstablishAsync(ct).ConfigureAwait(false);
        conn._readerTask = Task.Run(conn.ReadLoopAsync);
        return conn;
    }

    public async ValueTask DisposeAsync() => await CloseAsync().ConfigureAwait(false);

    public async Task CloseAsync()
    {
        _closed = true;
        _tcp?.Close();
        if (_readerTask is not null)
        {
            try { await _readerTask.ConfigureAwait(false); } catch { /* shutting down */ }
        }
    }

    private async Task EstablishAsync(CancellationToken ct)
    {
        var tcp = new TcpClient();
        await tcp.ConnectAsync(_host, _port, ct).ConfigureAwait(false);
        tcp.NoDelay = true;
        _tcp = tcp;
        _stream = tcp.GetStream();
        _frames = new();

        int authId = AllocId();
        await SendRawAsync("Authenticate", new List<object?> { authId, _token, null, null, null }, ct).ConfigureAwait(false);
        while (true)
        {
            var msg = await ReadInlineAsync(ct).ConfigureAwait(false);
            if (msg.Tag == "Error" && MsgId(msg) == authId) throw ErrorFrom(msg);
            if (msg.Tag == "AuthResult" && Protocol.ToInt(msg.Payload[0]) == authId)
            {
                lock (_sendLock) _identity = HexOf(msg.Payload[1]);
                break;
            }
        }

        List<(string, int)> subs;
        lock (_sendLock) subs = new(_subs);
        if (subs.Count > 0)
        {
            _cache.Clear();
            var sqls = subs.ConvertAll(s => s.Item1);
            lock (_sendLock) _subs.Clear();
            await ResubscribeInlineAsync(sqls, ct).ConfigureAwait(false);
        }
    }

    private async Task<ServerMessage> ReadInlineAsync(CancellationToken ct)
    {
        var buf = new byte[65536];
        while (true)
        {
            var body = _frames.NextBody();
            if (body is not null) return Protocol.DecodeMessage(body);
            int n = await _stream!.ReadAsync(buf, ct).ConfigureAwait(false);
            if (n == 0) throw new IOException("connection closed during handshake");
            _frames.Push(buf.AsSpan(0, n));
        }
    }

    private async Task ReadLoopAsync()
    {
        var buf = new byte[65536];
        int backoffMs = 200;
        while (!_closed)
        {
            try
            {
                int n = await _stream!.ReadAsync(buf).ConfigureAwait(false);
                if (n == 0) throw new IOException("connection closed");
                _frames.Push(buf.AsSpan(0, n));
                while (_frames.NextBody() is { } body)
                    Route(Protocol.DecodeMessage(body));
                backoffMs = 200;
            }
            catch (Exception)
            {
                if (_closed) return;
                FailPending();
                _tcp?.Close();
                while (!_closed)
                {
                    await Task.Delay(backoffMs).ConfigureAwait(false);
                    backoffMs = Math.Min(backoffMs * 2, 5000);
                    try
                    {
                        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                        await EstablishAsync(cts.Token).ConfigureAwait(false);
                        backoffMs = 200;
                        break;
                    }
                    catch { /* keep retrying */ }
                }
            }
        }
    }

    private void Route(ServerMessage msg)
    {
        if (msg.Tag == "TxUpdate") { ApplyTxUpdate(msg); return; }
        int id = MsgId(msg);
        if (id >= 0 && _pending.TryGetValue(id, out var ch)) ch.Writer.TryWrite(msg);
    }

    private void FailPending()
    {
        foreach (var ch in _pending.Values)
            ch.Writer.TryWrite(new ServerMessage("__disconnected__", Array.Empty<object?>()));
    }

    private int AllocId() { lock (_sendLock) return _nextId++; }

    private async Task SendRawAsync(string tag, List<object?> payload, CancellationToken ct)
    {
        var frame = Protocol.EncodeMessage(tag, payload);
        await _stream!.WriteAsync(frame, ct).ConfigureAwait(false);
    }

    private async Task<ServerMessage> RequestAsync(string tag, Func<int, List<object?>> payloadFn, CancellationToken ct)
    {
        int id = AllocId();
        var ch = Channel.CreateUnbounded<ServerMessage>();
        _pending[id] = ch;
        try
        {
            await SendRawAsync(tag, payloadFn(id), ct).ConfigureAwait(false);
            var msg = await ch.Reader.ReadAsync(ct).ConfigureAwait(false);
            if (msg.Tag == "__disconnected__") throw new IOException("disconnected while awaiting a reply");
            return msg;
        }
        finally { _pending.TryRemove(id, out _); }
    }

    /// <summary>Register queries; await each InitialData, apply it to the
    /// cache, and return the server-assigned query ids in query order.</summary>
    public async Task<List<int>> SubscribeAsync(IReadOnlyList<string> queries, CancellationToken ct = default)
    {
        int id = AllocId();
        var ch = Channel.CreateUnbounded<ServerMessage>();
        _pending[id] = ch;
        var queryIds = new List<int>();
        try
        {
            await SendRawAsync("Subscribe", new List<object?> { id, ToObjectList(queries) }, ct).ConfigureAwait(false);
            while (queryIds.Count < queries.Count)
            {
                var msg = await ch.Reader.ReadAsync(ct).ConfigureAwait(false);
                if (msg.Tag == "__disconnected__") throw new IOException("disconnected during subscribe");
                if (msg.Tag == "Error") throw ErrorFrom(msg);
                if (msg.Tag != "InitialData") continue;
                queryIds.AddRange(ApplyInitialData(msg));
            }
        }
        finally { _pending.TryRemove(id, out _); }
        lock (_sendLock)
            for (int i = 0; i < queryIds.Count && i < queries.Count; i++)
                _subs.Add((queries[i], queryIds[i]));
        return queryIds;
    }

    private async Task ResubscribeInlineAsync(List<string> queries, CancellationToken ct)
    {
        int id = AllocId();
        await SendRawAsync("Subscribe", new List<object?> { id, ToObjectList(queries) }, ct).ConfigureAwait(false);
        var queryIds = new List<int>();
        while (queryIds.Count < queries.Count)
        {
            var msg = await ReadInlineAsync(ct).ConfigureAwait(false);
            if (msg.Tag == "Error" && MsgId(msg) == id) throw ErrorFrom(msg);
            if (msg.Tag == "TxUpdate") { ApplyTxUpdate(msg); continue; }
            if (msg.Tag != "InitialData" || Protocol.ToInt(msg.Payload[0]) != id) continue;
            queryIds.AddRange(ApplyInitialData(msg));
        }
        lock (_sendLock)
            for (int i = 0; i < queryIds.Count && i < queries.Count; i++)
                _subs.Add((queries[i], queryIds[i]));
    }

    private List<int> ApplyInitialData(ServerMessage msg)
    {
        var ids = new List<int>();
        if (msg.Payload[2] is not List<object?> tables) return ids;
        lock (_cache.Lock)
            foreach (var entry in tables)
            {
                var (qid, table, inserts, deletes) = Protocol.TableUpdate(entry);
                ids.Add(qid);
                ApplyDiff(table, qid, inserts, deletes);
            }
        return ids;
    }

    /// <summary>Drop the subscriptions whose query ids are given (RPC-024).</summary>
    public async Task UnsubscribeAsync(IReadOnlyList<int> queryIds, CancellationToken ct = default)
    {
        await SendRawAsync("Unsubscribe", new List<object?> { AllocId(), ToObjectList(queryIds) }, ct).ConfigureAwait(false);
        foreach (var qid in queryIds) _cache.DropQuery(qid);
        var wanted = new HashSet<int>(queryIds);
        lock (_sendLock) _subs.RemoveAll(s => wanted.Contains(s.QueryId));
    }

    /// <summary>Call reducer <paramref name="name"/> with <paramref name="args"/>;
    /// returns on commit, throws <see cref="FluxumException"/> on rejection.</summary>
    public async Task CallReducerAsync(string name, IReadOnlyList<object?> args, CancellationToken ct = default)
    {
        var msg = await RequestAsync("ReducerCall",
            id => new List<object?> { id, name, null, new List<object?>(args), null }, ct).ConfigureAwait(false);
        if (msg.Tag == "Error") throw ErrorFrom(msg);
        if (msg.Tag == "ReducerResult")
        {
            if (msg.Payload[1] is List<object?> outcome && outcome.Count >= 2 && outcome[0] as string == "Err"
                && outcome[1] is List<object?> e && e.Count >= 3)
                throw new FluxumException(Protocol.ToInt(e[0]), StrOf(e[2]), appCode: e[1] as string);
            return;
        }
        throw new FluxumException(0, $"unexpected reply to reducer call: {msg.Tag}");
    }

    private void ApplyTxUpdate(ServerMessage msg)
    {
        if (msg.Payload.Count < 6 || msg.Payload[5] is not List<object?> tables) return;
        lock (_cache.Lock)
            foreach (var entry in tables)
            {
                var (qid, table, inserts, deletes) = Protocol.TableUpdate(entry);
                ApplyDiff(table, qid, inserts, deletes);
            }
    }

    // Deletes before inserts so an update (delete + insert of the same pk)
    // leaves the new row (SPEC-005). Caller holds the cache lock.
    private void ApplyDiff(string table, int qid, List<byte[]> inserts, List<byte[]> deletes)
    {
        if (!_schemas.TryGetValue(table, out var schema)) return;
        foreach (var entry in deletes) _cache.Delete(table, qid, schema.PkOfDelete(entry));
        foreach (var row in inserts) _cache.Insert(table, qid, schema.PkOfRow(row), row);
    }

    // --- helpers -----------------------------------------------------------

    private static (string, int) ParseUrl(string url)
    {
        string rest = url;
        foreach (var scheme in new[] { "fluxum://", "tcp://" })
            if (rest.StartsWith(scheme)) { rest = rest[scheme.Length..]; break; }
        int i = rest.LastIndexOf(':');
        if (i <= 0 || i == rest.Length - 1)
            throw new ArgumentException($"expected host:port, got '{url}'");
        return (rest[..i], int.Parse(rest[(i + 1)..]));
    }

    private static int MsgId(ServerMessage msg) => msg.Tag switch
    {
        "AuthResult" or "ReducerResult" or "InitialData" => Protocol.ToInt(msg.Payload[0]),
        "Error" => msg.Payload.Count > 0 && msg.Payload[0] is not null ? Protocol.ToInt(msg.Payload[0]) : -1,
        _ => -1,
    };

    private static FluxumException ErrorFrom(ServerMessage msg)
    {
        var p = msg.Payload;
        int code = p.Count > 1 ? Protocol.ToInt(p[1]) : 0;
        string? catalog = p.Count > 2 ? p[2] as string : null;
        string message = p.Count > 3 ? StrOf(p[3]) : "";
        return new FluxumException(code, message, catalog);
    }

    private static string HexOf(object? v) => v is byte[] b ? Convert.ToHexString(b).ToLowerInvariant() : StrOf(v);

    private static string StrOf(object? v) => v as string ?? v?.ToString() ?? "";

    private static List<object?> ToObjectList<T>(IEnumerable<T> items)
    {
        var outb = new List<object?>();
        foreach (var it in items) outb.Add(it);
        return outb;
    }
}
