// The shared SDK conformance corpus, run by the C# client (TST-052).
//
// Every SDK executes the SAME declarative corpus (tests/conformance/ at the
// repo root) against the same server build; identical observable results are
// required from all runners. This is the .NET runner: an interpreter of the
// corpus step vocabulary, release-blocking when red (SDK-064).

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Fluxum.Sdk;

namespace Fluxum.Sdk.Tests;

public class ConformanceTests
{
    private static readonly TimeSpan AwaitTimeout = TimeSpan.FromSeconds(5);
    private static readonly HashSet<string> StringyTypes = new() { "U64", "I64", "Timestamp", "EntityId" };

    private static string RepoRoot()
    {
        // The test binary runs from bin/<config>/<tfm>/; walk up to the repo.
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "Cargo.toml")))
            dir = dir.Parent;
        return dir?.FullName ?? throw new InvalidOperationException("repo root not found");
    }

    private static string ServerBinary()
    {
        var name = OperatingSystem.IsWindows() ? "fluxum-server.exe" : "fluxum-server";
        return Path.Combine(RepoRoot(), "target", "debug", name);
    }

    private static string CorpusDir() => Path.Combine(RepoRoot(), "tests", "conformance");

    private static bool ServerAvailable() => File.Exists(ServerBinary());

    public static IEnumerable<object[]> Scenarios()
    {
        // The corpus manifest ships in the repo, so scenarios enumerate even
        // without the server binary; a missing binary skips the body below.
        var manifest = JsonDocument.Parse(File.ReadAllText(Path.Combine(CorpusDir(), "corpus.json")));
        foreach (var s in manifest.RootElement.GetProperty("scenarios").EnumerateArray())
            yield return new object[] { s.GetString()! };
    }

    [Theory]
    [MemberData(nameof(Scenarios))]
    public async Task Scenario(string name)
    {
        if (!ServerAvailable())
            return; // no fluxum-server binary — run: cargo build -p fluxum-server
        var manifest = LoadManifest();
        var scenario = JsonDocument.Parse(
            File.ReadAllText(Path.Combine(CorpusDir(), "scenarios", name + ".json")));
        using var server = new Server(name);
        var runner = new Interp(manifest, server);
        try
        {
            foreach (var step in scenario.RootElement.GetProperty("steps").EnumerateArray())
            {
                var prop = step.EnumerateObject().First();
                await runner.RunStep(prop.Name, prop.Value);
            }
        }
        finally
        {
            await runner.CloseAll();
        }
    }

    private static Manifest LoadManifest()
    {
        var doc = JsonDocument.Parse(File.ReadAllText(Path.Combine(CorpusDir(), "corpus.json")));
        var tables = new Dictionary<string, TableSpec>();
        foreach (var t in doc.RootElement.GetProperty("tables").EnumerateObject())
        {
            var cols = t.Value.GetProperty("columns").EnumerateArray()
                .Select(c => (c[0].GetString()!, c[1].GetString()!)).ToList();
            tables[t.Name] = new TableSpec(t.Value.GetProperty("primary_key").GetString()!, cols);
        }
        return new Manifest(tables);
    }

    // --- server spawning ---------------------------------------------------

    private sealed class Server : IDisposable
    {
        public int TcpPort { get; }
        private readonly int _httpPort;
        private readonly string _dir;
        private readonly Dictionary<string, string?> _env;
        private Process? _proc;

        public Server(string label)
        {
            TcpPort = FreePort();
            _httpPort = FreePort();
            _dir = Path.Combine(Path.GetTempPath(), $"fluxum-cs-{label}-{Environment.ProcessId}-{Guid.NewGuid():N}");
            Directory.CreateDirectory(_dir);
            _env = new()
            {
                ["FLUXUM_PROFILE"] = "development",
                ["FLUXUM_SERVER_HTTP_PORT"] = _httpPort.ToString(),
                ["FLUXUM_SERVER_TCP_PORT"] = TcpPort.ToString(),
                ["FLUXUM_STORAGE_DATA_DIR"] = _dir,
                ["FLUXUM_STORAGE_COMMIT_LOG_DIR"] = Path.Combine(_dir, "log"),
            };
            Launch();
        }

        public string TcpUrl => $"fluxum://127.0.0.1:{TcpPort}";

        private void Launch()
        {
            var psi = new ProcessStartInfo(ServerBinary())
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            };
            foreach (var (k, v) in _env) psi.Environment[k] = v;
            _proc = Process.Start(psi) ?? throw new InvalidOperationException("spawn server");
            var deadline = DateTime.UtcNow.AddSeconds(20);
            while (DateTime.UtcNow < deadline)
            {
                try { using var c = new TcpClient(); c.Connect("127.0.0.1", TcpPort); return; }
                catch { Thread.Sleep(100); }
            }
            throw new InvalidOperationException($"server did not bind {TcpPort}");
        }

        public void Restart() { Stop(); Launch(); }

        public void Stop()
        {
            if (_proc is { HasExited: false }) { _proc.Kill(true); _proc.WaitForExit(10000); }
        }

        public void Dispose() => Stop();

        private static int FreePort()
        {
            var l = new TcpListener(System.Net.IPAddress.Loopback, 0);
            l.Start();
            int port = ((System.Net.IPEndPoint)l.LocalEndpoint).Port;
            l.Stop();
            return port;
        }
    }

    // --- corpus model ------------------------------------------------------

    private sealed record Manifest(Dictionary<string, TableSpec> Tables);
    private sealed record TableSpec(string PrimaryKey, List<(string Name, string Type)> Columns);

    private sealed class Interp
    {
        private readonly Manifest _m;
        private readonly Server _srv;
        private readonly Dictionary<string, Connection> _clients = new();
        private readonly Dictionary<string, List<int>> _handles = new();

        public Interp(Manifest m, Server srv) { _m = m; _srv = srv; }

        private List<TableSchema> TableSchemas()
        {
            var schemas = new List<TableSchema>();
            foreach (var (name, spec) in _m.Tables)
            {
                var types = spec.Columns.Select(c => c.Type).ToArray();
                int pkIndex = spec.Columns.FindIndex(c => c.Name == spec.PrimaryKey);
                var pkType = types[pkIndex];
                schemas.Add(new TableSchema(name,
                    row =>
                    {
                        var r = new RowReader(row);
                        object? v = null;
                        for (int i = 0; i <= pkIndex; i++) v = r.Read(types[i]);
                        return CanonicalStr(v);
                    },
                    entry => CanonicalStr(new RowReader(entry).Read(pkType))));
            }
            return schemas;
        }

        private static string CanonicalStr(object? v) => v switch
        {
            bool b => b ? "True" : "False",
            _ => v?.ToString() ?? "",
        };

        private Dictionary<string, object?> CanonicalRow(string table, byte[] row)
        {
            var spec = _m.Tables[table];
            var columns = spec.Columns.Select(c => new Column(c.Name, c.Type)).ToArray();
            var decoded = FluxBin.DecodeRow(row, columns);
            var outb = new Dictionary<string, object?>();
            foreach (var (colName, colType) in spec.Columns)
                outb[colName] = Canonicalize(decoded[colName], colType);
            return outb;
        }

        private static object? Canonicalize(object? v, string fluxType) =>
            StringyTypes.Contains(fluxType) ? v?.ToString() : v;

        private Connection Client(object? name)
        {
            var key = name?.ToString() ?? "";
            if (!_clients.TryGetValue(key, out var c))
                throw new Xunit.Sdk.XunitException($"step names client {key} before its connect step");
            return c;
        }

        // Corpus values arrive as JsonElement; unwrap a string one so the "*"
        // and "$identity:NAME" escapes are seen (they never match `is string`).
        private static string? AsString(object? expected) => expected switch
        {
            string s => s,
            JsonElement je when je.ValueKind == JsonValueKind.String => je.GetString(),
            _ => null,
        };

        private bool Matches(object? expected, object? actual)
        {
            var es = AsString(expected);
            if (es == "*") return true;
            if (es is not null && es.StartsWith("$identity:"))
            {
                var wantId = Client(es["$identity:".Length..]).Identity;
                return wantId == (actual as string ?? actual?.ToString());
            }
            return ValuesEqual(expected, actual);
        }

        private static bool ValuesEqual(object? expected, object? actual)
        {
            if (expected is JsonElement je) return JsonEquals(je, actual);
            return string.Equals(expected?.ToString(), actual?.ToString(), StringComparison.Ordinal);
        }

        private static bool JsonEquals(JsonElement e, object? actual) => e.ValueKind switch
        {
            JsonValueKind.String => e.GetString() == (actual as string ?? actual?.ToString()),
            JsonValueKind.True => actual is true,
            JsonValueKind.False => actual is false,
            JsonValueKind.Number => e.GetRawText() == actual?.ToString(),
            _ => e.ToString() == actual?.ToString(),
        };

        private bool RowMatches(JsonElement expected, Dictionary<string, object?> actual)
        {
            foreach (var prop in expected.EnumerateObject())
                if (!Matches(prop.Value, actual.GetValueOrDefault(prop.Name)))
                    return false;
            return true;
        }

        public async Task RunStep(string kind, JsonElement body)
        {
            switch (kind)
            {
                case "connect":
                    var name = body.GetProperty("client").GetString()!;
                    byte[] token = body.TryGetProperty("token", out var tok) && tok.ValueKind != JsonValueKind.Null
                        ? System.Text.Encoding.UTF8.GetBytes(tok.GetString()!)
                        : Array.Empty<byte>();
                    using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
                        _clients[name] = await Connection.ConnectAsync(_srv.TcpUrl, token, TableSchemas(), cts.Token);
                    break;
                case "close":
                    await Client(body.GetProperty("client")).CloseAsync();
                    break;
                case "restart_server":
                    _srv.Restart();
                    break;
                case "subscribe":
                    var ids = await Client(body.GetProperty("client")).SubscribeAsync(Strings(body.GetProperty("queries")));
                    if (body.TryGetProperty("as", out var asLabel) && asLabel.ValueKind == JsonValueKind.String)
                        _handles[asLabel.GetString()!] = ids;
                    break;
                case "unsubscribe":
                    var label = body.GetProperty("handles").GetString()!;
                    await Client(body.GetProperty("client")).UnsubscribeAsync(_handles[label]);
                    break;
                case "call":
                    await Call(body);
                    break;
                case "subscribe_error":
                    await ExpectError(
                        Client(body.GetProperty("client")).SubscribeAsync(Strings(body.GetProperty("queries"))),
                        body.GetProperty("expect_error"));
                    break;
                case "call_until_error":
                    await CallUntilError(body);
                    break;
                case "await_row":
                    await AwaitCount(body, 1, atLeast: true);
                    break;
                case "await_gone":
                    await AwaitCount(body, 0, atLeast: false);
                    break;
                case "await_count":
                    await AwaitCount(body, body.GetProperty("count").GetInt32(), atLeast: false);
                    break;
                case "expect_cache":
                    ExpectCache(body);
                    break;
                case "expect_distinct_identities":
                    var names = Strings(body.GetProperty("clients"));
                    var seen = new HashSet<string>();
                    foreach (var n in names)
                        Assert.True(seen.Add(Client(n).Identity), $"identities collide at {n}");
                    break;
                default:
                    throw new Xunit.Sdk.XunitException($"unknown step {kind}");
            }
        }

        private async Task Call(JsonElement body)
        {
            var call = Client(body.GetProperty("client"))
                .CallReducerAsync(body.GetProperty("reducer").GetString()!, Args(body.GetProperty("args")));
            if (body.TryGetProperty("expect_error", out var expect))
                await ExpectError(call, expect);
            else
                await call;
        }

        private async Task CallUntilError(JsonElement body)
        {
            var client = Client(body.GetProperty("client"));
            int attempts = body.GetProperty("attempts").GetInt32();
            var expect = body.GetProperty("expect_error");
            for (int i = 0; i < attempts; i++)
            {
                try { await client.CallReducerAsync(body.GetProperty("reducer").GetString()!, Args(body.GetProperty("args"))); }
                catch (FluxumException e) { MatchError(e, expect); return; }
            }
            throw new Xunit.Sdk.XunitException($"all {attempts} calls succeeded; expected an error");
        }

        private async Task AwaitCount(JsonElement body, int want, bool atLeast)
        {
            var client = Client(body.GetProperty("client"));
            var table = body.GetProperty("table").GetString()!;
            var where = body.TryGetProperty("where", out var w) ? w : default;
            var deadline = DateTime.UtcNow + AwaitTimeout;
            while (true)
            {
                int matching = client.Cache.Rows(table).Count(r =>
                    where.ValueKind != JsonValueKind.Object || RowMatches(where, CanonicalRow(table, r)));
                if (atLeast ? matching >= want : matching == want) return;
                if (DateTime.UtcNow > deadline)
                    throw new Xunit.Sdk.XunitException($"await {table}: {matching} matching, wanted {want} after {AwaitTimeout}");
                await Task.Delay(25);
            }
        }

        private void ExpectCache(JsonElement body)
        {
            var client = Client(body.GetProperty("client"));
            var table = body.GetProperty("table").GetString()!;
            var actual = client.Cache.Rows(table).Select(r => CanonicalRow(table, r)).ToList();
            foreach (var want in body.GetProperty("rows").EnumerateArray())
            {
                int idx = actual.FindIndex(row => RowMatches(want, row));
                if (idx < 0)
                    throw new Xunit.Sdk.XunitException($"{table}: no cached row matches {want}");
                actual.RemoveAt(idx);
            }
            if (actual.Count != 0)
                throw new Xunit.Sdk.XunitException($"{table}: unexpected extra rows ({actual.Count})");
        }

        private static async Task ExpectError(Task op, JsonElement expect)
        {
            try { await op; }
            catch (FluxumException e) { MatchError(e, expect); return; }
            throw new Xunit.Sdk.XunitException("operation succeeded; the scenario expected an error");
        }

        private static async Task ExpectError<T>(Task<T> op, JsonElement expect)
        {
            try { await op; }
            catch (FluxumException e) { MatchError(e, expect); return; }
            throw new Xunit.Sdk.XunitException("operation succeeded; the scenario expected an error");
        }

        private static void MatchError(FluxumException e, JsonElement expect)
        {
            if (expect.TryGetProperty("contains", out var c))
                Assert.Contains(c.GetString()!, e.Message);
            if (expect.TryGetProperty("code", out var code))
                Assert.Equal(code.GetInt32(), e.Code);
            if (expect.TryGetProperty("catalog", out var cat))
                Assert.Equal(cat.GetString(), e.Catalog);
        }

        public async Task CloseAll()
        {
            foreach (var c in _clients.Values)
                try { await c.CloseAsync(); } catch { }
        }

        private static List<string> Strings(JsonElement arr) =>
            arr.EnumerateArray().Select(e => e.GetString()!).ToList();

        private static List<object?> Args(JsonElement arr)
        {
            var outb = new List<object?>();
            foreach (var e in arr.EnumerateArray())
                outb.Add(e.ValueKind switch
                {
                    JsonValueKind.String => e.GetString(),
                    JsonValueKind.True => true,
                    JsonValueKind.False => false,
                    // Box the long explicitly: a `long ? : double` ternary would
                    // unify to double and turn an integer arg into a float.
                    JsonValueKind.Number => e.TryGetInt64(out var l) ? (object)l : e.GetDouble(),
                    _ => (object?)null,
                });
            return outb;
        }
    }
}
