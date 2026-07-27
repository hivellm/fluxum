// The shared SDK conformance corpus, run by the Go client (TST-052).
//
// Every SDK executes the SAME declarative corpus (tests/conformance/ at the
// repo root) against the same server build; identical observable results are
// required from all runners. This is the Go runner: an interpreter of the
// corpus step vocabulary, release-blocking when red (SDK-064).
package fluxum_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	fluxum "github.com/hivellm/fluxum-go/fluxum"
)

const awaitTimeout = 5 * time.Second

func repoRoot() string {
	// This file sits at sdks/go/; the repo root is two levels up.
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..")
}

func serverBinary() string {
	name := "fluxum-server"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	return filepath.Join(repoRoot(), "target", "debug", name)
}

func corpusDir() string { return filepath.Join(repoRoot(), "tests", "conformance") }

func serverAvailable() bool {
	_, err := os.Stat(serverBinary())
	return err == nil
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

// server is a spawned fluxum-server on fresh ports + data dir.
type server struct {
	t        *testing.T
	tcpPort  int
	httpPort int
	dir      string
	env      []string
	cmd      *exec.Cmd
}

func startServer(t *testing.T, label string) *server {
	s := &server{
		t:        t,
		tcpPort:  freePort(t),
		httpPort: freePort(t),
	}
	s.dir = t.TempDir()
	s.env = append(os.Environ(),
		"FLUXUM_PROFILE=development",
		"FLUXUM_SERVER_HTTP_PORT="+strconv.Itoa(s.httpPort),
		"FLUXUM_SERVER_TCP_PORT="+strconv.Itoa(s.tcpPort),
		"FLUXUM_STORAGE_DATA_DIR="+s.dir,
		"FLUXUM_STORAGE_COMMIT_LOG_DIR="+filepath.Join(s.dir, "log"),
	)
	s.launch()
	return s
}

func (s *server) launch() {
	cmd := exec.Command(serverBinary())
	cmd.Env = s.env
	if err := cmd.Start(); err != nil {
		s.t.Fatalf("spawn server: %v", err)
	}
	s.cmd = cmd
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", s.tcpPort), 500*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	s.t.Fatalf("server did not bind %d", s.tcpPort)
}

func (s *server) tcpURL() string { return fmt.Sprintf("fluxum://127.0.0.1:%d", s.tcpPort) }

func (s *server) restart() {
	s.stop()
	s.launch()
}

func (s *server) stop() {
	if s.cmd != nil && s.cmd.Process != nil {
		_ = s.cmd.Process.Kill()
		_, _ = s.cmd.Process.Wait()
	}
}

// --- corpus model ----------------------------------------------------------

type manifest struct {
	Tables    map[string]tableSpec `json:"tables"`
	Scenarios []string             `json:"scenarios"`
}

type tableSpec struct {
	PrimaryKey string     `json:"primary_key"`
	Columns    [][]string `json:"columns"`
}

type scenario struct {
	Steps []map[string]map[string]any `json:"steps"`
}

var stringyTypes = map[string]bool{"U64": true, "I64": true, "Timestamp": true, "EntityId": true}

// interp runs one scenario's steps against spawned sessions.
type interp struct {
	t       *testing.T
	m       *manifest
	srv     *server
	clients map[string]*fluxum.Connection
	handles map[string][]int
}

func loadManifest(t *testing.T) *manifest {
	raw, err := os.ReadFile(filepath.Join(corpusDir(), "corpus.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return &m
}

func (in *interp) tableSchemas() []fluxum.TableSchema {
	var schemas []fluxum.TableSchema
	for name, spec := range in.m.Tables {
		name, spec := name, spec
		types := make([]string, len(spec.Columns))
		pkIndex := 0
		for i, col := range spec.Columns {
			types[i] = col[1]
			if col[0] == spec.PrimaryKey {
				pkIndex = i
			}
		}
		pkType := types[pkIndex]
		schemas = append(schemas, fluxum.TableSchema{
			Name: name,
			PkOfRow: func(row []byte) string {
				reader := fluxum.NewRowReader(row)
				var value any
				for i := 0; i <= pkIndex; i++ {
					value, _ = reader.Read(types[i])
				}
				return canonicalStr(value)
			},
			PkOfDelete: func(entry []byte) string {
				v, _ := fluxum.NewRowReader(entry).Read(pkType)
				return canonicalStr(v)
			},
		})
	}
	return schemas
}

func canonicalStr(v any) string { return fmt.Sprintf("%v", v) }

func (in *interp) canonicalRow(table string, row []byte) map[string]any {
	spec := in.m.Tables[table]
	columns := make([]fluxum.Column, len(spec.Columns))
	for i, c := range spec.Columns {
		columns[i] = fluxum.Column{Name: c[0], Type: c[1]}
	}
	decoded, err := fluxum.DecodeRow(row, columns)
	if err != nil {
		in.t.Fatalf("decode %s row: %v", table, err)
	}
	out := map[string]any{}
	for _, c := range spec.Columns {
		out[c[0]] = canonicalize(decoded[c[0]], c[1])
	}
	return out
}

// canonicalize renders a decoded value into its corpus comparison form: 64-bit
// widths become decimal strings; everything else stays as decoded.
func canonicalize(v any, fluxType string) any {
	if stringyTypes[fluxType] {
		return fmt.Sprintf("%v", v)
	}
	return v
}

func numEqual(a int64, actual any) bool {
	switch x := actual.(type) {
	case int64:
		return x == a
	case uint64:
		return x == uint64(a)
	case float64:
		return x == float64(a)
	case string:
		return x == strconv.FormatInt(a, 10)
	}
	return false
}

func (in *interp) client(name any) *fluxum.Connection {
	c := in.clients[fmt.Sprintf("%v", name)]
	if c == nil {
		in.t.Fatalf("step names client %v before its connect step", name)
	}
	return c
}

func (in *interp) resolve(expected any) any {
	if s, ok := expected.(string); ok && strings.HasPrefix(s, "$identity:") {
		return in.client(s[len("$identity:"):]).Identity()
	}
	return expected
}

func (in *interp) matches(expected, actual any) bool {
	if expected == "*" {
		return true
	}
	return equalValues(in.resolve(expected), actual)
}

// equalValues compares a corpus-JSON expected value to a canonicalized actual.
// JSON numbers decode as float64; canonical actuals are float64 (small ints),
// bool, or string (64-bit, identities). Compare via string form for numerics.
func equalValues(expected, actual any) bool {
	switch e := expected.(type) {
	case string:
		if a, ok := actual.(string); ok {
			return e == a
		}
		return e == fmt.Sprintf("%v", actual)
	case bool:
		a, ok := actual.(bool)
		return ok && a == e
	case int64:
		return numEqual(e, actual)
	case float64:
		switch a := actual.(type) {
		case float64:
			return a == e
		case uint64:
			return float64(a) == e
		case int64:
			return float64(a) == e
		case string:
			return a == strconv.FormatFloat(e, 'f', -1, 64)
		}
	}
	return fmt.Sprintf("%v", expected) == fmt.Sprintf("%v", actual)
}

func (in *interp) rowMatches(expected, actual map[string]any) bool {
	for k, v := range expected {
		if !in.matches(v, actual[k]) {
			return false
		}
	}
	return true
}

func (in *interp) run(sc *scenario) {
	ctx := context.Background()
	defer func() {
		for _, c := range in.clients {
			_ = c.Close()
		}
	}()
	for _, step := range sc.Steps {
		for kind, body := range step {
			in.runStep(ctx, kind, body)
		}
	}
}

func (in *interp) runStep(ctx context.Context, kind string, body map[string]any) {
	t := in.t
	switch kind {
	case "connect":
		name := fmt.Sprintf("%v", body["client"])
		var token []byte
		if tok, ok := body["token"]; ok && tok != nil {
			token = []byte(fmt.Sprintf("%v", tok))
		}
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		conn, err := fluxum.Connect(cctx, in.srv.tcpURL(), token, in.tableSchemas())
		cancel()
		if err != nil {
			t.Fatalf("connect %s: %v", name, err)
		}
		in.clients[name] = conn
	case "close":
		_ = in.client(body["client"]).Close()
	case "restart_server":
		in.srv.restart()
	case "subscribe":
		ids, err := in.client(body["client"]).Subscribe(ctx, toStrings(body["queries"]))
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		if as, ok := body["as"].(string); ok {
			in.handles[as] = ids
		}
	case "unsubscribe":
		label := fmt.Sprintf("%v", body["handles"])
		ids := in.handles[label]
		if ids == nil {
			t.Fatalf("unsubscribe names handle %q before its subscribe", label)
		}
		if err := in.client(body["client"]).Unsubscribe(ctx, ids); err != nil {
			t.Fatalf("unsubscribe: %v", err)
		}
	case "call":
		err := in.client(body["client"]).CallReducer(ctx, fmt.Sprintf("%v", body["reducer"]), toAny(body["args"]))
		if expect, ok := body["expect_error"].(map[string]any); ok {
			in.matchError(err, expect)
		} else if err != nil {
			t.Fatalf("call %v: %v", body["reducer"], err)
		}
	case "subscribe_error":
		_, err := in.client(body["client"]).Subscribe(ctx, toStrings(body["queries"]))
		in.matchError(err, body["expect_error"].(map[string]any))
	case "call_until_error":
		client := in.client(body["client"])
		attempts := numToInt(body["attempts"])
		expect := body["expect_error"].(map[string]any)
		for i := 0; i < attempts; i++ {
			if err := client.CallReducer(ctx, fmt.Sprintf("%v", body["reducer"]), toAny(body["args"])); err != nil {
				in.matchError(err, expect)
				return
			}
		}
		t.Fatalf("all %d calls succeeded; expected an error", attempts)
	case "await_row":
		in.awaitCount(body, 1, true)
	case "await_gone":
		in.awaitCount(body, 0, false)
	case "await_count":
		in.awaitCount(body, numToInt(body["count"]), false)
	case "expect_cache":
		in.expectCache(body)
	case "expect_distinct_identities":
		names := toStrings(body["clients"])
		seen := map[string]bool{}
		for _, n := range names {
			id := in.client(n).Identity()
			if seen[id] {
				t.Fatalf("identities collide at %s", n)
			}
			seen[id] = true
		}
	default:
		t.Fatalf("unknown step %q", kind)
	}
}

func (in *interp) awaitCount(body map[string]any, want int, atLeast bool) {
	client := in.client(body["client"])
	table := fmt.Sprintf("%v", body["table"])
	where, _ := body["where"].(map[string]any)
	deadline := time.Now().Add(awaitTimeout)
	for {
		matching := 0
		for _, row := range client.Cache().Rows(table) {
			if in.rowMatches(where, in.canonicalRow(table, row)) {
				matching++
			}
		}
		if (atLeast && matching >= want) || (!atLeast && matching == want) {
			return
		}
		if time.Now().After(deadline) {
			in.t.Fatalf("await %s %v: %d matching, wanted %d after %s", table, where, matching, want, awaitTimeout)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func (in *interp) expectCache(body map[string]any) {
	client := in.client(body["client"])
	table := fmt.Sprintf("%v", body["table"])
	expected := body["rows"].([]any)
	var actual []map[string]any
	for _, row := range client.Cache().Rows(table) {
		actual = append(actual, in.canonicalRow(table, row))
	}
	remaining := append([]map[string]any(nil), actual...)
	for _, wantRaw := range expected {
		want := wantRaw.(map[string]any)
		idx := -1
		for i, row := range remaining {
			if in.rowMatches(want, row) {
				idx = i
				break
			}
		}
		if idx < 0 {
			in.t.Fatalf("%s: no cached row matches %v; cache: %v", table, want, remaining)
		}
		remaining = append(remaining[:idx], remaining[idx+1:]...)
	}
	if len(remaining) != 0 {
		in.t.Fatalf("%s: unexpected extra rows: %v", table, remaining)
	}
}

func (in *interp) matchError(err error, expect map[string]any) {
	if err == nil {
		in.t.Fatalf("operation succeeded; the scenario expected an error")
	}
	fe, ok := err.(*fluxum.Error)
	if !ok {
		in.t.Fatalf("error is not a *fluxum.Error: %v", err)
	}
	if c, ok := expect["contains"].(string); ok && !strings.Contains(fe.Message, c) {
		in.t.Fatalf("%q lacks %q", fe.Message, c)
	}
	if codeRaw, ok := expect["code"]; ok {
		if code := numToInt(codeRaw); fe.Code != code {
			in.t.Fatalf("code %d != %d (%s)", fe.Code, code, fe.Message)
		}
	}
	if cat, ok := expect["catalog"].(string); ok && fe.Catalog != cat {
		in.t.Fatalf("catalog %s != %s", fe.Catalog, cat)
	}
}

func toStrings(v any) []string {
	items, _ := v.([]any)
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = fmt.Sprintf("%v", it)
	}
	return out
}

func toAny(v any) []any {
	items, _ := v.([]any)
	return items
}

func numToInt(v any) int {
	switch n := v.(type) {
	case int64:
		return int(n)
	case float64:
		return int(n)
	case int:
		return n
	}
	return 0
}

// normalize turns `encoding/json`'s json.Number into int64 (integral) or
// float64 recursively, so a corpus arg `1` reaches the wire as an integer
// (the server's u32/u64 param), not a float — which is what every other
// runner's JSON parser does natively.
func normalize(v any) any {
	switch t := v.(type) {
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}
		f, _ := t.Float64()
		return f
	case []any:
		for i := range t {
			t[i] = normalize(t[i])
		}
		return t
	case map[string]any:
		for k := range t {
			t[k] = normalize(t[k])
		}
		return t
	default:
		return v
	}
}

func TestConformance(t *testing.T) {
	if !serverAvailable() {
		t.Skip("no fluxum-server binary — run: cargo build -p fluxum-server")
	}
	m := loadManifest(t)
	for _, name := range m.Scenarios {
		name := name
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(corpusDir(), "scenarios", name+".json"))
			if err != nil {
				t.Fatal(err)
			}
			dec := json.NewDecoder(strings.NewReader(string(raw)))
			dec.UseNumber()
			var sc scenario
			if err := dec.Decode(&sc); err != nil {
				t.Fatal(err)
			}
			for _, step := range sc.Steps {
				for _, body := range step {
					normalize(body)
				}
			}
			srv := startServer(t, name)
			defer srv.stop()
			in := &interp{t: t, m: m, srv: srv, clients: map[string]*fluxum.Connection{}, handles: map[string][]int{}}
			in.run(&sc)
		})
	}
}
