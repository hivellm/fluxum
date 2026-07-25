package fluxum

import (
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

// The context-aware Fluxum client (SPEC-011 SDK-062).
//
// One Connection drives a session over FluxRPC/TCP: authenticate, subscribe
// (each query's InitialData lands in a local row cache), call reducers, and
// receive TxUpdate diffs on the same socket. Every blocking call takes a
// context.Context. On connection loss the client reconnects, re-authenticates,
// resubscribes every active query and reconciles its cache (SDK-047).

// TableSchema is a table's cache hooks: its name and how to derive a primary
// key from a full row's bytes and from a delete entry's bytes.
type TableSchema struct {
	Name       string
	PkOfRow    func([]byte) string
	PkOfDelete func([]byte) string
}

// Error is a server-reported failure. Code is the stable SPEC-028 catalog code
// (the portable assertion); Catalog is the SCREAMING_SNAKE name for an Error
// frame (empty for a reducer rejection); AppCode is the reducer's optional
// application code.
type Error struct {
	Code    int
	Message string
	Catalog string
	AppCode string
}

func (e *Error) Error() string {
	return fmt.Sprintf("fluxum: error %d: %s", e.Code, e.Message)
}

// Cache is the client's row cache: per table, a pk -> row-bytes map,
// materialized from the rows any active subscription currently holds.
type Cache struct {
	mu     sync.Mutex
	rows   map[string]map[string][]byte
	owners map[string]map[string]map[int]struct{}
	known  map[string]struct{}
}

func newCache(tables []TableSchema) *Cache {
	c := &Cache{
		rows:   map[string]map[string][]byte{},
		owners: map[string]map[string]map[int]struct{}{},
		known:  map[string]struct{}{},
	}
	for _, t := range tables {
		c.rows[t.Name] = map[string][]byte{}
		c.owners[t.Name] = map[string]map[int]struct{}{}
		c.known[t.Name] = struct{}{}
	}
	return c
}

// Rows returns every currently-cached row of table, as raw FluxBIN bytes.
func (c *Cache) Rows(table string) [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][]byte, 0, len(c.rows[table]))
	for _, row := range c.rows[table] {
		out = append(out, row)
	}
	return out
}

func (c *Cache) insert(table string, queryID int, pk string, row []byte) {
	if _, ok := c.known[table]; !ok {
		return
	}
	c.rows[table][pk] = row
	if c.owners[table][pk] == nil {
		c.owners[table][pk] = map[int]struct{}{}
	}
	c.owners[table][pk][queryID] = struct{}{}
}

func (c *Cache) delete(table string, queryID int, pk string) {
	if _, ok := c.known[table]; !ok {
		return
	}
	owners := c.owners[table][pk]
	if owners == nil {
		return
	}
	delete(owners, queryID)
	if len(owners) == 0 {
		delete(c.owners[table], pk)
		delete(c.rows[table], pk)
	}
}

func (c *Cache) dropQuery(queryID int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for table, owners := range c.owners {
		for pk, set := range owners {
			delete(set, queryID)
			if len(set) == 0 {
				delete(owners, pk)
				delete(c.rows[table], pk)
			}
		}
	}
}

func (c *Cache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for table := range c.rows {
		c.rows[table] = map[string][]byte{}
		c.owners[table] = map[string]map[int]struct{}{}
	}
}

type sub struct {
	sql     string
	queryID int
}

// Connection is a live client session. Construct with Connect.
type Connection struct {
	host, port string
	token      []byte
	schemas    map[string]TableSchema
	cache      *Cache

	mu       sync.Mutex
	conn     net.Conn
	frames   *frameReader
	nextID   uint32
	pending  map[uint32]chan serverMessage
	subs     []sub
	identity string
	closed   bool
	done     chan struct{}
}

// Connect opens and authenticates a session. url is fluxum://host:port or a
// bare host:port (TCP). The context bounds the initial connect + handshake.
func Connect(ctx context.Context, url string, token []byte, tables []TableSchema) (*Connection, error) {
	host, port, err := parseURL(url)
	if err != nil {
		return nil, err
	}
	schemas := make(map[string]TableSchema, len(tables))
	for _, t := range tables {
		schemas[t.Name] = t
	}
	c := &Connection{
		host:     host,
		port:     port,
		token:    token,
		schemas:  schemas,
		cache:    newCache(tables),
		nextID:   1,
		pending:  map[uint32]chan serverMessage{},
		identity: strings.Repeat("00", 32),
		done:     make(chan struct{}),
	}
	if err := c.establish(ctx); err != nil {
		return nil, err
	}
	go c.readLoop()
	return c, nil
}

// Cache returns the row cache.
func (c *Connection) Cache() *Cache { return c.cache }

// Identity returns this session's 256-bit identity as hex.
func (c *Connection) Identity() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.identity
}

// Close ends the session; the reconnect loop stops.
func (c *Connection) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	conn := c.conn
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
	<-c.done
	return nil
}

func (c *Connection) establish(ctx context.Context) error {
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(c.host, c.port))
	if err != nil {
		return err
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	c.mu.Lock()
	c.conn = conn
	c.frames = newFrameReader()
	subs := c.subs
	c.mu.Unlock()

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}

	// Authenticate inline (the reader goroutine is not looping yet).
	authID := c.allocID()
	if err := c.sendRaw("Authenticate", []any{authID, c.token, nil, nil, nil}); err != nil {
		return err
	}
	for {
		msg, err := c.readInline()
		if err != nil {
			return err
		}
		if msg.tag == "Error" && msgID(msg) == int(authID) {
			return errorFrom(msg)
		}
		if msg.tag == "AuthResult" && toInt(msg.payload[0]) == int(authID) {
			c.mu.Lock()
			c.identity = hexOf(msg.payload[1])
			c.mu.Unlock()
			break
		}
	}

	// Resubscribe the replay set inline against the fresh session (SDK-047).
	if len(subs) > 0 {
		c.cache.clear()
		sqls := make([]string, len(subs))
		for i, s := range subs {
			sqls[i] = s.sql
		}
		c.mu.Lock()
		c.subs = nil
		c.mu.Unlock()
		if err := c.resubscribeInline(sqls); err != nil {
			return err
		}
	}
	_ = conn.SetDeadline(time.Time{})
	return nil
}

func (c *Connection) readInline() (serverMessage, error) {
	for {
		c.mu.Lock()
		body, ok, err := c.frames.nextBody()
		conn := c.conn
		c.mu.Unlock()
		if err != nil {
			return serverMessage{}, err
		}
		if ok {
			return decodeMessage(body)
		}
		chunk := make([]byte, 65536)
		n, rerr := conn.Read(chunk)
		if n > 0 {
			c.mu.Lock()
			c.frames.push(chunk[:n])
			c.mu.Unlock()
		}
		if rerr != nil {
			return serverMessage{}, rerr
		}
	}
}

func (c *Connection) readLoop() {
	defer close(c.done)
	backoff := 200 * time.Millisecond
	chunk := make([]byte, 65536)
	for {
		c.mu.Lock()
		conn := c.conn
		closed := c.closed
		c.mu.Unlock()
		if closed {
			return
		}
		n, err := conn.Read(chunk)
		if n > 0 {
			c.mu.Lock()
			c.frames.push(chunk[:n])
			for {
				body, ok, ferr := c.frames.nextBody()
				if ferr != nil || !ok {
					break
				}
				msg, derr := decodeMessage(body)
				if derr == nil {
					c.route(msg)
				}
			}
			c.mu.Unlock()
			backoff = 200 * time.Millisecond
		}
		if err != nil {
			c.mu.Lock()
			if c.closed {
				c.mu.Unlock()
				return
			}
			c.failPending()
			c.mu.Unlock()
			// Reconnect: re-establish (auth + resubscribe + reconcile).
			for {
				c.mu.Lock()
				closed := c.closed
				c.mu.Unlock()
				if closed {
					return
				}
				time.Sleep(backoff)
				backoff = minDur(backoff*2, 5*time.Second)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				eerr := c.establish(ctx)
				cancel()
				if eerr == nil {
					backoff = 200 * time.Millisecond
					break
				}
			}
		}
	}
}

// route runs with c.mu held.
func (c *Connection) route(msg serverMessage) {
	if msg.tag == "TxUpdate" {
		c.applyTxUpdate(msg)
		return
	}
	id := msgID(msg)
	if id >= 0 {
		if ch, ok := c.pending[uint32(id)]; ok {
			select {
			case ch <- msg:
			default:
			}
		}
	}
}

// failPending runs with c.mu held.
func (c *Connection) failPending() {
	for _, ch := range c.pending {
		select {
		case ch <- serverMessage{tag: "__disconnected__"}:
		default:
		}
	}
}

func (c *Connection) allocID() uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := c.nextID
	c.nextID++
	return id
}

func (c *Connection) sendRaw(tag string, payload []any) error {
	frame, err := encodeMessage(tag, payload)
	if err != nil {
		return err
	}
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	_, werr := conn.Write(frame)
	return werr
}

func (c *Connection) request(ctx context.Context, tag string, payloadFn func(uint32) []any) (serverMessage, error) {
	id := c.allocID()
	ch := make(chan serverMessage, 4)
	c.mu.Lock()
	c.pending[id] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}()
	if err := c.sendRaw(tag, payloadFn(id)); err != nil {
		return serverMessage{}, err
	}
	select {
	case <-ctx.Done():
		return serverMessage{}, ctx.Err()
	case msg := <-ch:
		if msg.tag == "__disconnected__" {
			return serverMessage{}, fmt.Errorf("fluxum: disconnected while awaiting a reply")
		}
		return msg, nil
	}
}

// Subscribe registers queries; awaits each InitialData, applies it to the
// cache, and returns the server-assigned query_ids in query order (RPC-022).
func (c *Connection) Subscribe(ctx context.Context, queries []string) ([]int, error) {
	id := c.allocID()
	ch := make(chan serverMessage, len(queries)+2)
	c.mu.Lock()
	c.pending[id] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}()
	if err := c.sendRaw("Subscribe", []any{id, toAnyStrings(queries)}); err != nil {
		return nil, err
	}
	var queryIDs []int
	for len(queryIDs) < len(queries) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case msg := <-ch:
			if msg.tag == "__disconnected__" {
				return nil, fmt.Errorf("fluxum: disconnected during subscribe")
			}
			if msg.tag == "Error" {
				return nil, errorFrom(msg)
			}
			if msg.tag != "InitialData" {
				continue
			}
			ids, err := c.applyInitialData(msg)
			if err != nil {
				return nil, err
			}
			queryIDs = append(queryIDs, ids...)
		}
	}
	c.mu.Lock()
	for i, qid := range queryIDs {
		if i < len(queries) {
			c.subs = append(c.subs, sub{sql: queries[i], queryID: qid})
		}
	}
	c.mu.Unlock()
	return queryIDs, nil
}

func (c *Connection) resubscribeInline(queries []string) error {
	id := c.allocID()
	if err := c.sendRaw("Subscribe", []any{id, toAnyStrings(queries)}); err != nil {
		return err
	}
	var queryIDs []int
	for len(queryIDs) < len(queries) {
		msg, err := c.readInline()
		if err != nil {
			return err
		}
		if msg.tag == "Error" && msgID(msg) == int(id) {
			return errorFrom(msg)
		}
		if msg.tag == "TxUpdate" {
			c.mu.Lock()
			c.applyTxUpdate(msg)
			c.mu.Unlock()
			continue
		}
		if msg.tag != "InitialData" || toInt(msg.payload[0]) != int(id) {
			continue
		}
		ids, err := c.applyInitialData(msg)
		if err != nil {
			return err
		}
		queryIDs = append(queryIDs, ids...)
	}
	c.mu.Lock()
	for i, qid := range queryIDs {
		if i < len(queries) {
			c.subs = append(c.subs, sub{sql: queries[i], queryID: qid})
		}
	}
	c.mu.Unlock()
	return nil
}

func (c *Connection) applyInitialData(msg serverMessage) ([]int, error) {
	// InitialData: [id, schema_version, tables, ...]
	tables, ok := msg.payload[2].([]any)
	if !ok {
		return nil, fmt.Errorf("fluxum: InitialData has no tables array")
	}
	var ids []int
	c.cache.mu.Lock()
	defer c.cache.mu.Unlock()
	for _, entry := range tables {
		qid, table, inserts, deletes, err := tableUpdate(entry)
		if err != nil {
			return nil, err
		}
		ids = append(ids, qid)
		c.applyDiff(table, qid, inserts, deletes)
	}
	return ids, nil
}

// Unsubscribe drops the subscriptions whose query_ids are given (RPC-024).
func (c *Connection) Unsubscribe(ctx context.Context, queryIDs []int) error {
	if err := c.sendRaw("Unsubscribe", []any{c.allocID(), toAnyInts(queryIDs)}); err != nil {
		return err
	}
	for _, qid := range queryIDs {
		c.cache.dropQuery(qid)
	}
	wanted := map[int]struct{}{}
	for _, q := range queryIDs {
		wanted[q] = struct{}{}
	}
	c.mu.Lock()
	kept := c.subs[:0]
	for _, s := range c.subs {
		if _, drop := wanted[s.queryID]; !drop {
			kept = append(kept, s)
		}
	}
	c.subs = kept
	c.mu.Unlock()
	return nil
}

// CallReducer calls reducer name with args; returns on commit, an *Error on
// rejection (RPC-021/031).
func (c *Connection) CallReducer(ctx context.Context, name string, args []any) error {
	msg, err := c.request(ctx, "ReducerCall", func(id uint32) []any {
		return []any{id, name, nil, args, nil}
	})
	if err != nil {
		return err
	}
	if msg.tag == "Error" {
		return errorFrom(msg)
	}
	if msg.tag == "ReducerResult" {
		// ["Ok", nil] or ["Err", [code, app_code, message]]
		if outcome, ok := msg.payload[1].([]any); ok && len(outcome) >= 2 {
			if tag, _ := outcome[0].(string); tag == "Err" {
				if e, ok := outcome[1].([]any); ok && len(e) >= 3 {
					return &Error{Code: toInt(e[0]), Message: strOf(e[2]), AppCode: strOf(e[1])}
				}
			}
		}
		return nil
	}
	return &Error{Code: 0, Message: "unexpected reply to reducer call: " + msg.tag}
}

// applyTxUpdate runs with c.mu held.
func (c *Connection) applyTxUpdate(msg serverMessage) {
	if len(msg.payload) < 6 {
		return
	}
	tables, ok := msg.payload[5].([]any)
	if !ok {
		return
	}
	c.cache.mu.Lock()
	defer c.cache.mu.Unlock()
	for _, entry := range tables {
		qid, table, inserts, deletes, err := tableUpdate(entry)
		if err != nil {
			continue
		}
		c.applyDiff(table, qid, inserts, deletes)
	}
}

// applyDiff runs with cache.mu held: deletes before inserts so an update (a
// delete + insert of the same pk) leaves the new row (SPEC-005).
func (c *Connection) applyDiff(table string, qid int, inserts, deletes [][]byte) {
	schema, ok := c.schemas[table]
	if !ok {
		return
	}
	for _, entry := range deletes {
		c.cache.delete(table, qid, schema.PkOfDelete(entry))
	}
	for _, row := range inserts {
		c.cache.insert(table, qid, schema.PkOfRow(row), row)
	}
}

// --- helpers ---------------------------------------------------------------

func parseURL(url string) (host, port string, err error) {
	rest := url
	for _, scheme := range []string{"fluxum://", "tcp://"} {
		if strings.HasPrefix(rest, scheme) {
			rest = rest[len(scheme):]
			break
		}
	}
	i := strings.LastIndex(rest, ":")
	if i <= 0 || i == len(rest)-1 {
		return "", "", fmt.Errorf("fluxum: expected host:port, got %q", url)
	}
	return rest[:i], rest[i+1:], nil
}

func msgID(msg serverMessage) int {
	switch msg.tag {
	case "AuthResult", "ReducerResult", "InitialData":
		if len(msg.payload) > 0 {
			return toInt(msg.payload[0])
		}
	case "Error":
		if len(msg.payload) > 0 && msg.payload[0] != nil {
			return toInt(msg.payload[0])
		}
	}
	return -1
}

func errorFrom(msg serverMessage) *Error {
	// Error: [id, code, name, message, ...]
	p := msg.payload
	e := &Error{}
	if len(p) > 1 {
		e.Code = toInt(p[1])
	}
	if len(p) > 2 {
		e.Catalog = strOf(p[2])
	}
	if len(p) > 3 {
		e.Message = strOf(p[3])
	}
	return e
}

func hexOf(v any) string {
	if b, ok := v.([]byte); ok {
		return hex.EncodeToString(b)
	}
	return strOf(v)
}

func strOf(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

func toAnyStrings(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

func toAnyInts(is []int) []any {
	out := make([]any, len(is))
	for i, n := range is {
		out[i] = n
	}
	return out
}

func minDur(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
