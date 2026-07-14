# 07 — Protocol & Client SDKs

## Transport layer

SpacetimeDB uses two transport channels:

| Channel | Protocol | Used for |
|---------|----------|----------|
| HTTP POST | REST over HTTPS | Reducer calls (one-shot), SQL queries, schema publishing |
| WebSocket | `v1.bin.spacetimedb` subprotocol | Subscriptions, real-time updates, streaming reducer calls |

All binary messages use **BSATN** encoding. A JSON variant (`v1.json.spacetimedb`) exists for
debugging but is not used in production clients.

---

## HTTP API endpoints

### Database operations
```
POST /database/:name/call/:reducer          → call a reducer
GET  /database/:name/schema                 → get module schema (JSON)
POST /database/:name/sql                    → execute raw SQL (read-only)
POST /database/:name/subscribe              → legacy HTTP polling (deprecated)
```

### Module management (CLI / admin)
```
POST /database                              → publish a new module
PUT  /database/:name                        → update an existing module
GET  /database/:name/logs                   → stream server logs
POST /identity                              → create a new identity
GET  /identity/:identity/databases          → list databases for identity
```

### Authentication
All requests must include:
```
Authorization: Bearer <token>
```
Where `<token>` is a JWT issued by an OIDC provider (or the SpacetimeDB test token for development).
The token is verified and resolved to an `Identity` (256-bit hash).

---

## WebSocket protocol

### Connection establishment
```
GET /database/:name/subscribe
Upgrade: websocket
Sec-WebSocket-Protocol: v1.bin.spacetimedb
Authorization: Bearer <token>
```

On successful upgrade, the server sends:
```
IdentityToken {
  identity: Identity,       // 256-bit client identity
  token: String,            // refreshed JWT
  connection_id: ConnectionId
}
```

### Message framing
- All WebSocket messages are binary frames
- Each frame contains exactly one BSATN-encoded message
- Messages are tagged with a discriminant byte (enum variant tag)

### Client → Server message types
```
ServerMessage {
  | Subscribe(Subscribe)
  | Unsubscribe(Unsubscribe)
  | CallReducer(CallReducer)
  | SubscribeSingle(SubscribeSingle)    // subscribe to one query
  | UnsubscribeMulti(UnsubscribeMulti)
  | OneOffQuery(OneOffQuery)            // one-shot SQL read, no subscription
}
```

### Server → Client message types
```
ClientMessage {
  | InitialSubscription(InitialSubscription)   // initial data for subscription
  | TransactionUpdate(TransactionUpdate)       // incremental diff on commit
  | TransactionUpdateLight(TransactionUpdateLight) // compact variant
  | IdentityToken(IdentityToken)               // auth confirmation
  | OneOffQueryResponse(OneOffQueryResponse)   // SQL query result
}
```

---

## BSATN encoding details

**Binary SpacetimeDB Algebraic Type Notation:**

```
Encoding rules:
  bool        → 0x00 (false) | 0x01 (true)
  u8/i8       → 1 byte, little-endian
  u16/i16     → 2 bytes, little-endian
  u32/i32     → 4 bytes, little-endian
  u64/i64     → 8 bytes, little-endian
  u128/i128   → 16 bytes, little-endian
  f32         → 4 bytes IEEE 754 LE
  f64         → 8 bytes IEEE 754 LE
  String      → u32 length prefix + UTF-8 bytes
  Bytes       → u32 length prefix + raw bytes
  Vec<T>      → u32 count prefix + N × encode(T)
  Option<T>   → 0x00 (None) | 0x01 + encode(T) (Some)
  SumType     → u8 tag + encode(variant payload)
  ProductType → sequential encode of each field (no separators)
  Identity    → 32 bytes raw
  ConnectionId→ 16 bytes raw
  Timestamp   → i64 microseconds since Unix epoch
```

**Comparison with alternatives:**
| Format | Size | Speed | Schema | Notes |
|--------|------|-------|--------|-------|
| BSATN | ★★★★★ | ★★★★★ | Required | No self-description; requires schema on both sides |
| Protobuf | ★★★★ | ★★★★ | Required | More overhead; field tags add bytes |
| MessagePack | ★★★ | ★★★★ | Optional | Self-describing; larger for typed data |
| JSON | ★★ | ★★ | Optional | Human-readable; very large |
| FlatBuffers | ★★★★★ | ★★★★★ | Required | Zero-copy reads; complex schema management |

BSATN is essentially a minimal binary serialization optimized for the SATS type system.
No field names, no type tags at the value level — the schema provides all needed context.

---

## SDK code generation

After publishing a module, the CLI generates strongly-typed client bindings:
```bash
spacetime generate --lang typescript --out-dir ./src/module_bindings
spacetime generate --lang rust --out-dir ./src/bindings
spacetime generate --lang csharp --out-dir ./Assets/Bindings
```

Generated code includes:
- Typed table classes with BSATN encode/decode
- Typed reducer call functions
- Typed subscription builder with correct query strings
- Event callback interfaces (`onPlayerInserted`, `onPlayerDeleted`, etc.)

This is one of SpacetimeDB's strongest UX features: **zero boilerplate for the client developer**.
The generated SDK handles all protocol details; the developer works only with typed objects.

---

## Identity system

### Identity
- 256-bit value derived from the user's OIDC credential
- Stable across connections and sessions — the same user always has the same `Identity`
- Used as a primary key for user-owned data (e.g., `Player` table keyed by `Identity`)

### ConnectionId
- 128-bit ephemeral value assigned per WebSocket connection
- Changes on every reconnect
- Used to track active connections (presence, connection state)

### Token flow
```
1. User authenticates with OIDC provider → JWT
2. Client sends JWT on WebSocket connect
3. SpacetimeDB verifies JWT, derives Identity
4. SpacetimeDB issues its own refreshed token (shorter-lived)
5. __connect__ reducer fires with { sender: Identity, connection_id: ConnectionId }
```

---

## Implications for UzDB

| SpacetimeDB decision | UzDB consideration |
|----------------------|--------------------|
| HTTP + WebSocket dual channel | Adopt — HTTP for setup/admin, WS for game data |
| BSATN binary encoding | Adopt or derive — define `UzBSATN` based on the same principles |
| SDK code generation from schema | Adopt — critical DX feature; generate TML client bindings |
| 256-bit stable Identity | Adopt — game player identity must be stable across sessions |
| ConnectionId for ephemeral presence | Adopt — needed for online/offline detection |
| OIDC-only auth | Reconsider — UzDB should support simple token auth for game clients |
| Typed subscription builders | Adopt — generated code eliminates query string bugs |
