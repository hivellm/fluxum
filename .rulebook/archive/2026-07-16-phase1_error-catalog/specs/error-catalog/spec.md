# Error Catalog (SPEC-028 ERR)

## ADDED Requirements

### Requirement: Stable per-subsystem error code ranges

The system SHALL define every client-visible error in a catalog of `u16`
codes partitioned into per-subsystem ranges: 1000–1999 protocol/framing
(`PROTO_`), 2000–2999 auth/authorization (`AUTH_`), 3000–3999 SQL, constraint
and transaction (`SQL_`/`TXN_`), 4000–4999 schema/migration/transform
(`SCHEMA_`), 5000–5999 reducer/scheduling (`REDUCER_`), 6000–6999
subscription (`SUB_`), 7000–7999 storage/durability/tiering (`STORAGE_`),
8000–8999 sharding/replication (`CLUSTER_`), 9000–9999 system/limits
(`SYS_`). Each code MUST have a unique canonical `SCREAMING_SNAKE` name.
Codes and names SHALL never be reused, renumbered or renamed once released;
retiring an error retires its number permanently.

#### Scenario: Code uniqueness enforced

Given the catalog registry in fluxum-protocol
When the registry test suite runs
Then it fails if any two entries share a code or a name, or if an entry's code lies outside its declared subsystem range

#### Scenario: Unknown table error carries a stable code

Given a client issuing a OneOffQuery against a table that does not exist
When the server rejects the query
Then the Error frame carries code 3001 and name "SQL_UNKNOWN_TABLE" in every release thereafter

### Requirement: Structured error payload

The wire `Error` message SHALL carry `id: Option<u32>`, `code: u16`,
`name: String`, `message: String`, `severity: error|fatal`,
`retryable: bool`, `retry_after_ms: Option<u32>`, `sqlstate: Option<String>`
and `details: Map<String, Value>`. `severity = fatal` MUST mean the server
will close the connection after the frame. The keys present in `details`
MUST be exactly those documented for the code in the registry, so clients
never parse `message` to extract data.

#### Scenario: Frame-too-large error is structured

Given a client sending a frame larger than max_frame_bytes
When the server rejects it
Then the Error frame has code 1003, name "PROTO_FRAME_TOO_LARGE", severity error, retryable false, and details containing the declared length and the configured maximum as separate keys

#### Scenario: Fatal severity precedes connection close

Given a connection idle beyond the configured idle timeout
When the server emits code 1004 "PROTO_IDLE_TIMEOUT"
Then the frame's severity is fatal and the server closes the connection immediately after writing it

### Requirement: Retry semantics in the registry

Every catalog entry SHALL declare whether the error is retryable, and
transient errors (rate limit, shard handoff, buffer-pool exhaustion,
migration in progress, overload) MUST set `retryable: true`, attaching
`retry_after_ms` whenever the server can estimate a safe delay. A client
retrying only when `retryable` is true and honoring `retry_after_ms` MUST
never worsen the condition that produced the error.

#### Scenario: Rate-limited reducer call advertises retry delay

Given a client exceeding the per-(Identity, reducer) token bucket
When the server rejects the ReducerCall
Then the Error frame has code 5005 "REDUCER_RATE_LIMITED", retryable true, and retry_after_ms populated from the token bucket refill estimate

#### Scenario: Shard handoff is retryable

Given a ReducerCall targeting an entity mid-handoff between shards
When the server cannot serve the request
Then the Error frame has code 8001 "CLUSTER_ENTITY_HANDOFF" with retryable true

### Requirement: PostgreSQL SQLSTATE mapping for SQL errors

Catalog entries in the 3000–3999 range SHALL declare a PostgreSQL-compatible
five-character SQLSTATE, carried in the wire payload's `sqlstate` field
(e.g. 3001 SQL_UNKNOWN_TABLE → 42P01, 3100 SQL_UNIQUE_VIOLATION → 23505,
3200 TXN_CONFLICT → 40001). Errors outside the SQL range MUST send
`sqlstate: None`.

#### Scenario: Unique violation exposes SQLSTATE

Given an insert that violates a unique constraint
When the server rejects the operation
Then the Error frame carries code 3100, name "SQL_UNIQUE_VIOLATION" and sqlstate "23505"

#### Scenario: Non-SQL errors omit SQLSTATE

Given an unauthenticated client sending a Subscribe message
When the server responds with code 2000 "AUTH_REQUIRED"
Then the sqlstate field is absent (None)

### Requirement: Single-source registry drives emission, docs and SDKs

The catalog SHALL exist as exactly one registry table in `fluxum-protocol`
holding, per entry: code, name, subsystem range, default severity, default
retryability, optional SQLSTATE, documented `details` keys and the
human-readable message template. All server emission paths, the generated
error reference documentation and future SDK enums (SPEC-011) MUST derive
from this registry; no code path may emit a `code` value absent from it.

#### Scenario: Emission outside the registry fails tests

Given a server code path constructing an Error frame with a code not present in the registry
When the registry-adherence test suite runs
Then the suite fails identifying the offending emission site

#### Scenario: Error reference generated from the registry

Given the registry with N entries
When the docs generator runs
Then docs/errors.md contains exactly N sections, each with the code, name, message template, details keys and retryability of one entry

### Requirement: Internal error mapping totality

Every `FluxumError` variant in fluxum-core SHALL map deterministically to a
catalog entry when it crosses the wire boundary, and
`FluxumError::Query { code }` MUST carry catalog codes instead of
HTTP-compatible codes. The mapping MUST be exhaustive — adding a
`FluxumError` variant without a catalog mapping fails compilation or tests.

#### Scenario: Buffer pool exhaustion maps to its catalog code

Given a transaction failing with FluxumError::BufferPoolExhausted
When the failure reaches the client
Then the Error frame has code 7002, name "STORAGE_BUFFER_POOL_EXHAUSTED", retryable true, and details containing the pool capacity

### Requirement: HTTP status derivation for Streamable HTTP

For the Streamable HTTP transport, the server SHALL derive the HTTP response
status from the catalog entry via a mapping table defined in SPEC-028
(e.g. AUTH_* → 401/403, PROTO_FRAME_TOO_LARGE → 413, rate limits → 429,
CLUSTER_SHARD_UNAVAILABLE → 503, SYS_INTERNAL → 500), preserving the
externally observable semantics of the current RPC-034 codes.

#### Scenario: Session expiry keeps its HTTP semantics

Given a Streamable HTTP request naming an expired session
When the server rejects it with code 1005 "PROTO_SESSION_EXPIRED"
Then the HTTP response status is 404 as mandated today by RPC-007

## MODIFIED Requirements

### Requirement: Reducer user errors are wrapped with a stable code

A reducer body returning `Err(message)` SHALL reach the caller as a
structured reducer error with code 5001 `REDUCER_USER_ERROR`, the message
string verbatim, and an optional application-defined `app_code: Option<String>`
the reducer MAY attach for client-side matching. Application codes live in
their own string namespace and MUST NOT collide with or shadow catalog
codes. System-caused reducer failures (panic, timeout, unknown reducer,
argument mismatch) SHALL use their own catalog codes instead of 5001.

#### Scenario: Plain Err string is wrapped verbatim

Given a reducer returning Err("saldo insuficiente") with no app code
When the ReducerResult reaches the caller
Then the outcome carries code 5001, name "REDUCER_USER_ERROR", app_code None and message "saldo insuficiente"

#### Scenario: Reducer panic is not a user error

Given a reducer body that panics during execution
When the transaction is rolled back and the caller notified
Then the outcome carries code 5002 "REDUCER_PANIC" and not 5001
