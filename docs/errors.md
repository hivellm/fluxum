# Error reference

Generated from the SPEC-028 catalog (`fluxum-protocol/src/codes.rs`) — do not edit by
hand. Regenerate with `FLUXUM_REGEN_DOCS=1 cargo test -p fluxum-protocol --lib`.

## 1000 `PROTO_MALFORMED`

malformed frame or message body

- severity: `error` · retryable: `false` · HTTP 400

## 1003 `PROTO_FRAME_TOO_LARGE`

frame exceeds the configured maximum size

- severity: `error` · retryable: `false` · HTTP 413
- details keys: `declared_len`, `max_frame_bytes`

## 1004 `PROTO_IDLE_TIMEOUT`

connection idle beyond the configured timeout

- severity: `fatal` · retryable: `true` · HTTP 408

## 1005 `PROTO_SESSION_EXPIRED`

unknown or expired session token

- severity: `fatal` · retryable: `false` · HTTP 404

## 2000 `AUTH_REQUIRED`

authenticate before sending this message

- severity: `error` · retryable: `false` · HTTP 401

## 2001 `AUTH_FAILED`

token validation failed

- severity: `error` · retryable: `false` · HTTP 401

## 3000 `SQL_MALFORMED`

SQL lexical or syntactic error

- severity: `error` · retryable: `false` · HTTP 400
- SQLSTATE: `42601`

## 3001 `SQL_UNKNOWN_TABLE`

query names a table that does not exist

- severity: `error` · retryable: `false` · HTTP 400
- SQLSTATE: `42P01`
- details keys: `table`

## 3002 `SQL_UNKNOWN_COLUMN`

query names a column that does not exist

- severity: `error` · retryable: `false` · HTTP 400
- SQLSTATE: `42703`
- details keys: `table`, `column`

## 3003 `SQL_UNSUPPORTED`

construct outside the supported SQL subset

- severity: `error` · retryable: `false` · HTTP 400
- SQLSTATE: `0A000`

## 3004 `SQL_TYPE_MISMATCH`

literal does not inhabit the column type

- severity: `error` · retryable: `false` · HTTP 400
- SQLSTATE: `42804`

## 3010 `SQL_NO_SPATIAL_INDEX`

spatial predicate on a table without a spatial index

- severity: `error` · retryable: `false` · HTTP 400
- SQLSTATE: `0A000`
- details keys: `table`

## 3100 `SQL_UNIQUE_VIOLATION`

unique constraint violation

- severity: `error` · retryable: `false` · HTTP 400
- SQLSTATE: `23505`
- details keys: `table`, `constraint`

## 3200 `TXN_CONFLICT`

transaction conflict; safe to retry

- severity: `error` · retryable: `true` · HTTP 409
- SQLSTATE: `40001`

## 4000 `SCHEMA_INVALID`

invalid schema or transform declaration

- severity: `error` · retryable: `false` · HTTP 400

## 5000 `REDUCER_UNKNOWN`

unknown reducer name

- severity: `error` · retryable: `false` · HTTP 404
- details keys: `reducer`

## 5001 `REDUCER_USER_ERROR`

the reducer rejected the call

- severity: `error` · retryable: `false` · HTTP 400
- details keys: `app_code`

## 5002 `REDUCER_PANIC`

the reducer panicked; the transaction was rolled back

- severity: `error` · retryable: `false` · HTTP 500
- details keys: `reducer`

## 5003 `REDUCER_BAD_ARGS`

argument count or type mismatch

- severity: `error` · retryable: `false` · HTTP 400
- details keys: `reducer`

## 5004 `REDUCER_SCHEDULE_ONLY`

reducer is schedule-only and not client-callable

- severity: `error` · retryable: `false` · HTTP 403
- details keys: `reducer`

## 5005 `REDUCER_RATE_LIMITED`

per-caller rate limit exceeded

- severity: `error` · retryable: `true` · HTTP 429
- details keys: `reducer`

## 5006 `REDUCER_UNKNOWN_VIEW`

unknown view name

- severity: `error` · retryable: `false` · HTTP 404
- details keys: `view`

## 6000 `SUB_LIMIT_EXCEEDED`

subscription admission cap exceeded

- severity: `error` · retryable: `false` · HTTP 429
- details keys: `limit`

## 6001 `SUB_TABLE_NOT_PUBLIC`

table is not visible to client subscriptions

- severity: `error` · retryable: `false` · HTTP 403
- details keys: `table`

## 7000 `STORAGE_INTERNAL`

internal storage failure

- severity: `error` · retryable: `false` · HTTP 500

## 7001 `STORAGE_PAGE_CORRUPT`

cold page failed integrity verification

- severity: `error` · retryable: `false` · HTTP 500
- details keys: `shard_id`, `table_id`, `page_id`

## 7002 `STORAGE_BUFFER_POOL_EXHAUSTED`

buffer pool has no evictable frame; retry shortly

- severity: `error` · retryable: `true` · HTTP 503
- details keys: `capacity`

## 7003 `STORAGE_SPATIAL_REBUILDING`

spatial index is rebuilding after recovery; retry shortly

- severity: `error` · retryable: `true` · HTTP 503
- details keys: `table`

## 8000 `CLUSTER_SHARD_UNAVAILABLE`

shard temporarily unavailable; retry shortly

- severity: `error` · retryable: `true` · HTTP 503
- details keys: `shard_id`

## 8001 `CLUSTER_ENTITY_HANDOFF`

entity is mid-handoff between shards; retry shortly

- severity: `error` · retryable: `true` · HTTP 503

## 9000 `SYS_INTERNAL`

unexpected internal error

- severity: `error` · retryable: `false` · HTTP 500

## 9001 `SYS_OVERLOADED`

shard-wide admission cap exceeded; retry shortly

- severity: `error` · retryable: `true` · HTTP 503
