# 06 — A05:2025 Injection

The strongest area. Fluxum's query surface is effectively injection-proof by
construction; there is no work to do here beyond keeping the corpus green.
Recorded as a positive finding so the analysis is balanced and the guarantee is
documented.

---

## F-013 — Query grammar is closed and typed; no string-interpolation path (INFO / positive)

**Evidence.**

- The lexer rejects every out-of-subset byte with a named diagnostic and never
  panics: `;` (`crates/fluxum-core/src/sql/lexer.rs:147`), `"`/`` ` `` quoted
  identifiers (`:148-152`), `--`/`/`/`#` comments (`:121-123`, `:153`), `!=`
  (`:173`). Query text is hard-capped at 8 KiB (`:16`, `:80-85`), and non-finite
  / overflowing float literals are rejected (`:244-246`).
- Literals are coerced to the schema column type
  (`crates/fluxum-core/src/sql/mod.rs:918-966`); there is **no concatenation
  between query text and evaluation** — a query can only fail to compile, never
  change meaning (`mod.rs:1-3`, `:19-28`). The parser rejects DML/DDL/JOIN/UNION/
  CTE/OR/NOT/LIKE by name (`parse.rs:108-135`).
- Queries are compiled once at subscribe time; commits evaluate the compiled
  plan, never the SQL string (`mod.rs:1-3`).
- The no-panic guarantee is pinned by an **injection corpus** (`mod.rs:27-28`).

**Impact.** No SQL-injection, no second-order injection via stored values (values
never re-enter as query text), no comment/stacked-query smuggling. There is no
other injection interpreter in the request path (no shell-out, no template
engine, no `eval`).

**Confidence: High.**

---

## Note — resource/complexity limits belong to Insecure Design, not Injection

The query surface *is* injection-safe, but it is **not** resource-bounded: there
is no mandatory or maximum `LIMIT`, and no query execution timeout. A
`SELECT * FROM big_table` compiles to a full scan and returns everything. That is
a denial-of-service / insecure-design problem, not an injection problem, and is
tracked as **F-014** in `07-insecure-design-and-exceptional-conditions.md`.

---

## Positives (A05) — additional hardening already present

- `OFFSET` is deliberately banned in favor of keyset pagination
  (`parse.rs:254-260`), removing a common deep-pagination DoS *and* a class of
  logic bugs.
- `IN`-list index expansion is capped at 128 probes (`mod.rs:504`, `:547-551`).
- `deny_unknown_fields` on config and typed argument decoding (see F-013 family)
  extend the "reject, don't coerce" discipline beyond SQL.
