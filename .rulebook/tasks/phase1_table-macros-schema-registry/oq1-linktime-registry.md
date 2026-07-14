# OQ-1 — Link-time registry mechanism: `inventory` (decided)

**Question (PRD OQ-1):** which distributed-collection mechanism assembles the
schema at link time — `inventory` or `linkme`?

**Decision:** `inventory` 0.3.

## How it is wired

- `#[fluxum::table]` (crates/fluxum-macros/src/table.rs) emits
  `::fluxum_core::schema::inventory::submit! { TableDef(&__FLUXUM_SCHEMA) }`
  next to the generated `static TableSchema`.
- `fluxum_core::schema` re-exports `inventory`, so application module crates
  register without depending on `inventory` directly.
- `Schema::assemble()` (crates/fluxum-core/src/schema/registry.rs) iterates
  `inventory::iter::<TableDef>` and validates the collected set (DM-040);
  `ServerBuilder::build()` (later phase) calls it before opening any
  transport. No source-file scanning, no dynamic loading (FR-03).

## Why `inventory` over `linkme`

- Registration rides on portable constructor functions (`ctor`-style), which
  behave identically across ELF (Linux), Mach-O (macOS), and COFF/MSVC
  (Windows) — matching the 3-OS CI matrix. `linkme`'s link-section slices
  have historically needed platform-specific section attributes and
  dead-strip workarounds (`#[used(linker)]`, Mach-O `-dead_strip` edge
  cases).
- No central `#[distributed_slice]` declaration item to coordinate between
  the macro crate and core; `submit!` is self-contained per expansion.
- Cost accepted: registrations materialize via pre-`main` constructors
  (negligible, once per process) instead of a pure static slice; collection
  order is linker-defined, so `Schema` sorts tables by name (`BTreeMap`).

## Caveat (must-know for module authors)

A crate that is linked but never *referenced* is dropped by the linker
together with its registrations. Application binaries must reference their
module crates (e.g. `use my_module;`). Documented on
`registry::registered_tables()`.

## Verification

- Cross-crate collection is exercised in-tree: `crates/fluxum-testmod`
  declares `AuditEvent`; `crates/fluxum-macros/tests/schema_registry.rs`
  (a second crate) declares six more tables and asserts all seven appear in
  one `Schema::assemble()` — SPEC-001 acceptance 2, on all three CI OSes.
- Duplicate table names across compilation units abort assembly with a
  descriptive error: `crates/fluxum-macros/tests/registry_duplicate.rs`.
