# SPEC-019 — Lexical Full-Text Search (minimal, native)

| | |
|---|---|
| **Status** | Draft |
| **Phase / tasks** | Phase 2 (inverted index) · Phase 4 (query operator) ([DAG](../DAG.md)) |
| **PRD requirements** | FR-30, FR-31 (extends); new: FR-95 (native lexical full-text), FR-96 (BM25 ranking) |
| **Requirement prefix** | `FTS-` |
| **Source** | New (Fluxum-native). Corrects the PRD non-goal that delegated full-text to "Nexus and Vectorizer": **Nexus is a graph database and Vectorizer is vector/semantic search — neither provides lexical full-text**, so it is a real gap Fluxum must close minimally. |

Keywords **MUST**, **MUST NOT**, **SHALL**, **SHOULD**, **MAY** are RFC 2119. Requirement IDs
`FTS-xxx` are stable. Priority tags: `[P0]` MVP · `[P1]` competitive launch · `[P2]` post-launch.

## 1. Scope & positioning

Conventional relational databases handle keyword search poorly (`LIKE '%…%'` is an unrankable full
scan), forcing an external engine (Meilisearch, Elasticsearch). Fluxum closes this **minimally and
natively**, mirroring how geospatial search is already a first-class native index
([SPEC-008](SPEC-008-spatial-indexes.md)) rather than an afterthought: a **positional inverted
index** maintained in the same commit-merge pipeline as the B-tree and spatial indexes, and a
**`MATCH` query operator** routed through that index — with **BM25 relevance ranking**.

This is a **lexical keyword** capability, deliberately bounded so it does not turn Fluxum into a
search platform:

- **In scope (this spec):** an analyzer pipeline (Unicode tokenization → case-folding → stop-words →
  stemming, per language), a positional posting-list index, boolean `AND`-of-terms matching, **phrase
  match**, **term-prefix (typeahead)**, and **BM25** ranking for the initial snapshot.
- **Out of scope (delegated / deferred):** vector/semantic search → the family's **Vectorizer**;
  fuzzy/typo tolerance (trigram/edit-distance), synonyms, faceting, and multi-field scoring DSLs →
  explicit non-goals ([PRD §8](../PRD.md)); a live-ranked top-N *subscription* → materialized views
  ([SPEC-005](SPEC-005-subscriptions.md) SUB-050, P2).

The design copies the **spatial precedent** exactly (native index + dedicated query predicate +
snapshot-ranked / boolean-live), so it introduces no new architectural shape.

## 2. Declaration

- **FTS-001** [P0] `#[fluxum::table]` SHALL recognise a table-level `#[fulltext(col, …)]` attribute
  declaring a full-text index over one `String` (or `Vec<String>`) column, alongside the existing
  `#[index(...)]`/`#[spatial(...)]` attributes ([SPEC-001](SPEC-001-data-model.md) DM-020):

  ```rust
  #[fluxum::table(public)]
  #[index(btree(category, price))]
  #[fulltext(name, language = "en")]
  #[fulltext(description, language = "en")]
  pub struct Item {
      #[primary_key] #[auto_inc] pub id: u64,
      pub category: String,
      pub name: String,
      pub description: String,
      pub price: Decimal,
      pub listed_at: Timestamp,
  }
  ```

  Attribute arguments: `language = "en"|"pt"|…` (selects the analyzer; default `"simple"` = tokenize
  + lowercase only, no stop-words/stemming), `stop_words = true|false` (default `true` when a real
  language is set), `stemming = true|false` (default `true` when a real language is set). A
  `#[fulltext]` column MUST be `String` or `Vec<String>`; any other type is a compile-time error.

- **FTS-002** [P0] A `#[fulltext]` column MAY also be a `#[normalize(string)]` column
  ([SPEC-017](SPEC-017-column-transforms.md)); normalization runs first (on write), then the
  analyzer tokenizes the normalized value. A `#[fulltext]` column MUST NOT be `#[encrypted]` (a
  ciphertext is untokenizable — compile-time error, consistent with CT-013).

## 3. Analyzer pipeline

- **FTS-010** [P0] Indexing and querying SHALL pass text through the **same** deterministic analyzer
  so index terms and query terms match by construction. The pipeline is:

  1. **Tokenize** — Unicode text segmentation (word boundaries, `unicode-segmentation`), yielding
     tokens with byte/char **positions** (positions are required for phrase match, FTS-032).
  2. **Case-fold** — Unicode lowercase/case-fold.
  3. **Stop-words** — drop language stop-words when enabled (FTS-001).
  4. **Stem** — reduce to a root per language when enabled (e.g. `rust-stemmers`/Snowball);
     `"espadas" → "espad"`.

  The analyzer SHALL be pure and versioned; its identifier SHALL be recorded in the index metadata
  so a query and the stored postings are guaranteed to have used the same pipeline
  (`__schema_meta__`, [SPEC-010](SPEC-010-schema-migration.md)).

## 4. Inverted index structure & maintenance

- **FTS-020** [P0] For each `#[fulltext]` column the runtime SHALL maintain a **positional inverted
  index**: `term → posting list`, where each posting is `(primary_key, term_frequency, positions)`.
  It SHALL additionally maintain the corpus statistics BM25 needs (FTS-040): per-document length
  (token count of the indexed field) and per-term document frequency (`df`), plus the document
  count `N`.

  ```rust
  pub struct FullTextIndex {
      column: u16,
      analyzer: AnalyzerId,
      postings: BTreeMap<Term, PostingList>,   // Term = analyzed token bytes; BTreeMap enables prefix scans (FTS-031)
      doc_len: HashMap<PkBytes, u32>,          // token count per document (BM25 length norm)
      total_docs: u64,
      total_len: u64,                          // for average doc length
  }
  // PostingList: pk -> (tf, positions)
  ```

- **FTS-021** [P0] Index maintenance SHALL ride the commit merge exactly like the B-tree/spatial
  indexes ([SPEC-002](SPEC-002-storage-engine.md); `crates/fluxum-core/src/index/mod.rs`): on insert,
  analyze the field and add postings + update stats; on delete, remove them; on update, delete-old +
  insert-new — all applied to a private pre-swap copy and published in the same atomic `TableState`
  swap, so readers never see a partially updated index. Rollback discards `TxState`. The index MUST
  be **bit-identical to a fresh rebuild** over committed rows (STG-007 rule-2 invariant), verified by
  a `verify_index_integrity` analogue.

- **FTS-022** [P1] The inverted index SHALL follow the tiered-storage rule
  ([SPEC-015](SPEC-015-tiered-storage.md) TIER-050): postings MUST be pageable and evictable under
  the memory budget — an FTS index over a large corpus MUST NOT be assumed RAM-resident. As with
  spatial indexes, the FTS index MAY be rebuilt from committed rows on recovery behind a "rebuilding"
  gate returning `503` until ready (`index/mod.rs:171`); durable persistence of postings is a
  tiered-storage optimization, not required for v1 correctness.

## 5. Query surface

- **FTS-030** [P0] The subscription/query SQL subset ([SPEC-005](SPEC-005-subscriptions.md) SUB-010,
  extended by [SPEC-018](SPEC-018-query-planner.md)) SHALL support a `MATCH` predicate over a
  `#[fulltext]` column, combinable with the ordinary `AND` filters:

  ```sql
  SELECT * FROM Item
  WHERE description MATCH 'espada rara'          -- AND-of-terms by default (both must occur)
    AND category = 'weapon' AND price <= 500     -- ordinary index/residual filters (SPEC-018)
  ORDER BY SCORE DESC LIMIT 20
  ```

  Evaluation SHALL route through the inverted index (never a full scan), intersecting the posting
  lists of the query terms and applying the ordinary filters as residual conditions. Like spatial
  predicates (SUB-011), a `MATCH` query has no full-scan fallback.

- **FTS-031** [P0] **Term prefix / typeahead.** A trailing `*` on a term (`MATCH 'esp*'`) SHALL match
  every indexed term with that prefix, resolved by a range scan over the `postings` `BTreeMap`
  (`[esp, esq)`) and unioning their posting lists. Only a trailing-`*` prefix is supported (no
  infix/suffix wildcards).

- **FTS-032** [P0] **Phrase match.** A quoted term group (`MATCH '"espada rara"'`) SHALL match only
  documents where the analyzed tokens occur **adjacently and in order**, resolved using the stored
  positions (FTS-020). Mixed queries are allowed (`MATCH '"espada rara" lendaria'` = phrase AND term).

- **FTS-033** [P0] A `MATCH` on a column without a `#[fulltext]` index SHALL be rejected at compile
  time; unsupported search constructs (fuzzy `~`, boolean `OR`/`NOT` inside `MATCH`, field-boost
  syntax) SHALL be rejected with the existing 400 diagnostic (SUB-012). `OR` across whole conditions
  remains unsupported per SUB-012.

## 6. Ranking (BM25)

- **FTS-040** [P0] Documents matching a `MATCH` predicate SHALL be scored with **BM25** using the
  maintained corpus statistics (FTS-020): for each query term, `idf(term) · (tf · (k1+1)) / (tf + k1
  · (1 − b + b · |d|/avgdl))`, summed over query terms. Parameters `k1` (default 1.2) and `b`
  (default 0.75) SHALL be configurable per index. Phrase matches score as a single synthetic term
  over the phrase's occurrences.

- **FTS-041** [P0] `ORDER BY SCORE [DESC]` SHALL sort the initial snapshot by BM25 score; `LIMIT n`
  with `ORDER BY SCORE` SHALL return the top-N by score. Consistent with SUB-013 and SPEC-018 QP-021,
  ranking and top-N apply to **`InitialData` / one-off queries only**. The computed score MAY be
  surfaced to clients as a synthetic `_score: f32` projection column (opt-in via
  `SELECT *, SCORE`).

- **FTS-042** [P0] **Live diffs are boolean.** For `TxUpdate` fan-out, a committed delta row SHALL be
  matched against a `MATCH` plan by re-analyzing the row's field and testing the boolean predicate
  (all terms / phrase present) — **no re-ranking**. This is the FTS analogue of value-level / spatial
  pruning ([SPEC-005](SPEC-005-subscriptions.md) SUB-023): a `MATCH` plan registers its query terms
  in a term→plans pruning index so only plans whose terms appear in the delta row are evaluated,
  keeping fan-out O(P_matched + S_matched) (SUB-022). Clients needing a live-ranked ordered window
  re-sort locally, or use a materialized view (SUB-050, P2).

## 7. Introspection, migration & SDK

- **FTS-050** [P0] `TableSchema`/`ColumnSchema` and `GET /schema` SHALL expose each full-text index:
  the column, analyzer id (language, stop-words, stemming flags), and BM25 params — never any corpus
  content. The schema hash SHALL incorporate the analyzer id so drift is detectable
  ([SPEC-011](SPEC-011-sdk-codegen.md)).

- **FTS-051** [P1] Changing a column's analyzer (language/stemming/stop-words) SHALL be an
  auto-diffable schema change requiring an index rebuild; `__schema_meta__` records the analyzer id,
  and a binary started against postings built by a different analyzer SHALL rebuild (or abort with a
  descriptive error) rather than mixing analyzers ([SPEC-010](SPEC-010-schema-migration.md)).

- **FTS-052** [P1] SDK codegen SHALL render the `MATCH`/phrase/prefix query surface and the optional
  `_score` column so all SDKs ([SPEC-011](SPEC-011-sdk-codegen.md)) can issue full-text queries and
  read scores.

## 8. Acceptance criteria

1. **Analyzer determinism (FTS-010):** the same text produces identical analyzed tokens at index time
   and query time across the supported languages; stemming/stop-words behave per the configured
   language; the `"simple"` analyzer does tokenize+lowercase only.
2. **Index correctness & maintenance (FTS-020/021):** after a random sequence of inserts/updates/
   deletes, the inverted index (postings, positions, `df`, doc lengths) is bit-identical to a fresh
   rebuild over committed rows; rollback leaves it untouched.
3. **Boolean + prefix + phrase (FTS-030/031/032):** `MATCH 'a b'` returns exactly the documents
   containing both analyzed terms; `MATCH 'a*'` returns exactly those with a term prefixed `a`;
   `MATCH '"a b"'` returns only documents with `a` immediately followed by `b`; combined with `AND`
   ordinary filters, results equal the intersection.
4. **BM25 ranking (FTS-040/041):** for a known corpus, `ORDER BY SCORE DESC LIMIT n` returns the
   documents in the exact BM25 order a reference implementation produces (parameterized by k1/b); the
   `_score` projection matches.
5. **Index-routed, no full scan (FTS-030):** a `MATCH` query's rows-scanned counter is bounded by the
   matched posting lists, not the table size; a `MATCH` on an unindexed column fails to compile.
6. **Live boolean fan-out (FTS-042):** with N clients on distinct `MATCH` queries, a 1-row commit
   evaluates only the plans whose terms occur in the delta row (term-pruning verified via counters);
   subscription correctness holds — client caches equal the server boolean match set after each
   commit (joint with SUB property test).
7. **Tiered/evictable (FTS-022):** an FTS index over a corpus larger than the buffer-pool budget is
   served correctly with postings paged in on demand; recovery rebuilds the index behind the 503 gate.
8. **Introspection & analyzer drift (FTS-050/051):** `/schema` reports the analyzer and BM25 params;
   starting against postings from a different analyzer rebuilds or aborts, never silently mixes.
