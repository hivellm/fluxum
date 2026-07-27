//! Positional inverted index for native lexical full-text search
//! (SPEC-019 FTS-010/020/021/022) — the storage-layer foundation.
//!
//! A `#[fulltext(col, …)]` column gets a [`FullTextIndexState`]: a
//! deterministic analyzer ([`Analyzer`]) turns a document's text into
//! `(term, position)` pairs, and the index keeps **positional posting lists**
//! (`term → { pk → positions }`) plus the corpus statistics BM25 needs
//! (per-document length, per-term document frequency, document count, total
//! length). Maintenance rides the commit merge exactly like the B-tree and
//! spatial indexes (private pre-swap copy, atomic publish, rollback discards
//! `TxState`), and the STG-007 rule-2 invariant holds: after any sequence of
//! commits the index holds precisely the committed rows' postings and
//! statistics. Paged storage has no meaningful structural equality, so that
//! is checked by *contents* — [`FullTextIndexState::entries`] against a
//! fresh rebuild — rather than by comparing layouts.
//!
//! The `MATCH` query operator, BM25 ranking, and subscription integration are
//! the sibling phase-4 task; this module is the index and its statistics
//! only.
//!
//! # Storage (TIER-051)
//!
//! Postings and document lengths live in the paged store, so a full-text
//! index faults and evicts under `memory.budget` like every other index
//! family instead of pinning RSS with the corpus. Two prefix-separated
//! keyspaces share one [`PagedTree`]:
//!
//! - `0x00 ++ term ++ 0x00 ++ pk → positions` — one flat key per
//!   `(term, document)` pair. Terms are maximal alphanumeric runs
//!   ([`tokenize`]), so a `0x00` separator is unambiguous, and byte order
//!   puts a term's whole posting list in one contiguous range: term lookup
//!   and the FTS-031 prefix union are both key-range scans.
//! - `0x01 ++ pk → doc_len` — the per-document length BM25 needs.
//!
//! `total_len` and `total_docs` stay resident: two counters, `O(1)` each,
//! that would otherwise cost a full scan per query.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::error::{FluxumError, Result};
use crate::store::TableId;
use crate::store::pager::{PagedTree, Pager};
use crate::store::row::{PkBytes, Row, RowValue};

/// Analyzer pipeline version (FTS-010). Bumping it changes tokenization,
/// folding, or stemming and therefore invalidates every stored
/// [`AnalyzerId`] — a schema-meta mismatch forces a rebuild (SPEC-010).
pub const ANALYZER_VERSION: u8 = 1;

/// The analyzer language (FTS-010): selects the stop-word set and stemmer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Language {
    /// Tokenize + case-fold only — no stop-words, no stemming. Language
    /// agnostic; the safe default for identifiers and mixed content.
    Simple,
    /// English stop-words and a light deterministic English stemmer.
    English,
}

/// A deterministic text-analysis pipeline (FTS-010): Unicode tokenization
/// with positions → case-fold → per-language stop-words → per-language
/// stemming. Determinism is the load-bearing property — the same text always
/// yields the same terms, so removal (re-analyze the old row) and rebuild
/// (re-analyze every row) reproduce the index exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Analyzer {
    /// Stop-word set + stemmer selection.
    pub language: Language,
    /// Drop language stop-words (no-op for [`Language::Simple`]).
    pub stop_words: bool,
    /// Apply the language stemmer (no-op for [`Language::Simple`]).
    pub stemming: bool,
}

/// A versioned analyzer identity (FTS-010, FTS-051): stable across restarts,
/// stored in `__schema_meta__` so a pipeline change (via [`ANALYZER_VERSION`]
/// or a config change) is detected and forces a rebuild.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AnalyzerId(pub u32);

impl Analyzer {
    /// The `simple` analyzer: tokenize + lowercase, nothing else.
    pub const fn simple() -> Self {
        Self {
            language: Language::Simple,
            stop_words: false,
            stemming: false,
        }
    }

    /// This analyzer's versioned identity (FTS-051).
    pub fn id(&self) -> AnalyzerId {
        let lang = match self.language {
            Language::Simple => 0u32,
            Language::English => 1u32,
        };
        // version(8) | lang(4) | stop(1) | stem(1)
        let bits = (u32::from(ANALYZER_VERSION) << 6)
            | (lang << 2)
            | (u32::from(self.stop_words && self.language != Language::Simple) << 1)
            | u32::from(self.stemming && self.language != Language::Simple);
        AnalyzerId(bits)
    }

    /// Analyze `text` into `(term, position)` pairs in document order
    /// (FTS-010). Positions count every token (including dropped stop-words),
    /// so phrase distance survives stop-word removal; the returned vector
    /// holds only the kept terms. Its length is **not** the document length —
    /// [`analyze_doc`](Self::analyze_doc) reports that.
    pub fn analyze(&self, text: &str) -> Vec<(String, u32)> {
        let mut out = Vec::new();
        for (pos, raw) in tokenize(text).into_iter().enumerate() {
            let pos = u32::try_from(pos).unwrap_or(u32::MAX);
            let folded = case_fold(raw);
            if self.stop_words
                && self.language == Language::English
                && is_english_stop_word(&folded)
            {
                continue;
            }
            let term = if self.stemming && self.language == Language::English {
                stem_english(&folded)
            } else {
                folded
            };
            if term.is_empty() {
                continue;
            }
            out.push((term, pos));
        }
        out
    }

    /// Analyze into `(terms, doc_len)` where `doc_len` is the number of kept
    /// terms — the BM25 document length (FTS-020).
    pub fn analyze_doc(&self, text: &str) -> (Vec<(String, u32)>, u32) {
        let terms = self.analyze(text);
        let len = u32::try_from(terms.len()).unwrap_or(u32::MAX);
        (terms, len)
    }
}

/// Unicode tokenization (FTS-010): maximal runs of alphanumeric characters,
/// in document order. Everything else is a separator. Deterministic and
/// language-agnostic; folding and stemming run downstream.
fn tokenize(text: &str) -> Vec<&str> {
    let mut tokens = Vec::new();
    let mut start = None;
    for (i, ch) in text.char_indices() {
        if ch.is_alphanumeric() {
            start.get_or_insert(i);
        } else if let Some(s) = start.take() {
            tokens.push(&text[s..i]);
        }
    }
    if let Some(s) = start {
        tokens.push(&text[s..]);
    }
    tokens
}

/// Case-fold a token (FTS-010). Unicode-aware lowercasing.
fn case_fold(token: &str) -> String {
    token.to_lowercase()
}

/// A small, sorted English stop-word set (FTS-010). Kept deliberately
/// compact and stable — it is part of the [`ANALYZER_VERSION`] contract.
const ENGLISH_STOP_WORDS: &[&str] = &[
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "from", "has", "have", "he",
    "her", "his", "in", "into", "is", "it", "its", "of", "on", "or", "she", "that", "the", "their",
    "then", "there", "these", "they", "this", "to", "was", "were", "which", "who", "will", "with",
];

fn is_english_stop_word(term: &str) -> bool {
    ENGLISH_STOP_WORDS.binary_search(&term).is_ok()
}

/// A light, deterministic English stemmer (FTS-010): common suffix stripping
/// (plurals, `-ing`, `-ed`, `-ly`). Not a full Porter stemmer — it trades
/// linguistic completeness for a small, stable, obviously-deterministic rule
/// set, which is what the storage foundation needs (ranking quality is the
/// phase-4 concern). Rules are ordered; only the first match applies.
fn stem_english(word: &str) -> String {
    let strip = |w: &str, suffix: &str, min_stem: usize| -> Option<String> {
        w.strip_suffix(suffix)
            .filter(|stem| stem.len() >= min_stem)
            .map(str::to_owned)
    };
    if let Some(stem) = word.strip_suffix("ies").filter(|s| s.len() >= 2) {
        return format!("{stem}y");
    }
    if let Some(stem) = strip(word, "sses", 2) {
        return format!("{stem}ss");
    }
    for (suffix, min_stem) in [("ing", 4), ("edly", 3), ("ed", 3), ("ly", 3), ("es", 3)] {
        if let Some(stem) = strip(word, suffix, min_stem) {
            return stem;
        }
    }
    // Plural `-s` (but never `-ss`), on a long enough word.
    if word.ends_with('s') && !word.ends_with("ss") && word.len() > 3 {
        return word[..word.len() - 1].to_owned();
    }
    word.to_owned()
}

/// BM25 `k1` (term-frequency saturation), the standard default (FTS-040).
pub const BM25_K1: f64 = 1.2;
/// BM25 `b` (length normalization), the standard default (FTS-040).
pub const BM25_B: f64 = 0.75;

/// One analyzed `MATCH` item (SPEC-019 FTS-030/031/032).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FtsItem {
    /// A plain term: the document must contain it.
    Term(String),
    /// A trailing-`*` prefix (typeahead): any indexed term extending it.
    Prefix(String),
    /// A quoted phrase: `(term, analyzed position)` pairs — the position
    /// deltas encode stop-word gaps so adjacency matches index semantics.
    Phrase(Vec<(String, u32)>),
}

/// A compiled `MATCH` predicate over one `#[fulltext]` column (FTS-030):
/// AND-of-items, analyzer-normalized at compile time so index-time and
/// query-time analysis can never disagree (FTS-010).
#[derive(Debug, Clone, PartialEq)]
pub struct FtsQuery {
    /// The matched column's ordinal.
    pub column: u16,
    /// The AND-ed items (non-empty; enforced by [`FtsQuery::parse`]).
    pub items: Vec<FtsItem>,
    /// The column's analyzer — used to re-analyze delta rows for the
    /// FTS-042 boolean live match.
    pub analyzer: Analyzer,
    /// The raw MATCH string as written — handed to ReadPath plugins
    /// (SPEC-020 PLG-040/041) so a model/retriever sees the user's query,
    /// not the analyzed terms.
    pub raw: String,
}

impl FtsQuery {
    /// Parse and analyze a raw `MATCH` string: bare terms AND together, a
    /// trailing `*` makes a prefix, a `"…"` group is a phrase. Unsupported
    /// search constructs — fuzzy `~`, `OR`/`NOT` inside the match, `^`
    /// field boosts, non-trailing wildcards — are rejected with a wire-ready
    /// 400 (FTS-033).
    pub fn parse(raw: &str, column: u16, analyzer: Analyzer) -> Result<Self> {
        let unsupported = |detail: &str| {
            FluxumError::query(
                fluxum_protocol::codes::SQL_UNSUPPORTED,
                format!("unsupported MATCH syntax: {detail} (FTS-033)"),
            )
        };
        for (marker, name) in [
            ('~', "fuzzy `~`"),
            ('^', "field boost `^`"),
            ('(', "grouping"),
            (')', "grouping"),
        ] {
            if raw.contains(marker) {
                return Err(unsupported(name));
            }
        }
        let mut items = Vec::new();
        let mut rest = raw.trim();
        while !rest.is_empty() {
            if let Some(after) = rest.strip_prefix('"') {
                // Quoted phrase group.
                let Some(end) = after.find('"') else {
                    return Err(unsupported("unterminated phrase quote"));
                };
                let phrase = &after[..end];
                if phrase.contains('*') {
                    return Err(unsupported("wildcards inside a phrase"));
                }
                let mut terms = analyzer.analyze(phrase);
                match terms.len() {
                    0 => return Err(unsupported("empty phrase")),
                    1 => items.push(FtsItem::Term(terms.swap_remove(0).0)),
                    _ => items.push(FtsItem::Phrase(terms)),
                }
                rest = after[end + 1..].trim_start();
                continue;
            }
            let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
            let word = &rest[..end];
            rest = rest[end..].trim_start();
            if word.eq_ignore_ascii_case("OR") || word.eq_ignore_ascii_case("NOT") {
                return Err(unsupported(
                    "boolean OR/NOT inside MATCH (AND-of-terms only)",
                ));
            }
            if let Some(prefix) = word.strip_suffix('*') {
                if prefix.contains('*') {
                    return Err(unsupported("only a single trailing `*` is supported"));
                }
                // Analyze the prefix WITHOUT stemming semantics changing it:
                // the prefix matches raw indexed terms, so it is case-folded
                // only (a stem could be shorter than what the user typed).
                let folded = case_fold(prefix);
                if folded.is_empty() {
                    return Err(unsupported("empty prefix before `*`"));
                }
                items.push(FtsItem::Prefix(folded));
                continue;
            }
            if word.contains('*') {
                return Err(unsupported("only a trailing `*` wildcard is supported"));
            }
            for (term, _) in analyzer.analyze(word) {
                items.push(FtsItem::Term(term));
            }
        }
        if items.is_empty() {
            return Err(unsupported(
                "the query analyzed to no terms (stop-words only or empty)",
            ));
        }
        Ok(Self {
            column,
            items,
            analyzer,
            raw: raw.to_owned(),
        })
    }

    /// FTS-042: boolean re-analysis of one delta row — every item present
    /// (phrases positionally). No scoring; live diffs are never ranked.
    pub fn matches_row(&self, row: &Row) -> bool {
        let text = match row.values().get(usize::from(self.column)) {
            Some(RowValue::Str(s)) => s.clone(),
            Some(RowValue::Optional(Some(inner))) => match inner.as_ref() {
                RowValue::Str(s) => s.clone(),
                _ => return false,
            },
            Some(RowValue::List(values)) => {
                let mut parts = Vec::with_capacity(values.len());
                for value in values {
                    match value {
                        RowValue::Str(s) => parts.push(s.as_str()),
                        _ => return false,
                    }
                }
                parts.join(" ")
            }
            _ => return false,
        };
        self.matches_text(&text)
    }

    /// Whether `text` (analyzed with this query's analyzer) satisfies every
    /// item — the same predicate [`FullTextIndexState::search`] evaluates
    /// through the index.
    pub fn matches_text(&self, text: &str) -> bool {
        let mut positions: HashMap<&str, Vec<u32>> = HashMap::new();
        let analyzed = self.analyzer.analyze(text);
        for (term, pos) in &analyzed {
            positions.entry(term.as_str()).or_default().push(*pos);
        }
        self.items.iter().all(|item| match item {
            FtsItem::Term(term) => positions.contains_key(term.as_str()),
            FtsItem::Prefix(prefix) => positions
                .keys()
                .any(|term| term.starts_with(prefix.as_str())),
            FtsItem::Phrase(terms) => {
                let Some(((first, first_pos), rest)) = terms.split_first() else {
                    return false;
                };
                let Some(anchors) = positions.get(first.as_str()) else {
                    return false;
                };
                anchors.iter().any(|anchor| {
                    rest.iter().all(|(term, pos)| {
                        positions.get(term.as_str()).is_some_and(|list| {
                            list.binary_search(&(anchor + (pos - first_pos))).is_ok()
                        })
                    })
                })
            }
        })
    }

    /// The pruning terms this query registers in the term→plans index
    /// (FTS-042): every plain term, a phrase's first term, and each prefix
    /// (matched by prefix against delta terms).
    pub fn pruning_terms(&self) -> (Vec<String>, Vec<String>) {
        let mut terms = Vec::new();
        let mut prefixes = Vec::new();
        for item in &self.items {
            match item {
                FtsItem::Term(term) => terms.push(term.clone()),
                FtsItem::Phrase(phrase) => {
                    if let Some((first, _)) = phrase.first() {
                        terms.push(first.clone());
                    }
                }
                FtsItem::Prefix(prefix) => prefixes.push(prefix.clone()),
            }
        }
        (terms, prefixes)
    }
}

/// One document's entry in a term's posting list (FTS-020): the positions at
/// which the term occurs, in ascending document order. The term frequency
/// `tf` is `positions.len()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Posting {
    /// Occurrence positions, ascending (document-order token indices).
    pub positions: Vec<u32>,
}

impl Posting {
    /// Term frequency: the number of occurrences (FTS-020).
    pub fn tf(&self) -> u32 {
        u32::try_from(self.positions.len()).unwrap_or(u32::MAX)
    }
}

/// A positional inverted index over one `#[fulltext(col, …)]` column
/// (SPEC-019 FTS-020), maintained inside [`crate::store::TableState`] by the
/// commit merge — the same lifecycle as the B-tree and spatial indexes.
///
/// # Readiness (FTS-022)
///
/// Full-text indexes are not persisted; after crash recovery they are rebuilt
/// from the recovered rows. A slot in the **rebuilding** state answers every
/// query with `STORAGE_FULLTEXT_REBUILDING` until the rebuild publishes the
/// ready state, mirroring the spatial-index readiness gate.
#[derive(Debug, Clone)]
pub struct FullTextIndexState {
    /// Indexed text column ordinal (FTS-001).
    column: u16,
    /// The deterministic analyzer (FTS-010).
    analyzer: Analyzer,
    /// Postings and document lengths, prefix-separated (see the module
    /// docs). Copy-on-write, so the commit merge's pre-swap clone stays
    /// `O(1)` in the corpus exactly as the persistent maps it replaced.
    store: PagedTree,
    /// Sum of `doc_len` over all documents — the BM25 `avgdl` numerator.
    total_len: u64,
    /// Number of indexed documents — the `0x01` keyspace's cardinality,
    /// counted rather than scanned.
    total_docs: usize,
    /// Paging handles, carried so `fresh_like`/`rebuilding_like` can build
    /// an empty index of the same configuration on the same page file.
    pager: Arc<Pager>,
    table_id: TableId,
    /// FTS-022 gate: `false` while the index awaits its post-recovery
    /// rebuild — queries return `STORAGE_FULLTEXT_REBUILDING`, commit-merge
    /// maintenance is skipped (the rebuild recreates it from the rows).
    ready: bool,
}

impl FullTextIndexState {
    /// An empty, ready full-text index over `column` with `analyzer`,
    /// storing its postings in `table_id`'s page file.
    pub(crate) fn new(
        column: u16,
        analyzer: Analyzer,
        pager: &Arc<Pager>,
        table_id: TableId,
    ) -> Result<Self> {
        Ok(Self {
            column,
            analyzer,
            store: PagedTree::create(pager, table_id, true)?,
            total_len: 0,
            total_docs: 0,
            pager: Arc::clone(pager),
            table_id,
            ready: true,
        })
    }

    /// An empty index with this index's exact configuration — the rebuild
    /// seed for the STG-007 rule-2 integrity check and FTS-022 rebuilds.
    pub(crate) fn fresh_like(&self) -> Result<Self> {
        Self::new(self.column, self.analyzer, &self.pager, self.table_id)
    }

    /// This configuration in the FTS-022 rebuilding state: empty, not ready.
    pub(crate) fn rebuilding_like(&self) -> Result<Self> {
        Ok(Self {
            ready: false,
            ..self.fresh_like()?
        })
    }

    /// Whether the index serves queries (FTS-022).
    pub(crate) fn is_ready(&self) -> bool {
        self.ready
    }

    /// The FTS-022 not-ready guard consumed by the phase-4 `MATCH` query
    /// surface (the readiness machinery lands with the storage foundation so
    /// recovery can gate queries the moment the operator exists).
    #[allow(dead_code)]
    pub(crate) fn check_ready(&self) -> Result<()> {
        if self.ready {
            Ok(())
        } else {
            Err(FluxumError::query(
                fluxum_protocol::codes::STORAGE_FULLTEXT_REBUILDING,
                "full-text index not ready",
            ))
        }
    }

    /// The analyzer's versioned identity, written to `__schema_meta__` by the
    /// phase-4 schema-meta writer so a pipeline change forces a rebuild
    /// (FTS-051).
    #[allow(dead_code)]
    pub(crate) fn analyzer_id(&self) -> AnalyzerId {
        self.analyzer.id()
    }

    /// The indexed column's ordinal.
    pub(crate) fn column(&self) -> u16 {
        self.column
    }

    /// Read the indexed column's text, concatenating `Vec<String>` elements
    /// with a token-breaking gap. A `NULL` (`Optional(None)`) document
    /// contributes no terms.
    fn document_text(&self, row: &Row) -> Result<Option<String>> {
        match row.values().get(usize::from(self.column)) {
            None | Some(RowValue::Optional(None)) => Ok(None),
            Some(RowValue::Str(s)) => Ok(Some(s.clone())),
            Some(RowValue::Optional(Some(inner))) => match inner.as_ref() {
                RowValue::Str(s) => Ok(Some(s.clone())),
                other => Err(Self::not_text(self.column, other)),
            },
            Some(RowValue::List(items)) => {
                let mut parts = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        RowValue::Str(s) => parts.push(s.as_str()),
                        other => return Err(Self::not_text(self.column, other)),
                    }
                }
                // A space separates elements so terms never merge across them.
                Ok(Some(parts.join(" ")))
            }
            Some(other) => Err(Self::not_text(self.column, other)),
        }
    }

    fn not_text(ordinal: u16, got: &RowValue) -> FluxumError {
        FluxumError::Storage(format!(
            "internal invariant violated: full-text column ordinal {ordinal} is not a \
             String/Vec<String> column (got {got:?}); the registry validates FTS-002"
        ))
    }

    // --- paged keyspaces ---------------------------------------------------

    /// The posting key for `(term, pk)` — see the module docs.
    fn posting_key(term: &str, pk: &PkBytes) -> Vec<u8> {
        let mut key = Self::term_prefix(term);
        key.extend_from_slice(pk.as_bytes());
        key
    }

    /// The exclusive-of-nothing prefix every posting key of `term` starts
    /// with: `0x00 ++ term ++ 0x00`. Scanning from here yields exactly that
    /// term's posting list.
    fn term_prefix(term: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(term.len() + 2);
        key.push(KEY_POSTING);
        key.extend_from_slice(term.as_bytes());
        key.push(0x00);
        key
    }

    /// The prefix shared by every posting key whose term starts with
    /// `prefix` — the FTS-031 scan bound. No trailing separator: the scan
    /// spans all terms extending `prefix`, itself included.
    fn term_prefix_scan(prefix: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(prefix.len() + 1);
        key.push(KEY_POSTING);
        key.extend_from_slice(prefix.as_bytes());
        key
    }

    /// The document-length key for `pk`.
    fn doc_len_key(pk: &PkBytes) -> Vec<u8> {
        let mut key = Vec::with_capacity(pk.as_bytes().len() + 1);
        key.push(KEY_DOC_LEN);
        key.extend_from_slice(pk.as_bytes());
        key
    }

    /// Visit every posting under `prefix` as `(pk, posting)`, in key order.
    /// `f` returns `false` to stop early.
    fn for_each_posting(
        &self,
        prefix: &[u8],
        f: &mut dyn FnMut(PkBytes, Posting) -> Result<bool>,
    ) -> Result<()> {
        self.store.scan(prefix, None, &mut |key, value| {
            if !key.starts_with(prefix) {
                return Ok(false); // past the prefix: keys are byte-ordered
            }
            // Everything after the term's separator is the PK. A term is a
            // run of alphanumerics, so the *first* `0x00` past the tag byte
            // is that separator — searching from the back would be wrong,
            // since PK bytes may themselves contain `0x00`.
            let Some(sep) = key[1..].iter().position(|b| *b == 0x00).map(|i| i + 1) else {
                return Err(FluxumError::Storage(
                    "malformed paged full-text posting key".into(),
                ));
            };
            f(
                PkBytes::from_bytes(key[sep + 1..].to_vec()),
                decode_posting(value)?,
            )
        })?;
        Ok(())
    }

    // --- maintenance (FTS-021) ---------------------------------------------

    /// Add `row`'s document to the index (commit merge, insert side —
    /// FTS-021). Skipped while rebuilding. Superseded pages go to `sup`.
    pub(crate) fn insert_row(&mut self, row: &Row, pk: PkBytes, sup: &mut Vec<u64>) -> Result<()> {
        if !self.ready {
            return Ok(());
        }
        let Some(text) = self.document_text(row)? else {
            // NULL document: still a document with length 0 (BM25 counts it).
            self.put_doc_len(&pk, 0, sup)?;
            return Ok(());
        };
        let (terms, doc_len) = self.analyzer.analyze_doc(&text);
        let mut per_term: BTreeMap<String, Vec<u32>> = BTreeMap::new();
        for (term, pos) in terms {
            per_term.entry(term).or_default().push(pos);
        }
        for (term, positions) in per_term {
            self.store.insert_cow(
                &Self::posting_key(&term, &pk),
                &encode_posting(&Posting { positions }),
                sup,
            )?;
        }
        self.put_doc_len(&pk, doc_len, sup)?;
        Ok(())
    }

    /// Write `pk`'s document length, keeping the resident counters in step
    /// with the keyspace (re-indexing the same PK must not double-count).
    fn put_doc_len(&mut self, pk: &PkBytes, len: u32, sup: &mut Vec<u64>) -> Result<()> {
        let key = Self::doc_len_key(pk);
        match self.store.get(&key)? {
            Some(old) => self.total_len -= u64::from(decode_doc_len(&old)?),
            None => self.total_docs += 1,
        }
        self.store.insert_cow(&key, &len.to_le_bytes(), sup)?;
        self.total_len += u64::from(len);
        Ok(())
    }

    /// Remove `row`'s document from the index (commit merge, delete side —
    /// FTS-021). Re-analyzes the old row (the analyzer is deterministic, so
    /// this reproduces exactly what was inserted). Skipped while rebuilding.
    pub(crate) fn remove_row(&mut self, row: &Row, pk: &PkBytes, sup: &mut Vec<u64>) -> Result<()> {
        if !self.ready {
            return Ok(());
        }
        if let Some(text) = self.document_text(row)? {
            for (term, _) in self.analyzer.analyze(&text) {
                self.store.delete_cow(&Self::posting_key(&term, pk), sup)?;
            }
        }
        let key = Self::doc_len_key(pk);
        if let Some(old) = self.store.get(&key)? {
            self.total_len -= u64::from(decode_doc_len(&old)?);
            self.total_docs -= 1;
            self.store.delete_cow(&key, sup)?;
        }
        Ok(())
    }

    // --- MATCH evaluation (SPEC-019 FTS-030/031/032/040) --------------------

    /// Evaluate a `MATCH` predicate: AND-of-items over the posting lists —
    /// term intersection, trailing-`*` prefix union (FTS-031), positional
    /// phrase adjacency (FTS-032) — scored with BM25 (FTS-040). Returns
    /// `(pk, score)` for every matching document, unordered. Routed through
    /// the index only; there is no full-scan fallback (FTS-030). Errors with
    /// the FTS-022 readiness gate while rebuilding.
    pub fn search(&self, query: &FtsQuery) -> Result<Vec<(PkBytes, f64)>> {
        self.check_ready()?;
        // Per item: matched docs with a synthetic term frequency, plus the
        // item's document frequency for idf.
        let mut per_item: Vec<HashMap<PkBytes, u32>> = Vec::with_capacity(query.items.len());
        for item in &query.items {
            let docs: HashMap<PkBytes, u32> = match item {
                // One key-range scan over the term's contiguous postings.
                FtsItem::Term(term) => {
                    let mut docs = HashMap::new();
                    self.for_each_posting(&Self::term_prefix(term), &mut |pk, posting| {
                        docs.insert(pk, posting.tf());
                        Ok(true)
                    })?;
                    docs
                }
                // FTS-031: one scan over every term extending `prefix`,
                // union of the covered posting lists (one synthetic term).
                FtsItem::Prefix(prefix) => {
                    let mut union: HashMap<PkBytes, u32> = HashMap::new();
                    self.for_each_posting(&Self::term_prefix_scan(prefix), &mut |pk, posting| {
                        *union.entry(pk).or_default() += posting.tf();
                        Ok(true)
                    })?;
                    union
                }
                // FTS-032: adjacency in stored positions, honoring the query
                // phrase's own analyzed position deltas (stop-word gaps).
                FtsItem::Phrase(terms) => self.phrase_docs(terms)?,
            };
            if docs.is_empty() {
                return Ok(Vec::new()); // AND semantics: one empty item = no match
            }
            per_item.push(docs);
        }
        if per_item.is_empty() {
            return Ok(Vec::new());
        }
        // Boolean AND: intersect, starting from the smallest item set.
        per_item.sort_by_key(HashMap::len);
        let Some((first, rest)) = per_item.split_first() else {
            return Ok(Vec::new()); // unreachable: emptiness returned above
        };
        let matched: Vec<&PkBytes> = first
            .keys()
            .filter(|pk| rest.iter().all(|docs| docs.contains_key(*pk)))
            .collect();

        // FTS-040: BM25 with the maintained corpus statistics.
        #[allow(clippy::cast_precision_loss)] // corpus sizes are far below 2^52
        let n = self.total_docs() as f64;
        let avgdl = self.avg_doc_len().max(1.0);
        let idf: Vec<f64> = per_item
            .iter()
            .map(|docs| {
                #[allow(clippy::cast_precision_loss)]
                let df = docs.len() as f64;
                (1.0 + (n - df + 0.5) / (df + 0.5)).ln()
            })
            .collect();
        let mut out = Vec::with_capacity(matched.len());
        for pk in matched {
            let dl = f64::from(self.doc_len(pk)?.unwrap_or(0));
            let norm = BM25_K1 * (1.0 - BM25_B + BM25_B * dl / avgdl);
            let mut score = 0.0;
            for (docs, idf) in per_item.iter().zip(&idf) {
                let tf = f64::from(docs.get(pk).copied().unwrap_or(0));
                score += idf * (tf * (BM25_K1 + 1.0)) / (tf + norm);
            }
            out.push((pk.clone(), score));
        }
        Ok(out)
    }

    /// The documents containing `terms` adjacently and in order (FTS-032),
    /// with the phrase's occurrence count as the synthetic tf. The terms
    /// carry their analyzed positions so stop-word gaps in the phrase are
    /// honored exactly as at index time.
    fn phrase_docs(&self, terms: &[(String, u32)]) -> Result<HashMap<PkBytes, u32>> {
        let Some(((first_term, first_pos), rest)) = terms.split_first() else {
            return Ok(HashMap::new());
        };
        // The anchor term's posting list drives the walk; later terms are
        // point lookups, so only one scan happens per phrase.
        let mut anchors: Vec<(PkBytes, Posting)> = Vec::new();
        self.for_each_posting(&Self::term_prefix(first_term), &mut |pk, posting| {
            anchors.push((pk, posting));
            Ok(true)
        })?;
        let mut out = HashMap::new();
        for (pk, first_posting) in anchors {
            // Every later term must appear at the position-delta offset from
            // the anchor occurrence.
            let mut count = 0u32;
            for anchor in &first_posting.positions {
                let mut all = true;
                for (term, pos) in rest {
                    let offset = pos - first_pos;
                    let needed = anchor + offset;
                    let present = self
                        .posting(term, &pk)?
                        .is_some_and(|p| p.positions.binary_search(&needed).is_ok());
                    if !present {
                        all = false;
                        break;
                    }
                }
                if all {
                    count += 1;
                }
            }
            if count > 0 {
                out.insert(pk, count);
            }
        }
        Ok(out)
    }

    // --- BM25 corpus statistics (FTS-020) ---------------------------------

    /// Total indexed documents.
    pub fn total_docs(&self) -> usize {
        self.total_docs
    }

    /// Average document length (`avgdl`), or `0.0` for an empty corpus.
    pub fn avg_doc_len(&self) -> f64 {
        let docs = self.total_docs();
        if docs == 0 {
            0.0
        } else {
            self.total_len as f64 / docs as f64
        }
    }

    /// Document frequency of `term`: how many documents contain it.
    pub fn doc_freq(&self, term: &str) -> Result<usize> {
        let mut count = 0;
        self.for_each_posting(&Self::term_prefix(term), &mut |_, _| {
            count += 1;
            Ok(true)
        })?;
        Ok(count)
    }

    /// The length of document `pk`, if indexed.
    pub fn doc_len(&self, pk: &PkBytes) -> Result<Option<u32>> {
        self.store
            .get(&Self::doc_len_key(pk))?
            .map(|bytes| decode_doc_len(&bytes))
            .transpose()
    }

    /// The posting for `term` in document `pk` (positions + `tf`), if any.
    pub fn posting(&self, term: &str, pk: &PkBytes) -> Result<Option<Posting>> {
        self.store
            .get(&Self::posting_key(term, pk))?
            .map(|bytes| decode_posting(&bytes))
            .transpose()
    }

    /// The posting list for `term`: every document that contains it, in PK
    /// key order.
    pub fn postings_for(&self, term: &str) -> Result<Vec<(PkBytes, Posting)>> {
        let mut out = Vec::new();
        self.for_each_posting(&Self::term_prefix(term), &mut |pk, posting| {
            out.push((pk, posting));
            Ok(true)
        })?;
        Ok(out)
    }

    /// Every `(term-keyed posting key, positions)` plus every document
    /// length, in canonical key order — the STG-007 rule-2 comparison
    /// surface. Paged storage has no meaningful structural equality, so a
    /// fresh rebuild is compared by contents; the keys embed both term and
    /// PK, so this covers postings and corpus statistics alike.
    pub(crate) fn entries(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out = Vec::new();
        // Both keyspaces sort at or after the lowest tag, so one unbounded
        // scan from it covers postings and document lengths in key order.
        self.store.scan(&[KEY_POSTING], None, &mut |key, value| {
            out.push((key.to_vec(), value.to_vec()));
            Ok(true)
        })?;
        Ok(out)
    }
}

/// Keyspace tags (see the module docs).
const KEY_POSTING: u8 = 0x00;
const KEY_DOC_LEN: u8 = 0x01;

#[cfg(test)]
impl FullTextIndexState {
    /// Test wrapper: insert, discarding the superseded-page sink.
    fn t_insert(&mut self, row: &Row, pk: PkBytes) {
        self.insert_row(row, pk, &mut Vec::new())
            .unwrap_or_else(|e| panic!("{e}"));
    }

    /// Test wrapper: remove, discarding the superseded-page sink.
    fn t_remove(&mut self, row: &Row, pk: &PkBytes) {
        self.remove_row(row, pk, &mut Vec::new())
            .unwrap_or_else(|e| panic!("{e}"));
    }

    fn t_posting(&self, term: &str, pk: &PkBytes) -> Option<Posting> {
        self.posting(term, pk).unwrap_or_else(|e| panic!("{e}"))
    }

    fn t_doc_freq(&self, term: &str) -> usize {
        self.doc_freq(term).unwrap_or_else(|e| panic!("{e}"))
    }

    fn t_doc_len(&self, pk: &PkBytes) -> Option<u32> {
        self.doc_len(pk).unwrap_or_else(|e| panic!("{e}"))
    }

    /// Logical content — what the resident `PartialEq` used to compare.
    fn t_entries(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.entries().unwrap_or_else(|e| panic!("{e}"))
    }
}

/// Encode a posting's positions as little-endian `u32`s (ascending, as
/// stored).
fn encode_posting(posting: &Posting) -> Vec<u8> {
    let mut out = Vec::with_capacity(posting.positions.len() * 4);
    for pos in &posting.positions {
        out.extend_from_slice(&pos.to_le_bytes());
    }
    out
}

/// Decode a posting written by [`encode_posting`].
fn decode_posting(bytes: &[u8]) -> Result<Posting> {
    let (chunks, rest) = bytes.as_chunks::<4>();
    if !rest.is_empty() {
        return Err(FluxumError::Storage(
            "malformed paged full-text posting".into(),
        ));
    }
    Ok(Posting {
        positions: chunks.iter().copied().map(u32::from_le_bytes).collect(),
    })
}

/// Decode a document length written as a little-endian `u32`.
fn decode_doc_len(bytes: &[u8]) -> Result<u32> {
    let raw: [u8; 4] = bytes
        .try_into()
        .map_err(|_| FluxumError::Storage("malformed paged full-text document length".into()))?;
    Ok(u32::from_le_bytes(raw))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::schema::{ColumnSchema, FluxType, TableAccess, TableSchema, VisibilityRule};
    use crate::store::row::encode_pk_values;

    static COLS: &[ColumnSchema] = &[ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    }];
    static T: TableSchema = TableSchema {
        name: "Doc",
        columns: COLS,
        primary_key: &[0],
        auto_inc: None,
        access: TableAccess::Public,
        partition_by: None,
        unique: &[],
        indexes: &[],
        visibility: VisibilityRule::PublicAll,
    };

    fn pk(n: u64) -> PkBytes {
        encode_pk_values(&T, &[RowValue::U64(n)]).unwrap()
    }

    fn text_row(s: &str) -> Row {
        Row::new(vec![RowValue::Str(s.to_owned())])
    }

    /// A throwaway pager, one fresh directory per call (parallel test
    /// threads must never share a page file for the same table id).
    fn test_pager() -> Arc<Pager> {
        use crate::config::PageCompression;
        use crate::store::pager::PagerOptions;
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("fluxum-fts-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("{e}"));
        Pager::open(
            dir,
            PagerOptions {
                shard_id: 0,
                page_size: 4096,
                pool_capacity_bytes: 512 * 4096,
                high_watermark: 0.95,
                low_watermark: 0.90,
                compression: PageCompression::None,
                compression_min_bytes: 1024,
            },
        )
        .unwrap_or_else(|e| panic!("{e}"))
    }

    /// An empty paged index over column 0 with `analyzer`, on its own file.
    fn new_index(analyzer: Analyzer) -> FullTextIndexState {
        FullTextIndexState::new(0, analyzer, &test_pager(), TableId::of("Doc"))
            .unwrap_or_else(|e| panic!("{e}"))
    }

    #[test]
    fn tokenization_is_unicode_and_positional() {
        let a = Analyzer::simple();
        // Punctuation/whitespace separate; Unicode letters and digits are kept
        // and case-folded; `café` stays one token, `42` its own.
        let terms = a.analyze("Hello, WORLD! café 42");
        assert_eq!(
            terms,
            vec![
                ("hello".to_owned(), 0),
                ("world".to_owned(), 1),
                ("café".to_owned(), 2),
                ("42".to_owned(), 3),
            ]
        );
    }

    #[test]
    fn english_stop_words_drop_but_positions_survive() {
        let a = Analyzer {
            language: Language::English,
            stop_words: true,
            stemming: false,
        };
        // "the" and "a" are dropped; the kept terms keep their token index.
        let terms = a.analyze("the quick brown fox and a dog");
        assert_eq!(
            terms,
            vec![
                ("quick".to_owned(), 1),
                ("brown".to_owned(), 2),
                ("fox".to_owned(), 3),
                ("dog".to_owned(), 6),
            ]
        );
    }

    #[test]
    fn english_stemmer_is_deterministic_and_folds_variants() {
        let a = Analyzer {
            language: Language::English,
            stop_words: false,
            stemming: true,
        };
        let stem = |w: &str| a.analyze(w)[0].0.clone();
        assert_eq!(stem("running"), "runn");
        assert_eq!(stem("berries"), "berry");
        assert_eq!(stem("classes"), "class");
        assert_eq!(stem("quickly"), "quick");
        assert_eq!(stem("walked"), "walk");
        assert_eq!(stem("cats"), "cat");
        assert_eq!(stem("class"), "class"); // -ss never stripped
    }

    #[test]
    fn analyzer_id_is_versioned_and_config_sensitive() {
        let simple = Analyzer::simple().id();
        let english_plain = Analyzer {
            language: Language::English,
            stop_words: false,
            stemming: false,
        }
        .id();
        let english_full = Analyzer {
            language: Language::English,
            stop_words: true,
            stemming: true,
        }
        .id();
        assert_ne!(simple, english_plain);
        assert_ne!(english_plain, english_full);
        // Simple ignores stop/stem flags (they are no-ops), so the id is stable.
        assert_eq!(
            simple,
            Analyzer {
                language: Language::Simple,
                stop_words: true,
                stemming: true,
            }
            .id()
        );
    }

    #[test]
    fn postings_carry_tf_and_positions_and_bm25_stats() {
        let mut idx = new_index(Analyzer::simple());
        idx.t_insert(&text_row("red fox red fox red"), pk(1));
        idx.t_insert(&text_row("blue fox"), pk(2));

        let red = idx.t_posting("red", &pk(1)).unwrap();
        assert_eq!(red.tf(), 3);
        assert_eq!(red.positions, vec![0, 2, 4]);
        assert_eq!(idx.t_doc_freq("fox"), 2, "fox appears in both docs");
        assert_eq!(idx.t_doc_freq("red"), 1);
        assert_eq!(idx.total_docs(), 2);
        assert_eq!(idx.t_doc_len(&pk(1)), Some(5));
        assert_eq!(idx.t_doc_len(&pk(2)), Some(2));
        assert_eq!(idx.avg_doc_len(), 3.5);
    }

    #[test]
    fn remove_reverses_insert_exactly() {
        let analyzer = Analyzer::simple();
        let empty = new_index(analyzer);
        let mut idx = new_index(analyzer);
        idx.t_insert(&text_row("alpha beta gamma"), pk(1));
        idx.t_insert(&text_row("beta gamma delta"), pk(2));
        idx.t_remove(&text_row("alpha beta gamma"), &pk(1));
        idx.t_remove(&text_row("beta gamma delta"), &pk(2));
        assert_eq!(
            idx.t_entries(),
            empty.t_entries(),
            "full delete returns to the empty index"
        );
        // The resident counters must unwind with the keyspace, not just the
        // keys — a stale `total_len` would skew every later BM25 score.
        assert_eq!(idx.total_docs(), 0);
        assert_eq!(idx.avg_doc_len(), 0.0);
    }

    #[test]
    fn null_and_list_documents_are_handled() {
        let mut idx = new_index(Analyzer::simple());
        // NULL document: counted with length 0, contributes no terms.
        let null_row = Row::new(vec![RowValue::Optional(None)]);
        idx.t_insert(&null_row, pk(1));
        assert_eq!(idx.t_doc_len(&pk(1)), Some(0));
        assert_eq!(idx.total_docs(), 1);

        // Vec<String>: elements join with a gap so terms never merge.
        let list_row = Row::new(vec![RowValue::List(vec![
            RowValue::Str("hello world".to_owned()),
            RowValue::Str("world peace".to_owned()),
        ])]);
        idx.t_insert(&list_row, pk(2));
        assert_eq!(idx.t_doc_freq("world"), 1);
        assert_eq!(
            idx.t_posting("world", &pk(2)).unwrap().positions,
            vec![1, 2]
        );
    }

    #[test]
    fn not_ready_index_skips_maintenance_and_gates_queries() {
        let mut idx = new_index(Analyzer::simple()).rebuilding_like_pub();
        assert!(!idx.is_ready());
        idx.t_insert(&text_row("ignored while rebuilding"), pk(1));
        assert_eq!(idx.total_docs(), 0, "maintenance skipped");
        let err = idx.check_ready().unwrap_err();
        assert_eq!(
            err.query_code(),
            Some(fluxum_protocol::codes::STORAGE_FULLTEXT_REBUILDING)
        );
    }

    impl FullTextIndexState {
        fn rebuilding_like_pub(&self) -> Self {
            self.rebuilding_like().unwrap_or_else(|e| panic!("{e}"))
        }
    }
}
