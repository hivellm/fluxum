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
//! commits the index is bit-identical to a fresh rebuild over the committed
//! rows.
//!
//! The `MATCH` query operator, BM25 ranking, and subscription integration are
//! the sibling phase-4 task; this module is the index and its statistics
//! only. Postings are keyed in a [`BTreeMap`] so term-prefix scans (and, when
//! the pager is wired into the live path, page-ordered eviction) are plain
//! range iteration — the same tiering story as the B-tree index.

use std::collections::BTreeMap;

use crate::error::{FluxumError, Result};
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
#[derive(Debug, Clone, PartialEq)]
pub struct FullTextIndexState {
    /// Indexed text column ordinal (FTS-001).
    column: u16,
    /// The deterministic analyzer (FTS-010).
    analyzer: Analyzer,
    /// Positional posting lists: `term → { pk → positions }` (FTS-020).
    /// A `BTreeMap` so term-prefix scans are range iteration.
    postings: BTreeMap<String, BTreeMap<PkBytes, Posting>>,
    /// Per-document length (kept-term count) for BM25 (FTS-020).
    doc_len: BTreeMap<PkBytes, u32>,
    /// Sum of `doc_len` over all documents — the BM25 `avgdl` numerator.
    total_len: u64,
    /// FTS-022 gate: `false` while the index awaits its post-recovery
    /// rebuild — queries return `STORAGE_FULLTEXT_REBUILDING`, commit-merge
    /// maintenance is skipped (the rebuild recreates it from the rows).
    ready: bool,
}

impl FullTextIndexState {
    /// An empty, ready full-text index over `column` with `analyzer`.
    pub(crate) fn new(column: u16, analyzer: Analyzer) -> Self {
        Self {
            column,
            analyzer,
            postings: BTreeMap::new(),
            doc_len: BTreeMap::new(),
            total_len: 0,
            ready: true,
        }
    }

    /// An empty index with this index's exact configuration — the rebuild
    /// seed for the STG-007 rule-2 integrity check and FTS-022 rebuilds.
    pub(crate) fn fresh_like(&self) -> Self {
        Self::new(self.column, self.analyzer)
    }

    /// This configuration in the FTS-022 rebuilding state: empty, not ready.
    pub(crate) fn rebuilding_like(&self) -> Self {
        Self {
            ready: false,
            ..self.fresh_like()
        }
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

    /// Add `row`'s document to the index (commit merge, insert side —
    /// FTS-021). Skipped while rebuilding.
    pub(crate) fn insert_row(&mut self, row: &Row, pk: PkBytes) -> Result<()> {
        if !self.ready {
            return Ok(());
        }
        let Some(text) = self.document_text(row)? else {
            // NULL document: still a document with length 0 (BM25 counts it).
            self.doc_len.insert(pk, 0);
            return Ok(());
        };
        let (terms, doc_len) = self.analyzer.analyze_doc(&text);
        let mut per_term: BTreeMap<String, Vec<u32>> = BTreeMap::new();
        for (term, pos) in terms {
            per_term.entry(term).or_default().push(pos);
        }
        for (term, positions) in per_term {
            self.postings
                .entry(term)
                .or_default()
                .insert(pk.clone(), Posting { positions });
        }
        self.doc_len.insert(pk, doc_len);
        self.total_len += u64::from(doc_len);
        Ok(())
    }

    /// Remove `row`'s document from the index (commit merge, delete side —
    /// FTS-021). Re-analyzes the old row (the analyzer is deterministic, so
    /// this reproduces exactly what was inserted). Skipped while rebuilding.
    pub(crate) fn remove_row(&mut self, row: &Row, pk: &PkBytes) -> Result<()> {
        if !self.ready {
            return Ok(());
        }
        if let Some(text) = self.document_text(row)? {
            for (term, _) in self.analyzer.analyze(&text) {
                if let Some(docs) = self.postings.get_mut(&term) {
                    docs.remove(pk);
                    if docs.is_empty() {
                        self.postings.remove(&term);
                    }
                }
            }
        }
        if let Some(len) = self.doc_len.remove(pk) {
            self.total_len -= u64::from(len);
        }
        Ok(())
    }

    // --- BM25 corpus statistics (FTS-020) ---------------------------------

    /// Total indexed documents.
    pub fn total_docs(&self) -> usize {
        self.doc_len.len()
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
    pub fn doc_freq(&self, term: &str) -> usize {
        self.postings.get(term).map_or(0, BTreeMap::len)
    }

    /// The length of document `pk`, if indexed.
    pub fn doc_len(&self, pk: &PkBytes) -> Option<u32> {
        self.doc_len.get(pk).copied()
    }

    /// The posting for `term` in document `pk` (positions + `tf`), if any.
    pub fn posting(&self, term: &str, pk: &PkBytes) -> Option<&Posting> {
        self.postings.get(term)?.get(pk)
    }

    /// The posting list for `term`: every document that contains it.
    pub fn postings_for(&self, term: &str) -> Option<&BTreeMap<PkBytes, Posting>> {
        self.postings.get(term)
    }
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
        let mut idx = FullTextIndexState::new(0, Analyzer::simple());
        idx.insert_row(&text_row("red fox red fox red"), pk(1))
            .unwrap();
        idx.insert_row(&text_row("blue fox"), pk(2)).unwrap();

        let red = idx.posting("red", &pk(1)).unwrap();
        assert_eq!(red.tf(), 3);
        assert_eq!(red.positions, vec![0, 2, 4]);
        assert_eq!(idx.doc_freq("fox"), 2, "fox appears in both docs");
        assert_eq!(idx.doc_freq("red"), 1);
        assert_eq!(idx.total_docs(), 2);
        assert_eq!(idx.doc_len(&pk(1)), Some(5));
        assert_eq!(idx.doc_len(&pk(2)), Some(2));
        assert_eq!(idx.avg_doc_len(), 3.5);
    }

    #[test]
    fn remove_reverses_insert_exactly() {
        let analyzer = Analyzer::simple();
        let empty = FullTextIndexState::new(0, analyzer);
        let mut idx = FullTextIndexState::new(0, analyzer);
        idx.insert_row(&text_row("alpha beta gamma"), pk(1))
            .unwrap();
        idx.insert_row(&text_row("beta gamma delta"), pk(2))
            .unwrap();
        idx.remove_row(&text_row("alpha beta gamma"), &pk(1))
            .unwrap();
        idx.remove_row(&text_row("beta gamma delta"), &pk(2))
            .unwrap();
        assert_eq!(idx, empty, "full delete returns to the empty index");
    }

    #[test]
    fn null_and_list_documents_are_handled() {
        let mut idx = FullTextIndexState::new(0, Analyzer::simple());
        // NULL document: counted with length 0, contributes no terms.
        let null_row = Row::new(vec![RowValue::Optional(None)]);
        idx.insert_row(&null_row, pk(1)).unwrap();
        assert_eq!(idx.doc_len(&pk(1)), Some(0));
        assert_eq!(idx.total_docs(), 1);

        // Vec<String>: elements join with a gap so terms never merge.
        let list_row = Row::new(vec![RowValue::List(vec![
            RowValue::Str("hello world".to_owned()),
            RowValue::Str("world peace".to_owned()),
        ])]);
        idx.insert_row(&list_row, pk(2)).unwrap();
        assert_eq!(idx.doc_freq("world"), 1);
        assert_eq!(idx.posting("world", &pk(2)).unwrap().positions, vec![1, 2]);
    }

    #[test]
    fn not_ready_index_skips_maintenance_and_gates_queries() {
        let mut idx = FullTextIndexState::new(0, Analyzer::simple()).rebuilding_like_pub();
        assert!(!idx.is_ready());
        idx.insert_row(&text_row("ignored while rebuilding"), pk(1))
            .unwrap();
        assert_eq!(idx.total_docs(), 0, "maintenance skipped");
        let err = idx.check_ready().unwrap_err();
        assert_eq!(
            err.query_code(),
            Some(fluxum_protocol::codes::STORAGE_FULLTEXT_REBUILDING)
        );
    }

    impl FullTextIndexState {
        fn rebuilding_like_pub(&self) -> Self {
            self.rebuilding_like()
        }
    }
}
