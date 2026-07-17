# Proposal: phase4_fulltext-rerank-hybrid-fusion

## Why
This is the motivating case for the whole plugin system: improving full-text results with models, and integrating with Vectorizer, without embedding either in the core. The impressive quality gains over pure BM25 come from dense/semantic retrieval (Vectorizer's model half) fused with lexical BM25, and from optional model re-ranking of the top-K. This task wires those two ReadPath hooks into the MATCH query path. Because ranking is snapshot-only (SUB-013), a non-deterministic model here can never corrupt stored state or TxUpdate correctness — it only reorders an initial result set, which is exactly why it is safe.

## What Changes
Bind the ScoreReranker and Retriever/Fusion capabilities (defined by phase3) into the SPEC-019 MATCH query path. ScoreReranker: evaluate BM25 to a candidate top-K (rerank_candidate_k, default 100), pass candidates to the reranker, return its order truncated to LIMIT; on failure/timeout the BM25 order stands. Retriever + Fusion: request the retriever's (Vectorizer) top-K, fuse with the BM25 list via Reciprocal Rank Fusion (default, no score-scale normalization), return fused top-N; on retriever failure the lexical result stands. Both are InitialData/one-off only and degrade gracefully. No model or Vectorizer client enters the Fluxum binary — they are sidecar plugins (phase5 host).

## Impact
- Governing spec: SPEC-020 (§5 query-path hooks PLG-040/041) — docs/specs/SPEC-020-plugin-system.md
- Related specs: SPEC-019 (FTS-040 BM25 candidates + _score), SPEC-005 (SUB-013 snapshot-only ranking), SPEC-018 (query access path)
- New PRD requirements: FR-97 (plugin framework — query hooks)
- Affected code: crates/fluxum-core/src/subscription/mod.rs + sql/ (MATCH evaluation calls the reranker/fusion hooks), fusion (RRF default)
- Depends on: phase2_fulltext-inverted-index + phase4_fulltext-match-query-bm25 (BM25 candidates), phase3_plugin-framework-core (capabilities), phase5_plugin-sidecar-host (to run model/Vectorizer sidecars)
- Sequencing: query-surface hooks — align with the FTS query work (before T6.1 freeze)
- Breaking change: NO (additive hooks; absent unless a plugin is bound)
- User benefit: model-grade full-text relevance and hybrid lexical+semantic search (via Vectorizer) with graceful fallback to BM25 — all without bloating the server
