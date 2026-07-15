## 1. Implementation
- [ ] 1.1 Wire ScoreReranker into the MATCH path: BM25 top-K candidates (rerank_candidate_k, default 100) -> reranker -> return its order truncated to LIMIT (PLG-040)
- [ ] 1.2 Reranker failure/timeout falls back to the BM25 order; snapshot-only (InitialData/one-off) (PLG-040; SUB-013)
- [ ] 1.3 Wire Retriever: request external top-K (e.g. Vectorizer) via the capability (PLG-041)
- [ ] 1.4 Fusion capability with default Reciprocal Rank Fusion (no score-scale normalization); return fused top-N (PLG-041)
- [ ] 1.5 Retriever/fusion failure falls back to the lexical BM25 result (PLG-041)
- [ ] 1.6 Verification: RRF of BM25 + stub retriever matches a reference RRF order; disabling each hook falls back correctly; deterministic-sim suite passes with a non-deterministic reranker stub (stored state + diffs bit-identical, only order differs)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
