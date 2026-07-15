## 1. Implementation
- [ ] 1.1 PluginRegistry + closed capability set with placement classes (WritePath | ReadPath | OffPath) (PLG-001/003)
- [ ] 1.2 Adopt existing seams as capabilities via adapters: AuthProvider, ColumnTransform, KeyProvider, VisibilityFn — APIs and call sites unchanged (PLG-002)
- [ ] 1.3 New capability traits (definitions only, bound by sibling tasks): ScoreReranker, Retriever/Fusion, StreamSink (PLG-001 §2.1)
- [ ] 1.4 In-process host: link-time registration + Cargo feature gating (absent unless enabled); catch_unwind isolation, panic disables plugin / rolls back WritePath tx (PLG-030)
- [ ] 1.5 Placement enforcement in ServerBuilder::build(): reject sidecar-on-WritePath, uncompiled in-proc feature, illegal capability/host combos, missing applies_to targets (PLG-020/021/032)
- [ ] 1.6 config.yml plugins manifest parsing + validation (PLG-032)
- [ ] 1.7 GET /plugins introspection (name, capability, host, placement, scope; no secrets) (PLG-060)
- [ ] 1.8 Security rules: no implicit privilege widening; RLS bypass only via explicit server-peer grant; hot disable/circuit-break without core restart (PLG-061)
- [ ] 1.9 Verification: existing seams appear in /plugins with unchanged behavior; illegal bindings abort build() with descriptive errors; a legal in-proc set starts and a panicking plugin is isolated + metered

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
