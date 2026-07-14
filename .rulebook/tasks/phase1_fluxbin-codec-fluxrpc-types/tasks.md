## 1. Implementation
- [ ] 1.1 Define the `FluxValue` enum covering all primitive types plus product/sum composite types
- [ ] 1.2 Implement the FluxBIN row encoder/decoder for every `FluxValue` variant (little-endian)
- [ ] 1.3 Define the FluxRPC message types (Authenticate, ReducerCall, Subscribe, SubscribeSingle, Unsubscribe, OneOffQuery, TxUpdate, InitialData, errors) - incl. enriched TxUpdate metadata and the per-connection `tx_updates: full|light` opt-out (FR-43)
- [ ] 1.4 Implement the `u32 LE length + MessagePack` frame codec with max-frame-size handling
- [ ] 1.5 Implement flat RowList batch encoding (Fixed size-hint vs Offsets degradation; inconsistent count/size/data rejected with 400) (SPEC-006 RPC-032)
- [ ] 1.6 FluxBIN golden vectors: fixed input to fixed expected bytes for every RPC-040 type; the RPC-041 Sensor row encodes to exactly 32 bytes and RPC-042 delete entries to exactly 8 bytes (SPEC-006 acceptance 2)
- [ ] 1.7 Size-advantage check: FluxBIN encoding of canonical Sensor/ChatMessage rows measurably smaller than self-describing MessagePack (target ~40%, FR-41; SPEC-006 acceptance 3)
- [ ] 1.8 Verification (DAG exit test): proptest roundtrip property tests for every type (FluxValue, FluxBIN rows, frames)
- [ ] 1.9 Gate G1 input: codec roundtrip property tests green (with T1.1 schema and T1.3 auth suites)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
