## 1. Implementation
- [x] 1.1 Define the `FluxValue` enum covering all primitive types plus product/sum composite types
- [x] 1.2 Implement the FluxBIN row encoder/decoder for every `FluxValue` variant (little-endian)
- [x] 1.3 Define the FluxRPC message types (Authenticate, ReducerCall, Subscribe, SubscribeSingle, Unsubscribe, OneOffQuery, TxUpdate, InitialData, errors) - incl. enriched TxUpdate metadata and the per-connection `tx_updates: full|light` opt-out (FR-43)
- [x] 1.4 Implement the `u32 LE length + MessagePack` frame codec with max-frame-size handling
- [x] 1.5 Implement flat RowList batch encoding (Fixed size-hint vs Offsets degradation; inconsistent count/size/data rejected with 400) (SPEC-006 RPC-032)
- [x] 1.6 FluxBIN golden vectors: fixed input to fixed expected bytes for every RPC-040 type; the RPC-041 Sensor row encodes to exactly 32 bytes and RPC-042 delete entries to exactly 8 bytes (SPEC-006 acceptance 2)
- [x] 1.7 Size-advantage check: FluxBIN encoding of canonical Sensor/ChatMessage rows measurably smaller than self-describing MessagePack (target ~40%, FR-41; SPEC-006 acceptance 3)
- [x] 1.8 Verification (DAG exit test): proptest roundtrip property tests for every type (FluxValue, FluxBIN rows, frames)
- [x] 1.9 Gate G1 input: codec roundtrip property tests green (with T1.1 schema and T1.3 auth suites)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation
- [x] 2.2 Write tests covering the new behavior
- [x] 2.3 Run tests and confirm they pass
