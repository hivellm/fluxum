## 1. Implementation
- [ ] 1.1 Finalize the `/schema` JSON document: tables (columns, types, pk/auto_inc, indexes, spatial_index, partition_by, visibility), reducers (name, version, params, return_type), views, procedures, schema_version (FR-81, SDK-001..)
- [ ] 1.2 Implement `fluxum schema export --server <url> --out schema.json` in fluxum-cli; exported JSON identical to `GET /schema` (SPEC-001 acceptance 8)
- [ ] 1.3 Commit the golden `schema.json` for the demo-app module; byte-for-byte CI comparison - any diff fails CI (freeze gate, SPEC-011 acceptance 1)
- [ ] 1.4 Declare the MODULE API FREEZE: document that SPEC-001/004/011 surface changes must be additive from here on (DAG change-control note)
- [ ] 1.5 Verification (DAG exit test): schema golden-file test green

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
