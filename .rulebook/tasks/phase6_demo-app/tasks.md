## 1. Implementation
- [ ] 1.1 Build the demo Fluxum module (chat + presence + per-user tasks) on the example schema: reducers (send_chat, complete_task, ...), on_connect/on_disconnect presence, `#[visibility(owner_only)]` on Task, a rate-limited reducer (UC-1/UC-2/UC-6)
- [ ] 1.2 Build the demo web client entirely on the generated TypeScript SDK over Streamable HTTP in the browser (FR-82)
- [ ] 1.3 Serve the demo statics from the repo, including the vanilla-JS smoke page used by the T6.2 SDK-081 test (script type=module, no build step)
- [ ] 1.4 End-to-end assertions: send_chat produces a TxUpdate, local cache reflects the new ChatMessage row, typed insert callback fires with a FluxBIN-decoded row (SPEC-011 acceptance 8)
- [ ] 1.5 Verification (DAG exit test): demo scenario scripted in CI (auth -> subscribe -> reducer -> TxUpdate -> cache assertions)
- [ ] 1.6 Gate G6 input: demo runs end-to-end on the generated SDK (PRD 12.1)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
