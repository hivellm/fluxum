## 1. Implementation
- [ ] 1.1 Implement full sync: checkpoint transfer to a cold replica, then switch to log streaming
- [ ] 1.2 Implement partial sync: resume streaming from a replica-provided log offset
- [ ] 1.3 Stream commit-log entries in the frozen G5 format with replica-side CRC verification and apply
- [ ] 1.4 Implement async mode and semi-sync quorum mode (commit acked after quorum of replicas persist)
- [ ] 1.5 Implement epoch fencing so entries from a deposed primary are rejected
- [ ] 1.6 Verification (DAG exit test): replica converges from cold and from an offset; semi-sync ack test green

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
