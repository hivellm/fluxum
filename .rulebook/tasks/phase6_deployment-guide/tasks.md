## 1. Implementation
- [ ] 1.1 Write the deployment guide (ROADMAP M7 definition-of-done): install, run, upgrade, data-directory layout, ports 15800/15801
- [ ] 1.2 systemd unit file (hardening options, restart policy) verified to boot the demo config on a clean machine
- [ ] 1.3 Dockerfile (+ compose example) building the single release binary; container respects cgroup limits via the FR-05 hardware probe
- [ ] 1.4 Config reference: every config.yml key, default, and FLUXUM_ env override (generated from or checked against the example config from phase0)
- [ ] 1.5 Droplet profile guidance: recommended settings for 1 vCPU / 512 MB (memory.budget auto, expectations per NFR-12)
- [ ] 1.6 Verification: clean-machine install run following the guide verbatim (docker and systemd paths both boot and serve /health)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
