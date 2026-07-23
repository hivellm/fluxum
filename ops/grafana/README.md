# Fluxum observability — Grafana

`fluxum-overview.json` is the committed dashboard (PRD §12.1 criterion) over
the SPEC-012 P0 `fluxum_*` metrics: liveness/lifecycle, reducer
throughput + latency (NFR-01/03), fan-out + subscriptions (NFR-04),
connections, and the security surface (auth, rejections, rate-limits).

## Provisioning

Fluxum exposes Prometheus text at `GET /metrics` on the HTTP admin port
(`open_health_metrics` leaves it ungated for scraping; SEC-054).

1. **Prometheus** — scrape the shard(s):

   ```yaml
   # prometheus.yml
   scrape_configs:
     - job_name: fluxum
       metrics_path: /metrics
       static_configs:
         - targets: ["127.0.0.1:15800"]   # each shard's HTTP admin port
   ```

2. **Grafana** — provision the datasource + dashboard (file provisioning):

   ```yaml
   # /etc/grafana/provisioning/datasources/prometheus.yml
   apiVersion: 1
   datasources:
     - name: Prometheus
       type: prometheus
       access: proxy
       url: http://prometheus:9090
       isDefault: true
   ```

   ```yaml
   # /etc/grafana/provisioning/dashboards/fluxum.yml
   apiVersion: 1
   providers:
     - name: fluxum
       type: file
       options:
         path: /var/lib/grafana/dashboards
   ```

   Copy `fluxum-overview.json` into that `path`. The dashboard declares a
   `DS_PROMETHEUS` input, so importing through the UI prompts for the
   datasource; file provisioning binds it to the default Prometheus.

3. Open **Fluxum — shard overview**. The `shard` template variable
   (populated from `label_values(fluxum_up, shard)`) filters every panel;
   `All` aggregates across a multi-shard host.

## Coverage

`crates/fluxum-server/tests/dashboard_metrics.rs` asserts every P0 metric
family the server exports appears in the dashboard JSON, so a new metric
without a panel fails CI rather than silently going unmonitored.
