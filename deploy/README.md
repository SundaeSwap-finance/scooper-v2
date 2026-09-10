# Deploy artifacts

Operational files that ship alongside the scooper.

## `scooper-v2.service`

Systemd unit installed at `/etc/systemd/system/scooper-v2.service` by
`./deploy.sh`. The script daemon-reloads when this file changes.

## `alerts.yml`

Prometheus alerting rules. Load via:

```yaml
# /etc/prometheus/prometheus.yml
rule_files:
  - /etc/prometheus/scooper-alerts.yml
```

Thresholds are calibrated for preview — production environments need
tuning. The alerts cover sync lag, crash loops, stuck-pending-orders,
submit/build error rates, and low-wallet-balance.

## `grafana-dashboard.json`

Grafana dashboard for the metrics exposed at `:9999/metrics`.

**Import via UI**: Grafana → Dashboards → New → Import → upload JSON →
pick a Prometheus data source.

**Provision via filesystem** (preferred for ops): drop this file into
Grafana's dashboard provisioning path and add a YAML provider:

```yaml
# /etc/grafana/provisioning/dashboards/scooper.yaml
apiVersion: 1
providers:
  - name: scooper
    folder: SundaeSwap
    type: file
    options:
      path: /var/lib/grafana/dashboards
```

Then copy `grafana-dashboard.json` to `/var/lib/grafana/dashboards/`.
The dashboard auto-reloads when the file changes.

The dashboard uses a `$DS_PROMETHEUS` data-source variable, so the
same JSON works against any Prometheus instance Grafana knows about.

### Panels

- **Sync status** — sync lag, tip slot, uptime, pending order count
- **Throughput** — batches/min, orders/min broken out by pool type
- **Failures & in-flight** — failures stacked by reason, in-flight txs
  alongside oldest-pending-order age
- **Submit latency** — p50/p95/p99 + heatmap
- **Wallet & state** — balance, UTxO count, quarantined orders
