# P0 Payment SR Alert Agent (Airflow + Kubernetes + ClickHouse + Trino)

> Stateful card payment SR monitoring agent.
> Airflow is a **thin scheduler only** — all SQL, data processing and alerting
> run inside an isolated Kubernetes pod so a script crash never affects the
> Airflow worker or any other DAG.

---

## What problem does this solve?

Card payment success rates (SR) can drop suddenly due to bank
rejections, network outages, or gateway issues. The impact is large — even a
5 % SR drop on a high-volume merchant means thousands of failed transactions.

This agent:

1. Runs every hour and checks whether the last completed hour breached an
   SR threshold for any of 12 monitored merchants.
2. Applies a two-step state machine (WATCH → CRITICAL) to avoid false alerts
   from single-hour anomalies.
3. Posts a structured Slack alert and **automatically triggers an AI RCA bot**
   to analyse the failure window and post a root-cause summary — all without
   manual intervention.

---

## Architecture

```
Airflow (scheduler)
        │
        │  KubernetesPodOperator — launches ONE pod per run
        │  (all credentials are env-var-mounted by make_kubernetes_python_task)
        ▼
┌──────────────────────────────────────────────────────────┐
│                  Kubernetes Pod                          │
│                                                          │
│  script.py                                               │
│    │                                                     │
│    ├─► ClickHouse  ──── SR data for last 2 hours         │
│    │   (columnar, fast, explicit IST timezone)           │
│    │                                                     │
│    ├─► Hive / Trino ─── persistent alert state table     │
│    │   (WATCH / CRITICAL / RECOVERED rows)               │
│    │                                                     │
│    └─► Slack Webhook                                     │
│         ├── human alert message (WATCH / CRITICAL / …)  │
│         └── AI RCA bot message (auto root-cause query)  │
└──────────────────────────────────────────────────────────┘
```

### Why Kubernetes pod isolation?

| Without pod isolation | With pod isolation |
|---|---|
| Script crash kills the Airflow worker | Crash is contained to the pod; worker is unaffected |
| All DAGs share one Python environment | Pod gets its own image, dependencies, CPU/memory limits |
| Secrets live in DAG code or Airflow vars | Secrets are mounted as env vars from a secrets store |
| Hard to debug a single script run | Pod logs are queryable independently; pod can be re-run |

Airflow does exactly **one thing** in `dag.py`: schedule the pod via
`KubernetesPodOperator`. Every query, threshold check, state update and Slack
post happens inside `script.py` running in the pod.

---

## Alert state machine

```
               ┌─────────────────────────────────┐
               │         No breach                │
               │    (SR above threshold OR         │
               │     volume below min_attempts)    │
               └─────────────────────────────────┘
                           ▲           ▲
               CLEARED     │           │ RECOVERED
               (auto)      │           │ (auto, only if volume OK)
                           │           │
┌──────────┐  breach    ┌──────────┐  breach   ┌──────────────┐
│  (idle)  │ ─────────► │  WATCH   │ confirmed │   CRITICAL   │
│          │            │ (hour 1) │ ─────────►│  (hour 2+)   │
└──────────┘            └──────────┘  (1h gap) └──────────────┘
                             │                       │
                             └── Slack + RCA bot ────┘
```

- **WATCH** — first hour of breach detected. A "potential issue" alert is
  posted. The AI RCA bot is tagged with a 1-hour analysis window.
- **CRITICAL** — breach confirmed in the **next consecutive** hour. An
  "action required" alert is posted. The AI RCA bot gets the full multi-hour
  window.
- **RECOVERED** — SR returns above threshold with sufficient volume. A
  recovery message is posted and the alert is closed.
- **CLEARED** — watch entry expires without escalating (gap in data, or SR
  recovered before hour 2). Silently removed from state.

State is persisted in Hive (`hive.demo_schema.sr_alert_state_log`) so it
survives Airflow restarts and pod recycling.

---

## Payment method segmentation

Each transaction is classified into one of 9 demo buckets by card
attributes and transaction type (names are illustrative — replace with
your own taxonomy):

| Bucket | Description |
|---|---|
| Demo Payment Method 1 | Card type A — One-time |
| Demo Payment Method 2 | Card type B — One-time |
| Demo Payment Method 3 | Card type C — One-time |
| Demo Payment Method 4 | Card type A — Recurring (auto) |
| Demo Payment Method 5 | Card type A — Recurring (initial/change) |
| Demo Payment Method 6 | Card type B — Recurring (auto) |
| Demo Payment Method 7 | Card type B — Recurring (initial/change) |
| Demo Payment Method 8 | Card type C — Recurring (auto) |
| Demo Payment Method 9 | Card type C — Recurring (initial/change) |

An **Overall** row (all segments combined) is also checked. Each merchant
config specifies which segments to watch and their individual thresholds.

---

## Key design decisions

| Decision | Rationale |
|---|---|
| **Pod isolation** | Script crashes never affect the Airflow worker or other DAGs. Resource limits can be tuned per pod without touching the scheduler. |
| **Connections via env vars in pod** | `KubernetesPodOperator` has no Airflow task-runner context inside the pod. `BaseHook.get_connection()` resolves from `AIRFLOW_CONN_*` env vars, which are pre-mounted by `make_kubernetes_python_task()`. |
| **Stateful state machine (2-hour confirmation)** | A single-hour anomaly (network blip, delayed data) should not page an on-call engineer. Requiring two consecutive hours filters noise. |
| **Explicit `toTimeZone(..., 'Asia/Kolkata')`** | ClickHouse server timezone is unreliable in cloud deployments. IST is baked into every timestamp operation rather than trusting a server setting. |
| **Hive for state storage** | State must survive across hourly runs and Airflow restarts. Hive (via Trino) is the existing data-lake store, avoiding a new dependency. |
| **AI RCA bot trigger** | Every alert fires a tagged Slack message to an AI bot that queries the failure window and posts a root-cause summary automatically, reducing MTTR without manual analyst involvement. |
| **Per-merchant per-segment thresholds** | Different merchants have very different SR baselines. A 60 % SR is fine for some and a P0 for others. Thresholds are configured per merchant × segment. |
| **Low-volume guard on RECOVERED** | If volume drops below `min_attempts`, an SR of 100 % is meaningless. Recovery is only declared when volume is sufficient to be statistically meaningful. |
| **`retries=0` on the DAG** | A retry after a Trino or ClickHouse timeout makes things worse. The pod fails fast; on-call investigates with full pod logs. |

---

## Sample Slack alert

```
*Demo Merchant 1 || P0 SR Alert*
Generated at: 2026-01-15 14:15:00 IST
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@alice @bob @charlie
🚨 *CRITICAL ALERT (Action Required)*

• Segment: Demo Payment Method 1
• Started: 12:00 | Duration: 2 hours (12:00–14:00)
• Threshold: >50 attempts, <57% SR
• Hour 1 (12:00): 312 attempts | 49.4% SR
• Hour 2 (13:00): 287 attempts | 46.1% SR
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@DEMO_RCA_BOT skills:sr-rca-analyzer
Why was Demo Merchant 1 (mid DEMO_MID_001) Demo Payment Method 1 SR low
between 12:00 and 14:00 IST on 2026-01-15?
```

---

## Stack

| Component | Technology |
|---|---|
| Orchestration | Apache Airflow v3 |
| Pod execution | Kubernetes (`KubernetesPodOperator`) |
| SR data | ClickHouse (columnar DB) |
| State storage | Hive via Trino |
| Alerting | Slack Webhooks |
| Timezone handling | `pytz` |
| Language | Python 3.11+ |

---

## Configuration

All credentials live in the Airflow connection/variable store — no secrets in
DAG or script code.

| Connection / Variable | Used for |
|---|---|
| `clickhouse_readonly_default` | ClickHouse host, port, read-only credentials |
| `slack_default` | Slack webhook for alert delivery |
| `presto_default` | Trino host for state table reads/writes |
| `AIRFLOW_VAR_DEFAULT_CATALOG` | Trino default catalog (rendered into pod env) |

---

## File structure

```
p0-payment-alert-agent/
├── dag.py          # Airflow DAG — launches the Kubernetes pod
├── script.py       # Script that runs inside the pod
├── requirements.txt
└── README.md
```

> **Note:** `lib/kube.py`, `lib/presto.py`, `lib/slack.py` and `lib/email.py`
> are internal helpers that wrap `KubernetesPodOperator`, Trino CLI, Slack
> webhooks and Airflow email utilities. They are not included here as they
> contain organisation-specific routing logic.

---

## What I'd add next

- [ ] Prometheus metrics export (attempts + SR per segment as gauges)
- [ ] Dead-man's-switch alert: page on-call if the pod fails to run at all
- [ ] Unit tests for `classify_alerts` and the state machine transitions
- [ ] Configurable per-run lookback window (currently fixed at 2 hours)

---

*Built as part of a payments observability platform. Merchant IDs, Slack user
IDs, channel IDs, and internal table names have been anonymised.*
