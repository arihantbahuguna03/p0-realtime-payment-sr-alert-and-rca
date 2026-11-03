# P0 Payment SR Alert Agent

Real-time success rate monitoring for international card payments, with automated root cause analysis. Detects SR drops, tracks alert state across hourly runs, and auto-triggers an AI RCA bot on every breach — giving the on-call team structured context before they've finished reading the alert.

> Built Aug 2024–Aug 2025 using the Anthropic SDK, Apache Airflow, and Kubernetes — before MCP, Claude Code, and modern agentic tooling existed in production-ready form. Raw API integration on a production-grade infra stack.

---

## The Problem

International card payment SR drops went undetected for hours. By the time an engineer noticed in a dashboard, the failure window was 2–3 hours old and thousands of transactions had failed silently. The on-call team needed:

1. **Early detection** — catch a drop in the first hour, not after a manual check
2. **Noise reduction** — don't fire on a single bad hour; confirm before escalating
3. **Instant context** — when an alert fires, automatically start the RCA so the team isn't starting from zero

---

## What It Does

Runs every hour. For each monitored merchant:

1. Fetches the last completed hour's SR data from ClickHouse (per payment segment)
2. Checks each segment against calibrated per-merchant thresholds
3. Applies a WATCH → CRITICAL → RECOVERED state machine
4. Posts to Slack and auto-triggers an AI RCA bot on every breach

**WATCH alert** (potential issue — 1 hour of breach):
```
*Demo Merchant 1 || P0 SR Alert*
Generated at: 2026-01-15 14:15:00 IST
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@oncall-pm @oncall-eng
⚠️  WATCH LIST (Potential Alert)

• Segment: Credit Card (Recurring - Auto)
• Hour: 13:00
• Threshold: >61 attempts, <60% SR
• Detected: 88 attempts | 54.3% SR
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

**CRITICAL alert** (confirmed — 2 consecutive hours of breach):
```
*Demo Merchant 1 || P0 SR Alert*
Generated at: 2026-01-15 15:15:00 IST
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@oncall-pm @oncall-eng @team-lead
🚨 CRITICAL ALERT (Action Required)

• Segment: Credit Card (Recurring - Auto)
• Started: 13:00 | Duration: 2 hours (13:00–15:00)
• Threshold: >61 attempts, <60% SR
• Hour 1 (13:00): 88 attempts | 54.3% SR
• Hour 2 (14:00): 94 attempts | 51.8% SR
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

Each alert is immediately followed by an AI RCA bot message:
```
@rca-bot skills:sr-rca-analyzer
Why was Demo Merchant 1 Credit Card (Recurring - Auto) SR low
between 13:00 and 15:00 IST on 2026-01-15?
```

The RCA bot analyses the failure window and posts a structured root-cause summary — automatically, before anyone has to ask.

---

## Architecture

```
Airflow Scheduler (cron: 15 * * * *)
    └── KubernetesPodOperator
            └── Isolated K8s Pod
                    │
                    ├── ClickHouse (read-only)
                    │     └── SR query: per-merchant, per-segment, last 2 hours
                    │
                    ├── detect_alerts()
                    │     └── checks last completed hour vs per-segment thresholds
                    │
                    ├── Hive via Trino
                    │     └── load_state() — WATCH/ACTIVE state per merchant/segment
                    │
                    ├── classify_alerts()  ← state machine
                    │     ├── WATCH     (new breach, 1 hour)
                    │     ├── CRITICAL  (breach confirmed, 2+ consecutive hours)
                    │     └── RECOVERED (breach cleared, volume sufficient)
                    │
                    ├── Slack
                    │     ├── human alert message (WATCH / CRITICAL / RECOVERED)
                    │     └── AI RCA bot message (auto-tagged after every breach)
                    │
                    └── Hive via Trino
                          └── save_state() — persist updated state + history record
```

---

## Key Design Decisions

**1. Kubernetes pod isolation**

The alert script runs inside a `KubernetesPodOperator` pod, not directly on the Airflow worker. This means:
- A script crash, OOM, or timeout never affects the Airflow worker or other DAGs
- The pod gets its own CPU/memory limits independent of the shared worker pool
- Secrets are mounted as env vars from the secrets store — never in DAG code
- The pod can be restarted or debugged independently

The DAG's only job is scheduling and failure notification. All data work happens inside the pod.

**2. State persisted to Hive, not in-memory**

Alert state (which segments are in WATCH, which are CRITICAL) is written to a Hive table via Trino after every run. This means:
- State survives pod restarts, Airflow worker restarts, and infra incidents
- No false re-alerts when a pod crashes mid-run and restarts
- Full audit history: every WATCH/CRITICAL/RECOVERED event is recorded with timestamp, SR value, and threshold

**3. WATCH → CRITICAL two-step to reduce noise**

A single bad hour could be a transient ClickHouse lag, a brief gateway blip, or low-volume statistical noise. The two-step state machine only escalates to CRITICAL when the breach holds for 2 consecutive hours. WATCH fires first so the team has early visibility without being paged.

Edge case handled: if the WATCH entry is more than 1 hour old when the next breach arrives (gap detected — the hour in between was clean), the WATCH is refreshed rather than escalated. This prevents false CRITICALs from intermittent issues.

**4. Per-merchant, per-segment thresholds**

A single global SR threshold would produce false positives for naturally lower-SR segments and miss real drops on high-SR ones. Thresholds are calibrated per merchant per segment based on their historical volume profile and baseline SR. A segment is only evaluated if its volume exceeds `min_attempts` — avoiding alerts on statistically insignificant sample sizes.

**5. AI RCA bot auto-triggered on every alert**

Instead of the on-call team having to manually investigate, an AI RCA bot is tagged in a follow-up message immediately after every WATCH or CRITICAL. The bot receives a structured query (merchant, segment, time window) and posts a root-cause summary. By the time the engineer reads the alert, the RCA is already in the thread.

---

## Files

| File | Description |
|---|---|
| `dag.py` | Airflow DAG — schedules the K8s pod, handles failure callbacks |
| `script.py` | Alert script — runs inside the pod: SR fetch, detection, state machine, Slack posting |
| `merchant_config.example.py` | Template for merchant thresholds (copy → `merchant_config.py`, gitignored) |
| `requirements.txt` | Dependencies |

**Note on thresholds and SQL:** Per-merchant alert thresholds and the SR segmentation query contain calibrated production values. They are not included in this demo repo. `merchant_config.example.py` shows the expected structure. Available on request.

---

## Configuration

**Required Airflow connections (pre-mounted as env vars inside the pod):**
- `clickhouse_readonly_default` — ClickHouse host/port/user/password/schema
- `slack_default` — Slack webhook token
- `presto_default` — Trino/Presto host for Hive state table
- `AIRFLOW_VAR_DEFAULT_CATALOG` — Trino catalog name

**Merchant thresholds:**
Copy `merchant_config.example.py` → `merchant_config.py` and configure each merchant's per-segment `min_attempts` and `sr_threshold`.

---

## Stack

- Apache Airflow 2.9+ with KubernetesPodOperator
- ClickHouse (`clickhouse-driver`) — SR data source
- Hive via Trino (`trino`) — alert state persistence
- Slack SDK (`slack_sdk`) — alert delivery
- Python 3.11+, pytz

---

## What I'd Change Today

- Replace Hive/Trino state store with Redis — sub-millisecond reads, simpler infra, same durability with AOF persistence
- Add a volume-weighted SR baseline (rolling 7-day) so thresholds self-adjust for merchants with seasonal volume patterns
- Surface the AI RCA output back into the Airflow task log so post-incident review has a complete timeline in one place
