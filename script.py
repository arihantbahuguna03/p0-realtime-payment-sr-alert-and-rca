"""
P0 Payment SR Alert Script — Kubernetes Pod
============================================
Runs inside an isolated Kubernetes pod (launched by dag.py).
Detects payment SR drops and posts structured Slack alerts with
WATCH → CRITICAL → RECOVERED state tracking.

Data sources
------------
  SR data  : ClickHouse  (columnar DB, IST-explicit window)
             Fast columnar reads, per-merchant per-segment SR
  State    : Hive via Trino (persistent alert state log)
             Persists alert state across hourly runs

Alert state machine
--------------------
  WATCH     — SR breach detected for 1 hour (potential issue)
  CRITICAL  — SR breach confirmed for 2 consecutive hours (action required)
  RECOVERED — SR returned above threshold (auto-resolved)

For every WATCH or CRITICAL alert, a second message is posted
tagging an AI RCA bot, which automatically analyses the failure
window and posts a root-cause summary.

Pod note: no Airflow task-runner context inside the pod.
Connections resolve from env vars only (AIRFLOW_CONN_* / AIRFLOW_VAR_*),
pre-mounted by the DAG's make_kubernetes_python_task().

Owner: demo-owner@demo.com
"""
import json
import logging
import sys
import time
import uuid
from datetime import datetime, timedelta

import pytz

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
)
logger = logging.getLogger(__name__)
IST = pytz.timezone('Asia/Kolkata')

DAG_OWNER = 'demo-owner@demo.com'

# AI RCA bot — tagged in a follow-up message after every WATCH/CRITICAL alert.
# It analyses the failure window and posts a root-cause summary automatically.
RCA_BOT_ID  = 'DEMO_RCA_BOT_ID'
RCA_SKILL   = 'sr-rca-analyzer'


# ============================================================================
# CLICKHOUSE CONNECTION
# ============================================================================

def get_clickhouse_client():
    """
    Build ClickHouse client from the Airflow connection store.
    Inside the pod, the connection resolves from the
    AIRFLOW_CONN_CLICKHOUSE_READONLY_DEFAULT env var.
    """
    from clickhouse_driver import Client as CHClient
    from airflow.sdk import BaseHook

    conn = BaseHook.get_connection('clickhouse_readonly_default')
    logger.info(
        "Connecting to ClickHouse host=%s port=%s user=%s db=%s",
        conn.host, conn.port or 9000, conn.login, conn.schema or 'default',
    )
    return CHClient(
        host=conn.host,
        port=conn.port or 9000,
        user=conn.login,
        password=conn.password,
        database=conn.schema or 'default',
        connect_timeout=10,
        send_receive_timeout=300,
        sync_request_timeout=5,
    )


# ============================================================================
# MERCHANT CONFIG
# ============================================================================
# Loaded from merchant_config.py (gitignored — not in this demo repo).
# See merchant_config.example.py for the expected structure.
#
# Each merchant entry contains:
#   merchant_id            — data warehouse key
#   thresholds             — per-payment-method {min_attempts, sr_threshold}
#   mention_users_watch    — Slack users tagged on WATCH alerts
#   mention_users_critical — Slack users tagged on CRITICAL alerts

def _load_merchants() -> list[dict]:
    """
    Load merchant config with per-segment alert thresholds.

    Thresholds are calibrated per merchant based on their historical
    volume profile and business SLA. They are not included in this
    demo repo — see merchant_config.example.py for structure.

    Implementation: proprietary — available on request.
    """
    try:
        from merchant_config import MERCHANTS
        return MERCHANTS
    except ImportError:
        raise RuntimeError(
            "merchant_config.py not found. "
            "Copy merchant_config.example.py → merchant_config.py and fill in your values."
        )

MERCHANTS = _load_merchants()


# ============================================================================
# SQL — SR DATA
# ============================================================================

def _load_sr_query() -> str:
    """
    Returns the ClickHouse SQL that fetches per-segment success rates
    for a single merchant over the last 2 completed hours (IST-explicit).

    Query design:
      - Maps transactions to one of nine payment segments using a CASE
        block keyed on card type and recurrence pattern
      - Appends an Overall row (no segmentation) via UNION ALL so a
        single query returns both granularities
      - Excludes a known non-actionable error code from the denominator
        so it doesn't mask genuine SR degradation
      - All timestamps cast to IST explicitly to be independent of the
        ClickHouse server's system timezone

    Implementation: proprietary — available on request.
    """
    raise NotImplementedError(
        "Production SR query not included in demo repo. "
        "Contact demo-owner@demo.com to discuss implementation."
    )


# ============================================================================
# SQL — STATE TABLE  (Hive via Trino)
# ============================================================================
# Persists alert state so the script can track WATCH → CRITICAL → RECOVERED
# across hourly runs without holding anything in memory.

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS hive.demo_schema.sr_alert_state_log (
    alert_id        VARCHAR,
    merchant_id     VARCHAR,
    merchant_name   VARCHAR,
    payment_segment VARCHAR,
    alert_type      VARCHAR,
    record_type     VARCHAR,
    alert_system    VARCHAR,
    breach_hour     VARCHAR,
    sr_value        DOUBLE,
    attempts        BIGINT,
    sr_threshold    VARCHAR,
    fired_at        VARCHAR,
    month           VARCHAR,
    env             VARCHAR
) WITH (format = 'PARQUET')
"""


# ============================================================================
# STATE MANAGEMENT
# ============================================================================

def load_state(merchant_id: str, merchant_name: str) -> dict:
    """
    Load the latest alert state for a merchant from Hive.
    Returns: {watch_list_posted: {segment: {...}}, active_alerts: {segment: {...}}}

    Uses ROW_NUMBER() OVER PARTITION to get the most recent record per segment,
    avoiding duplicates from multiple runs.
    """
    from lib import presto

    query = f"""
        SELECT payment_segment, alert_type, breach_hour,
               sr_value, attempts, alert_id
        FROM (
            SELECT
                payment_segment, alert_type, breach_hour,
                sr_value, attempts, alert_id,
                ROW_NUMBER() OVER (
                    PARTITION BY merchant_id, merchant_name, payment_segment
                    ORDER BY fired_at DESC, alert_id DESC
                ) AS rn
            FROM hive.demo_schema.sr_alert_state_log
            WHERE merchant_id    = '{merchant_id}'
              AND merchant_name  = '{merchant_name}'
              AND record_type    = 'state'
              AND alert_system   = 'P0_SR'
              AND fired_at >= DATE_FORMAT(
                  CURRENT_TIMESTAMP - INTERVAL '7' DAY,
                  '%Y-%m-%d %H:%i:%S'
              )
        ) t
        WHERE rn = 1
    """
    for attempt in range(2):
        try:
            result = presto.execute_presto_query_cli_with_result(
                query, user=DAG_OWNER,
            )
            break
        except Exception as e:
            err = str(e)
            if 'does not exist' in err or 'TABLE_NOT_FOUND' in err:
                logger.info("State table not found — starting with empty state.")
                return {'watch_list_posted': {}, 'active_alerts': {}}
            permanent = any(x in err for x in [
                'SYNTAX_ERROR', 'COLUMN_NOT_FOUND', 'line ', 'not authenticated',
            ])
            if attempt == 0 and not permanent:
                logger.warning("Transient Trino error — retrying in 5s: %s", err)
                time.sleep(5)
                continue
            raise

    state = {'watch_list_posted': {}, 'active_alerts': {}}
    if not result or not result.strip():
        return state

    for line in result.strip().split('\n'):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
            segment  = row['payment_segment']
            alert_id = row.get('alert_id', str(uuid.uuid4()))
            if row['alert_type'] == 'WATCH_LIST':
                state['watch_list_posted'][segment] = {
                    'hour': row['breach_hour'],
                    'attempts': row['attempts'],
                    'sr': row['sr_value'],
                    'alert_id': alert_id,
                    'threshold_str': '',
                }
            elif row['alert_type'] == 'ACTIVE_ALERT':
                state['active_alerts'][segment] = {
                    'start_hour': row['breach_hour'],
                    'alert_id': alert_id,
                }
        except (json.JSONDecodeError, KeyError):
            continue
    return state


def save_state(merchant_id: str, merchant_name: str, state: dict,
               classified: dict, cleared_watch_list: list) -> None:
    """Persist updated alert state + history records to Hive via Trino."""
    from lib import presto

    fired_at = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')
    month    = datetime.now(IST).strftime('%Y-%m')

    def insert(aid, segment, atype, rtype, bhour, srv, att, thr):
        sv = str(float(srv)) if srv is not None else 'NULL'
        seg = segment.replace("'", "''")
        thr = thr.replace("'", "''")
        q = (
            "INSERT INTO hive.demo_schema.sr_alert_state_log "
            f"VALUES ('{aid}','{merchant_id}','{merchant_name}',"
            f"'{seg}','{atype}','{rtype}','P0_SR','{bhour}',"
            f"{sv},{int(att)},'{thr}','{fired_at}','{month}','airflow')"
        )
        presto.execute_presto_query_cli(q, user=DAG_OWNER)

    # Current state snapshot
    for seg, d in state.get('watch_list_posted', {}).items():
        insert(d['alert_id'], seg, 'WATCH_LIST', 'state',
               d['hour'], d['sr'], d['attempts'], d.get('threshold_str', ''))
    for seg, d in state.get('active_alerts', {}).items():
        insert(d['alert_id'], seg, 'ACTIVE_ALERT', 'state',
               d['start_hour'], None, 0, '')

    # History records
    for a in classified.get('watch_list', []):
        thr = f">{a['min_attempts']} attempts, <{a['sr_threshold']}% SR"
        insert(a['alert_id'], a['payment_segment'], 'WATCH_LIST', 'history',
               a['hour'], a['sr'], a['attempts'], thr)
    for a in classified.get('critical', []):
        h1  = a['hours'][0]
        thr = f">{a['min_attempts']} attempts, <{a['sr_threshold']}% SR"
        insert(a['alert_id'], a['payment_segment'], 'CRITICAL', 'history',
               a['start_hour'], float(h1[2]), int(h1[1]), thr)
    for a in classified.get('recovered', []):
        insert(a['alert_id'], a['payment_segment'], 'RECOVERED', 'history',
               a['start_hour'], a.get('recovery_sr'), a.get('recovery_attempts', 0), '')
        insert(a['alert_id'], a['payment_segment'], 'RECOVERED', 'state',
               a['start_hour'], a.get('recovery_sr'), a.get('recovery_attempts', 0), '')
    for item in cleared_watch_list:
        insert(item['alert_id'], item['payment_segment'], 'CLEARED', 'state',
               item['hour'], None, 0, '')


# ============================================================================
# ALERT DETECTION
# ============================================================================

def detect_alerts(sr_data: list, merchant: dict):
    """
    Check the most recently completed hour against per-segment thresholds.
    Returns (breaches, target_hour, current_hour_data).

    Skips the merchant entirely if the expected hour is missing from the data
    (e.g. ClickHouse lag) rather than false-alerting.
    """
    if not sr_data:
        return [], None, {}

    thresholds = merchant['thresholds']
    now_ist    = datetime.now(IST)
    target_dt  = now_ist.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    target_hour = target_dt.strftime('%Y-%m-%d %H:%M:%S')

    available = {r['hour_interval'] for r in sr_data}
    if target_hour not in available:
        logger.warning(
            "Expected hour %s not in data (available: %s) — skipping merchant.",
            target_hour, sorted(available),
        )
        return [], None, {}

    logger.info("Evaluating hour: %s", target_hour)
    breaches          = []
    current_hour_data = {}

    for row in sr_data:
        if row['hour_interval'] != target_hour:
            continue
        segment  = row['payment_segment']
        attempts = int(row['attempts'])
        sr       = float(row['success_rate'])
        current_hour_data[segment] = {'attempts': attempts, 'sr': sr}

        if segment in thresholds:
            t = thresholds[segment]
            if attempts >= t['min_attempts'] and sr < t['sr_threshold']:
                breaches.append({
                    'payment_segment': segment,
                    'hour':            target_hour,
                    'attempts':        attempts,
                    'sr':              sr,
                    'sr_threshold':    t['sr_threshold'],
                    'min_attempts':    t['min_attempts'],
                })

    return breaches, target_hour, current_hour_data


# ============================================================================
# ALERT CLASSIFICATION  (state machine)
# ============================================================================

def classify_alerts(current_breaches: list, state: dict,
                    current_hour=None, current_hour_data=None,
                    merchant=None) -> dict:
    """
    Apply the WATCH → CRITICAL → RECOVERED state machine.

    Rules:
      New breach, not in watch list    → WATCH
      New breach, in watch list 1h ago → CRITICAL (escalate)
      New breach, in watch list >1h ago → WATCH (refresh; gap detected)
      Active alert, breach gone, vol OK → RECOVERED
      Active alert, breach gone, low vol → keep active (insufficient data)
    """
    watch_list = []
    critical   = []
    recovered  = []
    wlp = state.get('watch_list_posted', {})
    aa  = state.get('active_alerts', {})
    cbm = {b['payment_segment']: b for b in current_breaches}
    chd = current_hour_data or {}
    thresholds = merchant.get('thresholds', {}) if merchant else {}

    for breach in current_breaches:
        seg = breach['payment_segment']
        if seg in aa:
            continue  # already critical — no re-escalation
        if seg in wlp:
            wd  = wlp[seg]
            wdt = datetime.strptime(wd['hour'], '%Y-%m-%d %H:%M:%S')
            cdt = datetime.strptime(breach['hour'], '%Y-%m-%d %H:%M:%S')
            if cdt - wdt == timedelta(hours=1):
                critical.append({
                    'payment_segment': seg,
                    'alert_id':   wd['alert_id'],
                    'start_hour': wd['hour'],
                    'hours': [
                        (wd['hour'], wd['attempts'], wd['sr']),
                        (breach['hour'], breach['attempts'], breach['sr']),
                    ],
                    'sr_threshold': breach['sr_threshold'],
                    'min_attempts': breach['min_attempts'],
                })
            else:
                logger.info("%s: gap > 1h — refreshing watch entry.", seg)
                watch_list.append({**breach, 'alert_id': wd['alert_id']})
            continue
        watch_list.append({**breach, 'alert_id': str(uuid.uuid4())})

    for seg, ad in aa.items():
        if seg not in cbm:
            mn = chd.get(seg, {})
            ca = mn.get('attempts', 0)
            cs = mn.get('sr', 0)
            t  = thresholds.get(seg, {})
            ma = t.get('min_attempts', 0)
            st = t.get('sr_threshold', 0)
            if ca < ma:
                logger.info("%s: low volume (%d < %d) — keeping active.", seg, ca, ma)
                continue
            if cs < st:
                logger.info("%s: SR still low (%.1f%% < %.1f%%) — keeping active.", seg, cs, st)
                continue
            recovered.append({
                'payment_segment':   seg,
                'alert_id':          ad['alert_id'],
                'start_hour':        ad['start_hour'],
                'recovery_hour':     current_hour,
                'recovery_sr':       cs,
                'recovery_attempts': ca,
            })

    return {'watch_list': watch_list, 'critical': critical, 'recovered': recovered}


def update_state_dict(state: dict, classified: dict,
                      current_breaches: list, merchant: dict):
    """Mutate state dict in-place and return (state, cleared_watch_list)."""
    thresholds = merchant['thresholds']
    wlp = state.get('watch_list_posted', {})
    aa  = state.get('active_alerts', {})
    cbm = {b['payment_segment']: b for b in current_breaches}

    for a in classified['watch_list']:
        t = thresholds.get(a['payment_segment'], {})
        wlp[a['payment_segment']] = {
            'hour':          a['hour'],
            'attempts':      a['attempts'],
            'sr':            a['sr'],
            'alert_id':      a['alert_id'],
            'threshold_str': (
                f">{t.get('min_attempts', '?')} attempts, "
                f"<{t.get('sr_threshold', '?')}% SR"
            ),
        }
    for a in classified['critical']:
        wlp.pop(a['payment_segment'], None)
        aa[a['payment_segment']] = {
            'start_hour': a['start_hour'],
            'alert_id':   a['alert_id'],
        }
    for a in classified['recovered']:
        aa.pop(a['payment_segment'], None)

    # Clear stale watch entries (breach gone without escalating)
    stale   = [seg for seg in wlp if seg not in cbm]
    cleared = [
        {'payment_segment': seg, 'alert_id': wlp[seg]['alert_id'], 'hour': wlp[seg]['hour']}
        for seg in stale
    ]
    for seg in stale:
        del wlp[seg]

    state['watch_list_posted'] = wlp
    state['active_alerts']     = aa
    return state, cleared


# ============================================================================
# MESSAGE FORMATTING
# ============================================================================

def format_watch_message(merchant: dict, alert: dict) -> str:
    thresholds = merchant['thresholds']
    mentions   = ' '.join(f"<@{u}>" for u in merchant.get('mention_users_watch', []))
    t          = thresholds.get(alert['payment_segment'], {})
    hour_str   = alert['hour'].split(' ')[1][:5]
    msg = (
        f"*{merchant['name']} || P0 SR Alert*\n"
        f"Generated at: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} IST\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    )
    if mentions:
        msg += f"{mentions}\n"
    msg += (
        "⚠️  *WATCH LIST (Potential Alert)*\n\n"
        f"• Segment: {alert['payment_segment']}\n"
        f"• Hour: {hour_str}\n"
        f"• Threshold: >{t.get('min_attempts','?')} attempts, <{t.get('sr_threshold','?')}% SR\n"
        f"• Detected: {alert['attempts']} attempts | {alert['sr']}% SR\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    )
    return msg


def format_critical_message(merchant: dict, alert: dict) -> str:
    thresholds = merchant['thresholds']
    mentions   = ' '.join(f"<@{u}>" for u in merchant.get('mention_users_critical', []))
    t          = thresholds.get(alert['payment_segment'], {})
    h          = alert['hours']
    st         = alert['start_hour'].split(' ')[1][:5]
    end_dt     = datetime.strptime(h[-1][0], '%Y-%m-%d %H:%M:%S') + timedelta(hours=1)
    et         = end_dt.strftime('%H:%M')
    h1t        = h[0][0].split(' ')[1][:5]
    h2t        = h[1][0].split(' ')[1][:5]
    msg = (
        f"*{merchant['name']} || P0 SR Alert*\n"
        f"Generated at: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} IST\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    )
    if mentions:
        msg += f"{mentions}\n"
    msg += (
        "🚨 *CRITICAL ALERT (Action Required)*\n\n"
        f"• Segment: {alert['payment_segment']}\n"
        f"• Started: {st} | Duration: {len(h)} hours ({st}–{et})\n"
        f"• Threshold: >{t.get('min_attempts','?')} attempts, <{t.get('sr_threshold','?')}% SR\n"
        f"• Hour 1 ({h1t}): {h[0][1]} attempts | {h[0][2]}% SR\n"
        f"• Hour 2 ({h2t}): {h[1][1]} attempts | {h[1][2]}% SR\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    )
    return msg


def format_recovered_message(merchant: dict, alert: dict) -> str:
    thresholds = merchant['thresholds']
    mentions   = ' '.join(f"<@{u}>" for u in merchant.get('mention_users_critical', []))
    t          = thresholds.get(alert['payment_segment'], {})
    ts         = (f">{t.get('min_attempts','?')} attempts, <{t.get('sr_threshold','?')}% SR"
                  if t else '')
    started    = alert['start_hour'].split(' ')[1][:5]
    msg = (
        f"*{merchant['name']} || P0 SR Alert*\n"
        f"Generated at: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} IST\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    )
    if mentions:
        msg += f"{mentions}\n"
    msg += f"✅ *RECOVERED*\n\n• Segment: {alert['payment_segment']}\n"
    msg += f"• Alert started: {started}\n"
    if alert.get('recovery_hour'):
        msg += f"• Recovered at: {alert['recovery_hour'].split(' ')[1][:5]}\n"
    if alert.get('recovery_sr') is not None:
        msg += f"• Current SR: {alert['recovery_sr']}%\n"
    if alert.get('recovery_attempts'):
        msg += f"• Current attempts: {alert['recovery_attempts']}\n"
    if ts:
        msg += f"• Threshold: {ts}\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    return msg


def format_rca_request(merchant: dict, tier: str, alert: dict) -> str:
    """
    Plain-English message tagging the AI RCA bot.
    The bot analyses the failure window and posts a root-cause summary.
    WATCH    → 1-hour window
    CRITICAL → from first breach hour to end of last breach hour
    """
    mid    = merchant['merchant_id']
    name   = merchant['name']
    seg    = alert['payment_segment']
    if tier == 'WATCH':
        start_dt = datetime.strptime(alert['hour'], '%Y-%m-%d %H:%M:%S')
        end_dt   = start_dt + timedelta(hours=1)
    else:
        h        = alert['hours']
        start_dt = datetime.strptime(h[0][0], '%Y-%m-%d %H:%M:%S')
        end_dt   = datetime.strptime(h[-1][0], '%Y-%m-%d %H:%M:%S') + timedelta(hours=1)
    date_str = start_dt.strftime('%Y-%m-%d')
    start_hm = start_dt.strftime('%H:%M')
    end_hm   = end_dt.strftime('%H:%M')
    return (
        f"<@{RCA_BOT_ID}> skills:{RCA_SKILL}\n"
        f"Why was {name} (mid {mid}) {seg} SR low "
        f"between {start_hm} and {end_hm} IST on {date_str}?"
    )


def post_to_slack(message: str, merchant: dict, mention_users=None) -> None:
    from lib import slack
    slack.notify(
        f"P0 SR Alert - {merchant['name']}",
        message,
        merchant['channel_id'],
        mention_users or [],
        {},
    )
    logger.info("Posted to Slack for %s.", merchant['name'])


# ============================================================================
# MAIN
# ============================================================================

def create_table_if_not_exists() -> None:
    from lib import presto
    logger.info("Ensuring state table exists...")
    presto.execute_presto_query_cli(CREATE_TABLE_SQL, user=DAG_OWNER)
    logger.info("State table ready.")


def main() -> None:
    create_table_if_not_exists()

    ch_client        = get_clickhouse_client()
    failed_merchants = []

    for merchant in MERCHANTS:
        mid   = merchant['merchant_id']
        mname = merchant['name']

        if not merchant.get('enabled', True):
            logger.info("Skipping %s — disabled.", mname)
            continue

        logger.info("=" * 60)
        logger.info("Processing %s (%s)", mname, mid)
        logger.info("=" * 60)

        try:
            # 1. Fetch SR data from ClickHouse
            logger.info("1. Fetching SR data from ClickHouse...")
            sr_data = None
            for attempt in range(2):
                try:
                    rows    = ch_client.execute(_load_sr_query().format(merchant_id=mid))
                    sr_data = [
                        dict(zip(['hour_interval', 'payment_segment', 'attempts', 'success_rate'], r))
                        for r in rows
                    ]
                    break
                except Exception as e:
                    if attempt == 0 and 'Quota' not in str(e):
                        logger.warning("ClickHouse error — retrying in 5s: %s", e)
                        time.sleep(5)
                        continue
                    raise
            logger.info("   → %d rows", len(sr_data))
            if not sr_data:
                logger.info("   No data — skipping.")
                continue

            # 2. Detect breaches in the last completed hour
            logger.info("2. Detecting SR breaches...")
            breaches, hour, hdata = detect_alerts(sr_data, merchant)
            logger.info("   → %d breach(es)", len(breaches))

            # 3. Load persisted state from Hive
            logger.info("3. Loading state from Hive...")
            state = load_state(mid, mname)
            logger.info("   watch=%d active=%d",
                        len(state['watch_list_posted']), len(state['active_alerts']))

            # 4. Classify: WATCH / CRITICAL / RECOVERED
            logger.info("4. Classifying alerts...")
            classified = classify_alerts(breaches, state, hour, hdata, merchant)
            logger.info("   watch=%d critical=%d recovered=%d",
                        len(classified['watch_list']),
                        len(classified['critical']),
                        len(classified['recovered']))

            # 5. Post to Slack — one human message + one RCA bot message per breach
            posted = False
            for a in classified['watch_list']:
                post_to_slack(format_watch_message(merchant, a), merchant,
                              merchant.get('mention_users_watch', []))
                post_to_slack(format_rca_request(merchant, 'WATCH', a),
                              merchant, [RCA_BOT_ID])
                posted = True
            for a in classified['critical']:
                post_to_slack(format_critical_message(merchant, a), merchant,
                              merchant.get('mention_users_critical', []))
                post_to_slack(format_rca_request(merchant, 'CRITICAL', a),
                              merchant, [RCA_BOT_ID])
                posted = True
            for a in classified['recovered']:
                post_to_slack(format_recovered_message(merchant, a), merchant,
                              merchant.get('mention_users_critical', []))
                posted = True
            if not posted:
                logger.info("5. No alerts to post.")

            # 6. Persist updated state
            logger.info("6. Saving state to Hive...")
            state, cleared = update_state_dict(state, classified, breaches, merchant)
            save_state(mid, mname, state, classified, cleared)
            logger.info("   Done.")

        except Exception:
            logger.exception("Failed processing %s (%s)", mname, mid)
            failed_merchants.append(mname)
            continue

    ch_client.disconnect()

    if failed_merchants:
        logger.error("Failed merchants: %s", ', '.join(failed_merchants))
        sys.exit(1)


if __name__ == '__main__':
    main()
