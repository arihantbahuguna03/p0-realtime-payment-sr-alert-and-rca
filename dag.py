"""
P0 Payment SR Alert Agent — Airflow DAG (Kubernetes Pod)
=========================================================
Orchestrates the SR alert script on an isolated Kubernetes pod.
Airflow's only job here is scheduling and failure notification —
all heavy work (SQL, data filtering, alerting) runs inside the pod.

Why Kubernetes pod isolation?
------------------------------
Running the script inside a KubernetesPodOperator means:
  - A script crash never affects the Airflow worker or other DAGs
  - The pod gets its own CPU/memory limits
  - Secrets are mounted as env vars, never in DAG code
  - The pod can be debugged/restarted independently

Why env vars for connections inside the pod?
---------------------------------------------
In Airflow v3, a KubernetesPodOperator pod has no task-runner
context. BaseHook.get_connection() and Variable.get() inside the
pod resolve only from environment variables
(AIRFLOW_CONN_* / AIRFLOW_VAR_*), never from the Airflow metadata
DB. All secrets are pre-mounted from the secrets store into the pod
by make_kubernetes_python_task().

Connection dependencies (seed before enabling):
  - clickhouse_readonly_default  — ClickHouse SR data source
  - slack_default                — Slack webhook for alert delivery
  - AIRFLOW_VAR_SLACK_UTIL_APP_SECRET — Slack mention resolution
  - Trino: presto_default connection + default_catalog variable,
    rendered into the pod as host/catalog by make_presto_env_vars()

Schedule: Hourly at :15
Owner   : demo-owner@demo.com
"""
from datetime import datetime, timedelta
from functools import partial

from airflow.sdk import DAG
from lib import email
from lib.kube import make_kubernetes_python_task, make_presto_env_vars

DAG_ID    = 'p0_sr_alert_agent'
DAG_OWNER = 'demo-owner@demo.com'
DAG_OWNERS = [DAG_OWNER]

# Script runs inside the Kubernetes pod (not on the Airflow worker)
SCRIPT_PATH = 'python/payments/scripts/p0_sr_alert_script.py'

tags_dct = {
    'org.team':          'payments-platform',
    'org.business-unit': 'merchant-ops',
}

default_args = {
    'owner':               DAG_OWNER,
    'start_date':          datetime(2026, 1, 1),
    'on_failure_callback': partial(email.send_mail_slack, DAG_OWNERS),
    'retries':             0,           # pod failures are logged, not retried
    'execution_timeout':   timedelta(hours=2),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule='15 * * * *',             # hourly at :15
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    tags=['payments', 'p0-alerts', 'sr-monitoring', 'kubernetes'],
    description=(
        'P0 SR alert agent for international card payments. '
        'Runs on an isolated Kubernetes pod via KubernetesPodOperator '
        'so script failures never crash the Airflow worker.'
    ),
    doc_md=__doc__,
) as dag:

    # Airflow does ONE thing: launch the pod.
    # Everything else (queries, filtering, alerting) happens inside it.
    run_alert = make_kubernetes_python_task(
        task_id='run_p0_sr_alert',
        airflow_dag_id=DAG_ID,
        dag_owner=DAG_OWNER,
        name='p0-sr-alert-agent',
        script_path=SCRIPT_PATH,
        tags=tags_dct,
        extra_env_vars=make_presto_env_vars(),  # injects Trino host/catalog
    )

    run_alert
