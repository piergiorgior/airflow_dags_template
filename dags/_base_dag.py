"""
_base_dag.py
============
Base class and shared utilities for all DAGs in this project.

Features:
- Default args with retry logic and exponential backoff
- Slack/email alerting on failure (configurable via Airflow Variables)
- Idempotency helpers
- Type-annotated throughout (Python 3.12+)

Usage:
    from _base_dag import default_args, on_failure_callback, build_dag_doc
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import DAG  # Airflow 3.x SDK
from airflow.models import Variable

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default arguments — inherited by all tasks in a DAG
# ---------------------------------------------------------------------------

def get_default_args(
    owner: str = "data-engineering",
    retries: int = 3,
    retry_delay_minutes: int = 5,
    retry_exponential_backoff: bool = True,
    email_on_failure: bool = False,
    email_on_retry: bool = False,
    emails: list[str] | None = None,
) -> dict[str, Any]:
    """
    Build a default_args dict for a DAG.

    Args:
        owner:                      Team or person responsible for the DAG.
        retries:                    Number of retries before marking task as failed.
        retry_delay_minutes:        Base delay between retries (minutes).
        retry_exponential_backoff:  If True, delay doubles at each retry attempt.
        email_on_failure:           Send email on task failure.
        email_on_retry:             Send email on task retry.
        emails:                     List of email addresses for alerts.

    Returns:
        dict compatible with DAG(default_args=...)
    """
    return {
        "owner": owner,
        "retries": retries,
        "retry_delay": timedelta(minutes=retry_delay_minutes),
        "retry_exponential_backoff": retry_exponential_backoff,
        "email_on_failure": email_on_failure,
        "email_on_retry": email_on_retry,
        "email": emails or [],
        # Never catch up on missed runs — important for idempotent pipelines
        "depends_on_past": False,
    }


# ---------------------------------------------------------------------------
# Failure callback
# ---------------------------------------------------------------------------

def on_failure_callback(context: dict[str, Any]) -> None:
    """
    Callback triggered on task failure.

    Reads Airflow Variables at runtime so you can toggle alerting
    without redeploying DAGs:
        - alert_slack_enabled  (default: "false")
        - alert_slack_webhook  (Slack Incoming Webhook URL)

    Args:
        context: Airflow task context dict passed automatically.
    """
    dag_id: str = context["dag"].dag_id
    task_id: str = context["task_instance"].task_id
    run_id: str = context["run_id"]
    logical_date: datetime = context["logical_date"]
    exception: BaseException | None = context.get("exception")

    log.error(
        "Task failed | dag=%s | task=%s | run_id=%s | logical_date=%s | error=%s",
        dag_id,
        task_id,
        run_id,
        logical_date,
        exception,
    )

    # --- Slack alert (opt-in via Airflow Variable) ---
    slack_enabled = Variable.get("alert_slack_enabled", default_var="false").lower()
    if slack_enabled == "true":
        _send_slack_alert(dag_id, task_id, run_id, logical_date, exception)


def _send_slack_alert(
    dag_id: str,
    task_id: str,
    run_id: str,
    logical_date: datetime,
    exception: BaseException | None,
) -> None:
    """Send a Slack notification via Incoming Webhook."""
    try:
        import requests  # noqa: PLC0415

        webhook_url = Variable.get("alert_slack_webhook")
        message = {
            "text": (
                f":red_circle: *Airflow Task Failed*\n"
                f"• *DAG*: `{dag_id}`\n"
                f"• *Task*: `{task_id}`\n"
                f"• *Run ID*: `{run_id}`\n"
                f"• *Logical date*: `{logical_date}`\n"
                f"• *Error*: `{exception}`"
            )
        }
        response = requests.post(webhook_url, json=message, timeout=10)
        response.raise_for_status()
        log.info("Slack alert sent successfully.")
    except Exception as exc:  # noqa: BLE001
        # Never let alerting break the pipeline reporting
        log.warning("Failed to send Slack alert: %s", exc)


# ---------------------------------------------------------------------------
# Idempotency helpers
# ---------------------------------------------------------------------------

def build_idempotent_run_key(dag_id: str, logical_date: datetime) -> str:
    """
    Build a unique key for a DAG run, useful for deduplication logic
    in tasks that write to databases or object storage.

    Example:
        key = build_idempotent_run_key(dag_id, context["logical_date"])
        # → "etl_postgres_to_clickhouse__2024-03-15T00:00:00"

    Args:
        dag_id:       DAG identifier.
        logical_date: The logical date of the run (formerly execution_date).

    Returns:
        A string key unique per DAG + run date.
    """
    return f"{dag_id}__{logical_date.strftime('%Y-%m-%dT%H:%M:%S')}"


# ---------------------------------------------------------------------------
# DAG docstring helper
# ---------------------------------------------------------------------------

def build_dag_doc(
    description: str,
    schedule: str,
    owner: str,
    source: str | None = None,
    destination: str | None = None,
) -> str:
    """
    Generate a standardised docstring for a DAG, visible in the Airflow UI.

    Args:
        description:  One-line description of what the DAG does.
        schedule:     Cron expression or preset (e.g. "@daily").
        owner:        Team or person responsible.
        source:       Data source (optional).
        destination:  Data destination (optional).

    Returns:
        Formatted markdown string for DAG.doc_md.
    """
    lines = [
        f"## {description}",
        "",
        f"| Field       | Value         |",
        f"|-------------|---------------|",
        f"| **Owner**   | {owner}       |",
        f"| **Schedule**| `{schedule}`  |",
    ]
    if source:
        lines.append(f"| **Source**  | {source}      |")
    if destination:
        lines.append(f"| **Dest**    | {destination} |")

    lines += [
        "",
        "### Retry policy",
        "Tasks retry up to **3 times** with exponential backoff (5 min base delay).",
        "",
        "### Idempotency",
        "Each run is keyed by `dag_id + logical_date`. Re-running the same date",
        "is safe and will not produce duplicate records.",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Convenience factory — returns a pre-configured DAG context manager
# ---------------------------------------------------------------------------

def make_dag(
    dag_id: str,
    description: str,
    schedule: str,
    start_date: datetime,
    owner: str = "data-engineering",
    tags: list[str] | None = None,
    catchup: bool = False,
    source: str | None = None,
    destination: str | None = None,
    extra_default_args: dict[str, Any] | None = None,
) -> DAG:
    """
    Factory that returns a fully configured DAG instance.

    Args:
        dag_id:            Unique DAG identifier.
        description:       Short description shown in the UI.
        schedule:          Cron or preset string (e.g. "0 6 * * *", "@daily").
        start_date:        DAG start date.
        owner:             Owning team or person.
        tags:              List of UI tags for filtering.
        catchup:           Whether to backfill missed runs (default False).
        source:            Source system label for docs.
        destination:       Destination system label for docs.
        extra_default_args: Any additional keys to merge into default_args.

    Returns:
        Configured airflow.sdk.DAG instance.

    Example::

        with make_dag(
            dag_id="etl_postgres_to_clickhouse",
            description="Hourly ETL from Postgres to ClickHouse",
            schedule="0 * * * *",
            start_date=datetime(2024, 1, 1),
            tags=["etl", "postgres", "clickhouse"],
        ) as dag:
            ...
    """
    args = get_default_args(owner=owner)
    if extra_default_args:
        args.update(extra_default_args)

    return DAG(
        dag_id=dag_id,
        description=description,
        default_args=args,
        schedule=schedule,
        start_date=start_date,
        catchup=catchup,
        tags=tags or [],
        doc_md=build_dag_doc(
            description=description,
            schedule=schedule,
            owner=owner,
            source=source,
            destination=destination,
        ),
        on_failure_callback=on_failure_callback,
    )