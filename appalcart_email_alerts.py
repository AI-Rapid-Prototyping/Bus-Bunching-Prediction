#!/usr/bin/env python3
"""
Send AppalCart bus bunching email alerts from predicted-event CSV output.

Supports:
1) Daily digest for today's predicted bunching events.
2) One-hour-ahead alerts for events near a target lead time.
"""

from __future__ import annotations

import argparse
import os
import smtplib
import sqlite3
from datetime import datetime, timedelta
from email.message import EmailMessage
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

import pandas as pd


BASE_REQUIRED_COLUMNS = ["first_predicted_arrival", "route_id"]
OPTIONAL_COLUMNS = [
    "route_label",
    "route_name",
    "direction_id",
    "stop_id",
    "stop_name",
    "lead_trip_id",
    "trailing_trip_id",
    "event_date",
    "min_actual_headway_minutes",
    "bunch_threshold_minutes",
]


def parse_env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return int(value)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Send daily and one-hour-ahead AppalCart bus bunching email alerts."
    )
    parser.add_argument(
        "--mode",
        choices=["daily", "upcoming", "both"],
        default="both",
        help="Which alert mode to run. Default: %(default)s",
    )
    parser.add_argument(
        "--csv",
        default="Outputs/AppalCart/appalcart_bunching_events.csv",
        help="Path to AppalCart predicted bunching events CSV. Default: %(default)s",
    )
    parser.add_argument(
        "--timezone",
        default="America/New_York",
        help="Timezone used for alert windows and digest day. Default: %(default)s",
    )
    parser.add_argument(
        "--state-db",
        default="Outputs/AppalCart/appalcart_alert_state.db",
        help="SQLite state database for dedupe tracking. Default: %(default)s",
    )
    parser.add_argument(
        "--lead-minutes",
        type=int,
        default=60,
        help="Lead time for upcoming alerts. Default: %(default)s",
    )
    parser.add_argument(
        "--window-minutes",
        type=int,
        default=10,
        help="Window size around lead time for upcoming alerts. Default: %(default)s",
    )
    parser.add_argument(
        "--max-events-in-email",
        type=int,
        default=200,
        help="Maximum event lines included in an email body. Default: %(default)s",
    )
    parser.add_argument(
        "--send-empty-digest",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Send daily digest even when no events are predicted. Default: %(default)s",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Bypass dedupe checks and send alerts even if already sent.",
    )
    parser.add_argument(
        "--subject-prefix",
        default="[AppalCart]",
        help="Subject prefix for outgoing emails. Default: %(default)s",
    )
    parser.add_argument(
        "--from-address",
        default=os.getenv("APPALCART_ALERT_FROM"),
        help="SMTP from address (or APPALCART_ALERT_FROM).",
    )
    parser.add_argument(
        "--to-addresses",
        default=os.getenv("APPALCART_ALERT_TO"),
        help="Comma-separated recipients (or APPALCART_ALERT_TO).",
    )
    parser.add_argument(
        "--smtp-host",
        default=os.getenv("APPALCART_SMTP_HOST"),
        help="SMTP host (or APPALCART_SMTP_HOST).",
    )
    parser.add_argument(
        "--smtp-port",
        type=int,
        default=parse_env_int("APPALCART_SMTP_PORT", 587),
        help="SMTP port (or APPALCART_SMTP_PORT). Default: %(default)s",
    )
    parser.add_argument(
        "--smtp-user",
        default=os.getenv("APPALCART_SMTP_USER"),
        help="SMTP username (or APPALCART_SMTP_USER).",
    )
    parser.add_argument(
        "--smtp-password",
        default=os.getenv("APPALCART_SMTP_PASSWORD"),
        help="SMTP password (or APPALCART_SMTP_PASSWORD).",
    )
    parser.add_argument(
        "--smtp-starttls",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Use STARTTLS for non-SSL SMTP sessions. Default: %(default)s",
    )
    parser.add_argument(
        "--smtp-ssl",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Use implicit SSL SMTP connection. Default: %(default)s",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Render emails to stdout without sending or updating state.",
    )
    return parser.parse_args()


def ensure_required_columns(df: pd.DataFrame, required: Iterable[str]) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        missing_text = ", ".join(missing)
        raise ValueError(f"CSV is missing required columns: {missing_text}")


def load_events(csv_path: Path, timezone_name: str) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"Events CSV not found: {csv_path}")

    df = pd.read_csv(csv_path, dtype=str).fillna("")
    ensure_required_columns(df, BASE_REQUIRED_COLUMNS)

    for col in OPTIONAL_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    event_time_naive = pd.to_datetime(
        df["first_predicted_arrival"].astype(str).str.slice(0, 19),
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce",
    )
    df["event_time_local"] = event_time_naive.dt.tz_localize(
        timezone_name, ambiguous="NaT", nonexistent="shift_forward"
    )
    df = df.dropna(subset=["event_time_local"]).copy()

    if df.empty:
        df["event_key"] = []
        return df

    df["route_label"] = df["route_label"].where(df["route_label"] != "", df["route_id"])
    df["event_date"] = df["event_time_local"].dt.strftime("%Y-%m-%d")

    for numeric_col in ["min_actual_headway_minutes", "bunch_threshold_minutes"]:
        df[numeric_col] = pd.to_numeric(df[numeric_col], errors="coerce")

    key_parts = [
        df["event_time_local"].dt.strftime("%Y-%m-%dT%H:%M:%S%z"),
        df["route_id"].astype(str),
        df["direction_id"].astype(str),
        df["stop_id"].astype(str),
        df["lead_trip_id"].astype(str),
        df["trailing_trip_id"].astype(str),
    ]
    df["event_key"] = key_parts[0]
    for part in key_parts[1:]:
        df["event_key"] = df["event_key"] + "|" + part

    return df.sort_values("event_time_local")


def initialize_state(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sent_digests (
            digest_date TEXT PRIMARY KEY,
            sent_at_utc TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sent_events (
            event_key TEXT PRIMARY KEY,
            event_time_local TEXT NOT NULL,
            sent_at_utc TEXT NOT NULL
        )
        """
    )
    conn.commit()


def has_digest_been_sent(conn: sqlite3.Connection, digest_date: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sent_digests WHERE digest_date = ?",
        (digest_date,),
    ).fetchone()
    return row is not None


def mark_digest_sent(conn: sqlite3.Connection, digest_date: str) -> None:
    sent_at_utc = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    conn.execute(
        """
        INSERT OR REPLACE INTO sent_digests (digest_date, sent_at_utc)
        VALUES (?, ?)
        """,
        (digest_date, sent_at_utc),
    )
    conn.commit()


def chunked(values: list[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(values), size):
        yield values[i : i + size]


def filter_unsent_events(conn: sqlite3.Connection, events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return events

    keys = events["event_key"].astype(str).tolist()
    sent_keys: set[str] = set()
    for chunk in chunked(keys, 900):
        placeholders = ",".join("?" for _ in chunk)
        query = f"SELECT event_key FROM sent_events WHERE event_key IN ({placeholders})"
        rows = conn.execute(query, chunk).fetchall()
        sent_keys.update(row[0] for row in rows)
    return events[~events["event_key"].isin(sent_keys)].copy()


def mark_events_sent(conn: sqlite3.Connection, events: pd.DataFrame) -> None:
    if events.empty:
        return

    sent_at_utc = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    rows = [
        (
            row.event_key,
            row.event_time_local.isoformat(),
            sent_at_utc,
        )
        for row in events.itertuples()
    ]
    conn.executemany(
        """
        INSERT OR REPLACE INTO sent_events (event_key, event_time_local, sent_at_utc)
        VALUES (?, ?, ?)
        """,
        rows,
    )
    conn.commit()


def parse_recipients(value: str | None) -> list[str]:
    if value is None:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def format_optional_number(value: object) -> str:
    if value is None:
        return "n/a"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if pd.isna(number):
        return "n/a"
    return f"{number:.1f}"


def format_event_line(row: pd.Series) -> str:
    event_time = row["event_time_local"].strftime("%Y-%m-%d %H:%M %Z")
    route_label = str(row.get("route_label", "")).strip() or str(row.get("route_id", ""))
    direction_id = str(row.get("direction_id", "")).strip() or "unknown"
    stop_name = str(row.get("stop_name", "")).strip()
    stop_id = str(row.get("stop_id", "")).strip()
    stop_label = f"{stop_name} [{stop_id}]" if stop_name else f"Stop {stop_id}"
    gap = format_optional_number(row.get("min_actual_headway_minutes"))
    threshold = format_optional_number(row.get("bunch_threshold_minutes"))
    return (
        f"{event_time} | Route {route_label} dir {direction_id} | {stop_label} | "
        f"gap {gap} min (threshold {threshold} min)"
    )


def build_event_lines(events: pd.DataFrame, max_events: int) -> list[str]:
    if events.empty:
        return []
    events_sorted = events.sort_values("event_time_local")
    shown = events_sorted.head(max_events)
    lines = [format_event_line(row) for _, row in shown.iterrows()]
    hidden_count = len(events_sorted) - len(shown)
    if hidden_count > 0:
        lines.append(f"... plus {hidden_count} more events not shown.")
    return lines


def build_daily_body(
    now_local: datetime,
    csv_path: Path,
    events_today: pd.DataFrame,
    max_events: int,
) -> str:
    day_text = now_local.strftime("%Y-%m-%d")
    lines = [
        f"AppalCart bus bunching daily outlook for {day_text} ({now_local.tzname()})",
        f"Generated at: {now_local.strftime('%Y-%m-%d %H:%M:%S %Z')}",
        f"Source CSV: {csv_path.resolve()}",
        "",
    ]

    if events_today.empty:
        lines.append("No predicted bunching events were found for today.")
        return "\n".join(lines)

    lines.append(f"Total predicted bunching events today: {len(events_today)}")
    lines.append(f"Affected routes: {events_today['route_label'].nunique()}")
    lines.append("")
    lines.append("Top routes by predicted events:")
    route_counts = (
        events_today.groupby("route_label")
        .size()
        .sort_values(ascending=False)
        .head(10)
    )
    for route_label, count in route_counts.items():
        lines.append(f"- Route {route_label}: {int(count)}")
    lines.append("")
    lines.append("Event list:")
    lines.extend(build_event_lines(events_today, max_events))
    return "\n".join(lines)


def build_upcoming_body(
    now_local: datetime,
    csv_path: Path,
    events_upcoming: pd.DataFrame,
    lead_minutes: int,
    window_minutes: int,
    window_start: datetime,
    window_end: datetime,
    max_events: int,
) -> str:
    lines = [
        "AppalCart one-hour-ahead bus bunching alert",
        f"Generated at: {now_local.strftime('%Y-%m-%d %H:%M:%S %Z')}",
        f"Source CSV: {csv_path.resolve()}",
        f"Target lead: {lead_minutes} minutes",
        (
            "Alert window: "
            f"{window_start.strftime('%Y-%m-%d %H:%M %Z')} to "
            f"{window_end.strftime('%Y-%m-%d %H:%M %Z')} "
            f"(window {window_minutes} minutes)"
        ),
        "",
        f"Events in this alert: {len(events_upcoming)}",
        "",
        "Event list:",
    ]
    lines.extend(build_event_lines(events_upcoming, max_events))
    return "\n".join(lines)


def send_email(args: argparse.Namespace, subject: str, body: str) -> None:
    recipients = parse_recipients(args.to_addresses)
    sender = (args.from_address or "").strip()

    if args.dry_run:
        print("")
        print("=" * 72)
        print("DRY RUN EMAIL")
        print(f"Subject: {subject}")
        print(f"From: {sender or '(not set)'}")
        print(f"To: {', '.join(recipients) if recipients else '(not set)'}")
        print("-" * 72)
        print(body)
        print("=" * 72)
        print("")
        return

    if not sender:
        raise ValueError("Missing from address. Set --from-address or APPALCART_ALERT_FROM.")
    if not recipients:
        raise ValueError("Missing recipients. Set --to-addresses or APPALCART_ALERT_TO.")
    if not args.smtp_host:
        raise ValueError("Missing SMTP host. Set --smtp-host or APPALCART_SMTP_HOST.")

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = sender
    message["To"] = ", ".join(recipients)
    message.set_content(body)

    if args.smtp_ssl:
        with smtplib.SMTP_SSL(args.smtp_host, args.smtp_port, timeout=60) as smtp:
            if args.smtp_user:
                smtp.login(args.smtp_user, args.smtp_password or "")
            smtp.send_message(message)
        return

    with smtplib.SMTP(args.smtp_host, args.smtp_port, timeout=60) as smtp:
        smtp.ehlo()
        if args.smtp_starttls:
            if not smtp.has_extn("starttls"):
                raise RuntimeError("SMTP server does not support STARTTLS.")
            smtp.starttls()
            smtp.ehlo()
        if args.smtp_user:
            smtp.login(args.smtp_user, args.smtp_password or "")
        smtp.send_message(message)


def run_daily_alert(
    args: argparse.Namespace,
    conn: sqlite3.Connection,
    now_local: datetime,
    csv_path: Path,
    events: pd.DataFrame,
) -> bool:
    digest_date = now_local.strftime("%Y-%m-%d")
    if not args.force and has_digest_been_sent(conn, digest_date):
        print(f"Skipping daily digest for {digest_date}: already sent.")
        return False

    events_today = events[events["event_date"] == digest_date].copy()
    if events_today.empty and not args.send_empty_digest:
        print(f"Skipping daily digest for {digest_date}: no events and --no-send-empty-digest.")
        return False

    subject = (
        f"{args.subject_prefix} Daily predicted bus bunching for {digest_date} "
        f"({len(events_today)} events)"
    )
    body = build_daily_body(now_local, csv_path, events_today, args.max_events_in_email)
    send_email(args, subject, body)

    if not args.dry_run:
        mark_digest_sent(conn, digest_date)
    return True


def run_upcoming_alert(
    args: argparse.Namespace,
    conn: sqlite3.Connection,
    now_local: datetime,
    csv_path: Path,
    events: pd.DataFrame,
) -> bool:
    half_window = timedelta(minutes=args.window_minutes / 2.0)
    target_time = now_local + timedelta(minutes=args.lead_minutes)
    window_start = target_time - half_window
    window_end = target_time + half_window

    candidates = events[
        (events["event_time_local"] >= window_start)
        & (events["event_time_local"] < window_end)
        & (events["event_time_local"] > now_local)
    ].copy()

    if not args.force:
        candidates = filter_unsent_events(conn, candidates)

    if candidates.empty:
        print("Skipping one-hour-ahead alert: no unsent events in target window.")
        return False

    subject = (
        f"{args.subject_prefix} One-hour-ahead bunching alert "
        f"({len(candidates)} events)"
    )
    body = build_upcoming_body(
        now_local,
        csv_path,
        candidates,
        args.lead_minutes,
        args.window_minutes,
        window_start,
        window_end,
        args.max_events_in_email,
    )
    send_email(args, subject, body)

    if not args.dry_run:
        mark_events_sent(conn, candidates)
    return True


def main() -> int:
    args = parse_args()
    csv_path = Path(args.csv)
    state_db_path = Path(args.state_db)
    state_db_path.parent.mkdir(parents=True, exist_ok=True)

    timezone_name = args.timezone
    ZoneInfo(timezone_name)
    now_local = datetime.now(ZoneInfo(timezone_name))

    events = load_events(csv_path, timezone_name)
    print(f"Loaded {len(events)} parsed events from {csv_path}.")

    with sqlite3.connect(state_db_path) as conn:
        initialize_state(conn)

        sent_count = 0
        if args.mode in {"daily", "both"}:
            sent = run_daily_alert(args, conn, now_local, csv_path, events)
            sent_count += int(sent)
        if args.mode in {"upcoming", "both"}:
            sent = run_upcoming_alert(args, conn, now_local, csv_path, events)
            sent_count += int(sent)

    print(f"Complete. Emails sent: {sent_count}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
