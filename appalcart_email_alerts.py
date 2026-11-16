#!/usr/bin/env python3
"""
Send AppalCart bus bunching email alerts using the exact data source and
recurring-hotspot prediction model from the GitHub Pages dashboard (appalcart.html).

Supports:
1) Daily digest for all predicted bunching hotspots today, sorted chronologically.
2) One-hour-ahead alerts for hotspots in the upcoming target hour.
"""

from __future__ import annotations

import argparse
import html as html_lib
import io
import json
import os
import re
import smtplib
import sqlite3
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime, timedelta
from email.message import EmailMessage
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

import pandas as pd


DEFAULT_DASHBOARD_URL = (
    "https://ai-rapid-prototyping.github.io/Bus-Bunching-Prediction/agencies/appalcart.html"
)

BASE_REQUIRED_COLUMNS = ["route_id"]
OPTIONAL_COLUMNS = [
    "first_predicted_arrival",
    "route_label",
    "route_name",
    "direction_id",
    "stop_id",
    "stop_name",
    "lead_trip_id",
    "trailing_trip_id",
    "lead_headsign",
    "event_date",
    "event_hour",
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
        "--dashboard-url",
        default=os.getenv("APPALCART_DASHBOARD_URL", DEFAULT_DASHBOARD_URL),
        help="GitHub Pages URL to appalcart.html. Default: %(default)s",
    )
    parser.add_argument(
        "--csv",
        default="Outputs/AppalCart/appalcart_bunching_events.csv",
        help="Local fallback path to AppalCart events CSV. Default: %(default)s",
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


# ---------------------------------------------------------------------------
# Data loading & dashboard prediction model (mirrors appalcart.html)
# ---------------------------------------------------------------------------

def fetch_text(url: str, timeout: int = 30) -> str:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "AppalCart-Alert-Job/1.0", "Cache-Control": "no-cache"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8")


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.fillna("").astype(str)
    if "route_id" not in df.columns and "route_label" in df.columns:
        df["route_id"] = df["route_label"]
    for col in OPTIONAL_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df["route_label"] = df["route_label"].str.strip()
    df["route_label"] = df["route_label"].where(
        df["route_label"] != "", df["route_id"].str.strip()
    )
    return df


def load_historical_rows(dashboard_url: str, csv_path: Path) -> tuple[pd.DataFrame, str]:
    """
    Load historical bunching observations from:
      1) The CSV referenced by appalcart.html (../assets/data/appalcart/bus_bunching_events.csv)
      2) The <script id="appalcart-event-history"> JSON embedded inside appalcart.html
      3) The local CSV path fallback
    """
    dashboard_url = (dashboard_url or "").strip()
    if dashboard_url:
        remote_csv_url = urllib.parse.urljoin(
            dashboard_url, "../assets/data/appalcart/bus_bunching_events.csv"
        )
        try:
            csv_text = fetch_text(remote_csv_url)
            df = pd.read_csv(io.StringIO(csv_text), dtype=str)
            return normalize_dataframe(df), remote_csv_url
        except Exception as csv_err:
            print(
                f"Remote CSV unavailable ({csv_err}); falling back to embedded JSON in {dashboard_url}"
            )
            try:
                html_text = fetch_text(dashboard_url)
                match = re.search(
                    r'<script[^>]*id=["\']appalcart-event-history["\'][^>]*>(.*?)</script>',
                    html_text,
                    flags=re.DOTALL | re.IGNORECASE,
                )
                if not match:
                    raise ValueError(
                        f"Could not find #appalcart-event-history JSON in {dashboard_url}"
                    ) from csv_err
                records = json.loads(match.group(1).strip())
                df = pd.DataFrame(records)
                return normalize_dataframe(df), f"{dashboard_url}#appalcart-event-history"
            except Exception as html_err:
                print(
                    f"Dashboard HTML unavailable ({html_err}); falling back to local CSV {csv_path}"
                )

    if not csv_path.exists():
        raise FileNotFoundError(f"Events CSV not found: {csv_path}")
    df = pd.read_csv(csv_path, dtype=str)
    return normalize_dataframe(df), str(csv_path)


def js_round(value: float) -> int:
    """Match JavaScript Math.round() half-up behavior for positive scores."""
    return int(value + 0.5)


def normalize_hour_num(row: pd.Series) -> int | None:
    hour_text = str(row.get("event_hour", "")).strip()
    match = re.match(r"^(\d{1,2})", hour_text)
    if match:
        hour = int(match.group(1))
        if 0 <= hour <= 23:
            return hour
    arrival = str(row.get("first_predicted_arrival", "")).strip()
    match = re.search(r"\b(\d{1,2}):\d{2}", arrival)
    if match:
        hour = int(match.group(1))
        if 0 <= hour <= 23:
            return hour
    return None


def priority_for_score(score: int) -> str:
    if score >= 70:
        return "High"
    if score >= 45:
        return "Medium"
    return "Monitor"


def build_prediction_stats(history_df: pd.DataFrame) -> dict:
    """Replicate buildStats() from appalcart.html."""
    route_hour_counts: Counter[tuple[str, int]] = Counter()
    route_hour_stops: Counter[tuple[str, int, str, str]] = Counter()
    weekday_route_hour_counts: Counter[tuple[str, str, int]] = Counter()
    weekday_route_hour_stops: Counter[tuple[str, str, int, str, str]] = Counter()

    for _, row in history_df.iterrows():
        route = str(row.get("route_label", "")).strip()
        stop_id = str(row.get("stop_id", "")).strip()
        stop_name = str(row.get("stop_name", "")).strip() or stop_id or "Stop"
        hour = normalize_hour_num(row)
        if not route or not stop_id or hour is None:
            continue

        route_hour_counts[(route, hour)] += 1
        route_hour_stops[(route, hour, stop_id, stop_name)] += 1

        event_date = str(row.get("event_date", "")).strip()
        if event_date:
            try:
                parsed = datetime.strptime(event_date[:10], "%Y-%m-%d")
                weekday = parsed.strftime("%A")
                weekday_route_hour_counts[(weekday, route, hour)] += 1
                weekday_route_hour_stops[(weekday, route, hour, stop_id, stop_name)] += 1
            except ValueError:
                pass

    return {
        "route_hour_counts": route_hour_counts,
        "route_hour_stops": route_hour_stops,
        "weekday_route_hour_counts": weekday_route_hour_counts,
        "weekday_route_hour_stops": weekday_route_hour_stops,
    }


def top_location_for_window(
    stats: dict, weekday: str, route: str, hour: int
) -> tuple[str, str, int]:
    """Replicate topLocationForWindow() from appalcart.html."""
    best_stop_id = ""
    best_stop_name = "Location varies"
    best_count = 0

    for (day, r, h, stop_id, stop_name), count in stats["weekday_route_hour_stops"].items():
        if day == weekday and r == route and h == hour and count > best_count:
            best_stop_id, best_stop_name, best_count = stop_id, stop_name, count

    if best_count == 0:
        for (r, h, stop_id, stop_name), count in stats["route_hour_stops"].items():
            if r == route and h == hour and count > best_count:
                best_stop_id, best_stop_name, best_count = stop_id, stop_name, count

    return best_stop_id, best_stop_name, best_count


def predict_events_for_dates(
    history_df: pd.DataFrame,
    target_dates: list[datetime],
    timezone_name: str,
) -> pd.DataFrame:
    """Generate predicted bunching hotspots matching appalcart.html, sorted chronologically."""
    stats = build_prediction_stats(history_df)
    tz = ZoneInfo(timezone_name)
    rows: list[dict] = []

    for target in target_dates:
        weekday = target.strftime("%A")
        date_str = target.strftime("%Y-%m-%d")

        candidates = [
            (route, hour, count)
            for (day, route, hour), count in stats["weekday_route_hour_counts"].items()
            if day == weekday
        ]
        if candidates:
            basis = f"same weekday history ({weekday})"
            maximum = max([1, *stats["weekday_route_hour_counts"].values()])
        else:
            basis = "all recent history"
            candidates = [
                (route, hour, count)
                for (route, hour), count in stats["route_hour_counts"].items()
            ]
            maximum = max([1, *stats["route_hour_counts"].values()])

        for route, hour, count in candidates:
            stop_id, stop_name, _ = top_location_for_window(stats, weekday, route, hour)
            score = min(100, max(1, js_round((count / maximum) * 100)))
            priority = priority_for_score(score)
            event_dt = datetime(target.year, target.month, target.day, hour, 0, 0, tzinfo=tz)
            event_key = f"pred|{event_dt.strftime('%Y-%m-%dT%H:%M:%S%z')}|{route}|{stop_id}"

            rows.append(
                {
                    "event_time_local": event_dt,
                    "event_date": date_str,
                    "route_id": route,
                    "route_label": route,
                    "stop_id": stop_id,
                    "stop_name": stop_name,
                    "priority": priority,
                    "score": score,
                    "count": count,
                    "score_label": f"{score}/100",
                    "evidence": f"{count} obs; {basis}",
                    "event_key": event_key,
                }
            )

    if not rows:
        return pd.DataFrame(
            columns=[
                "event_time_local",
                "event_date",
                "route_id",
                "route_label",
                "stop_id",
                "stop_name",
                "priority",
                "score",
                "count",
                "score_label",
                "evidence",
                "event_key",
            ]
        )

    return pd.DataFrame(rows).sort_values(
        ["event_time_local", "score", "route_label"], ascending=[True, False, True]
    )


# ---------------------------------------------------------------------------
# SQLite dedupe state
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Email content helpers
# ---------------------------------------------------------------------------

TEXT_RULE = "-" * 48
HTML_ACCENT = "#1f3a5f"


def format_time(value: datetime) -> str:
    return value.strftime("%I:%M %p").lstrip("0")


def format_long_date(value: datetime) -> str:
    return f"{value:%A, %B} {value.day}, {value.year}"


def pluralize(count: int, word: str) -> str:
    return f"{count} {word}" + ("" if count == 1 else "s")


def event_fields(row: pd.Series) -> dict[str, str]:
    route_label = str(row.get("route_label", "")).strip() or str(row.get("route_id", "")).strip()
    stop_name = str(row.get("stop_name", "")).strip()
    stop_id = str(row.get("stop_id", "")).strip()
    priority = str(row.get("priority", "")).strip()
    score_label = str(row.get("score_label", "")).strip()

    if stop_name and stop_id:
        stop_label = f"{stop_name} (Stop {stop_id})"
    elif stop_name:
        stop_label = stop_name
    elif stop_id:
        stop_label = f"Stop {stop_id}"
    else:
        stop_label = "Unknown stop"

    if priority and score_label:
        priority_label = f"{priority} ({score_label})"
    else:
        priority_label = priority or score_label

    return {
        "time": format_time(row["event_time_local"]),
        "route": f"Route {route_label}",
        "stop": stop_label,
        "priority": priority_label,
        "priority_level": priority.lower(),
    }


def select_events(events: pd.DataFrame, max_events: int) -> tuple[list[dict[str, str]], int]:
    if events.empty:
        return [], 0
    events_sorted = events.sort_values(
        ["event_time_local", "score", "route_label"], ascending=[True, False, True]
    )
    shown = events_sorted.head(max_events)
    fields = [event_fields(row) for _, row in shown.iterrows()]
    return fields, len(events_sorted) - len(shown)


def top_route_counts(events: pd.DataFrame, limit: int = 10) -> list[tuple[str, int]]:
    counts = events.groupby("route_label").size().sort_values(ascending=False).head(limit)
    return [(str(label), int(count)) for label, count in counts.items()]


# Plain-text rendering -------------------------------------------------------

def text_section(title: str) -> list[str]:
    return ["", "", title.upper(), TEXT_RULE]


def text_key_values(rows: list[tuple[str, str]]) -> list[str]:
    width = max(len(key) for key, _ in rows) + 2
    return [f"  {(key + ':'):<{width}} {value}" for key, value in rows]


def text_event_lines(fields: list[dict[str, str]], hidden_count: int) -> list[str]:
    lines: list[str] = []
    for item in fields:
        priority_suffix = f" [{item['priority']}]" if item.get("priority") else ""
        lines.append(f"  {item['time']:>8}   {item['route']}{priority_suffix}")
        lines.append(f"  {'':>8}   {item['stop']}")
        lines.append("")
    if lines:
        lines.pop()
    if hidden_count > 0:
        lines += ["", f"  ...plus {pluralize(hidden_count, 'more hotspot')} not shown."]
    return lines


# HTML rendering -------------------------------------------------------------

def esc(value: object) -> str:
    return html_lib.escape(str(value))


def priority_color(level: str) -> str:
    if level == "high":
        return "#a93d33"
    if level == "medium":
        return "#9a5b00"
    return "#2f6f9f"


def html_section(title: str, inner_html: str) -> str:
    return (
        f'<h3 style="margin:28px 0 10px;font-size:15px;color:{HTML_ACCENT};'
        f'border-bottom:1px solid #d0d7de;padding-bottom:6px;">{esc(title)}</h3>'
        f"{inner_html}"
    )


def html_key_values(rows: list[tuple[str, str]]) -> str:
    cells = "".join(
        f'<tr><td style="padding:4px 24px 4px 0;color:#57606a;">{esc(key)}</td>'
        f'<td style="padding:4px 0;font-weight:bold;">{esc(value)}</td></tr>'
        for key, value in rows
    )
    return f'<table style="border-collapse:collapse;font-size:14px;">{cells}</table>'


def html_events_table(fields: list[dict[str, str]], hidden_count: int) -> str:
    header_style = (
        f"text-align:left;padding:8px 12px;background:{HTML_ACCENT};"
        "color:#ffffff;font-size:13px;font-weight:bold;"
    )
    headers = "".join(
        f'<th style="{header_style}">{name}</th>' for name in ["Hour", "Route", "Stop", "Priority"]
    )
    rows = []
    for index, item in enumerate(fields):
        background = "#f6f8fa" if index % 2 else "#ffffff"
        cell = f"padding:8px 12px;border-bottom:1px solid #e1e4e8;background:{background};"
        p_color = priority_color(item.get("priority_level", ""))
        rows.append(
            "<tr>"
            f'<td style="{cell}white-space:nowrap;font-weight:bold;">{esc(item["time"])}</td>'
            f'<td style="{cell}white-space:nowrap;">{esc(item["route"])}</td>'
            f'<td style="{cell}">{esc(item["stop"])}</td>'
            f'<td style="{cell}white-space:nowrap;font-weight:bold;color:{p_color};">{esc(item["priority"])}</td>'
            "</tr>"
        )
    table = (
        '<table style="border-collapse:collapse;width:100%;font-size:14px;">'
        f"<thead><tr>{headers}</tr></thead><tbody>{''.join(rows)}</tbody></table>"
    )
    if hidden_count > 0:
        table += (
            '<p style="margin:10px 0 0;color:#57606a;font-size:13px;">'
            f"...plus {esc(pluralize(hidden_count, 'more hotspot'))} not shown.</p>"
        )
    return table


def html_document(title: str, subtitle: str, body_html: str, footer: str) -> str:
    return (
        '<!DOCTYPE html><html><body style="margin:0;padding:0;background:#f0f2f5;">'
        '<div style="max-width:720px;margin:0 auto;padding:24px;'
        'font-family:Arial,Helvetica,sans-serif;color:#24292f;line-height:1.4;">'
        '<div style="background:#ffffff;border-radius:6px;padding:28px;">'
        f'<h2 style="margin:0 0 6px;font-size:20px;color:{HTML_ACCENT};">{esc(title)}</h2>'
        f'<p style="margin:0;color:#57606a;font-size:14px;">{esc(subtitle)}</p>'
        f"{body_html}"
        f'<p style="margin:32px 0 0;color:#8c959f;font-size:12px;">{esc(footer)}</p>'
        "</div></div></body></html>"
    )


# Email bodies ---------------------------------------------------------------

def build_daily_body(
    now_local: datetime,
    events_today: pd.DataFrame,
    max_events: int,
) -> tuple[str, str]:
    """Return (plain_text, html) bodies for the daily digest."""
    title = "AppalCART Bus Bunching: Daily Outlook"
    subtitle = format_long_date(now_local)
    footer = f"Generated {format_time(now_local)} {now_local.tzname()}"
    text = [title, subtitle]

    if events_today.empty:
        message = "No predicted bunching hotspots for today."
        text += ["", message, "", "", footer]
        body_html = f'<p style="margin:28px 0 0;font-size:14px;">{esc(message)}</p>'
        return "\n".join(text), html_document(title, subtitle, body_html, footer)

    summary = [
        ("Predicted hotspots", str(len(events_today))),
        ("Routes affected", str(events_today["route_label"].nunique())),
    ]
    routes = [
        (f"Route {label}", pluralize(count, "hotspot"))
        for label, count in top_route_counts(events_today)
    ]
    fields, hidden_count = select_events(events_today, max_events)

    text += text_section("Summary") + text_key_values(summary)
    text += text_section("Top routes") + text_key_values(routes)
    text += text_section("Predicted hotspots") + text_event_lines(fields, hidden_count)
    text += ["", "", footer]

    body_html = (
        html_section("Summary", html_key_values(summary))
        + html_section("Top routes", html_key_values(routes))
        + html_section("Predicted hotspots", html_events_table(fields, hidden_count))
    )
    return "\n".join(text), html_document(title, subtitle, body_html, footer)


def build_upcoming_body(
    now_local: datetime,
    events_upcoming: pd.DataFrame,
    lead_minutes: int,
    window_start: datetime,
    window_end: datetime,
    max_events: int,
) -> tuple[str, str]:
    """Return (plain_text, html) bodies for the one-hour-ahead alert."""
    title = "AppalCART Bus Bunching: Upcoming Alert"
    subtitle = f"Predicted in about {lead_minutes} minutes"
    footer = f"Generated {format_time(now_local)} {now_local.tzname()}"
    summary = [
        ("Hotspots in this alert", str(len(events_upcoming))),
        (
            "Alert window",
            f"{format_time(window_start)} - {format_time(window_end)} {window_end.tzname()}",
        ),
    ]
    fields, hidden_count = select_events(events_upcoming, max_events)

    text = [title, subtitle]
    text += text_section("Summary") + text_key_values(summary)
    text += text_section("Predicted hotspots") + text_event_lines(fields, hidden_count)
    text += ["", "", footer]

    body_html = (
        html_section("Summary", html_key_values(summary))
        + html_section("Predicted hotspots", html_events_table(fields, hidden_count))
    )
    return "\n".join(text), html_document(title, subtitle, body_html, footer)


def send_email(
    args: argparse.Namespace,
    subject: str,
    body: str,
    html_body: str | None = None,
) -> None:
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
    if html_body:
        message.add_alternative(html_body, subtype="html")

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
    events: pd.DataFrame,
) -> bool:
    digest_date = now_local.strftime("%Y-%m-%d")
    if not args.force and has_digest_been_sent(conn, digest_date):
        print(f"Skipping daily digest for {digest_date}: already sent.")
        return False

    events_today = (
        events[events["event_date"] == digest_date]
        .sort_values(["event_time_local", "score", "route_label"], ascending=[True, False, True])
        .copy()
    )
    if events_today.empty and not args.send_empty_digest:
        print(f"Skipping daily digest for {digest_date}: no events and --no-send-empty-digest.")
        return False

    subject = (
        f"{args.subject_prefix} Daily predicted bus bunching for {digest_date} "
        f"({len(events_today)} hotspots)"
    )
    body, html_body = build_daily_body(now_local, events_today, args.max_events_in_email)
    send_email(args, subject, body, html_body)

    if not args.dry_run:
        mark_digest_sent(conn, digest_date)
    return True


def run_upcoming_alert(
    args: argparse.Namespace,
    conn: sqlite3.Connection,
    now_local: datetime,
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
        print("Skipping one-hour-ahead alert: no unsent hotspots in target window.")
        return False

    subject = (
        f"{args.subject_prefix} One-hour-ahead bunching alert "
        f"({len(candidates)} hotspots)"
    )
    body, html_body = build_upcoming_body(
        now_local,
        candidates,
        args.lead_minutes,
        window_start,
        window_end,
        args.max_events_in_email,
    )
    send_email(args, subject, body, html_body)

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

    history_df, source_used = load_historical_rows(args.dashboard_url, csv_path)
    target_dates = [now_local, now_local + timedelta(days=1)]
    events = predict_events_for_dates(history_df, target_dates, timezone_name)
    print(
        f"Loaded {len(history_df)} historical rows from {source_used}; "
        f"generated {len(events)} predicted hotspots."
    )

    with sqlite3.connect(state_db_path) as conn:
        initialize_state(conn)

        sent_count = 0
        if args.mode in {"daily", "both"}:
            sent = run_daily_alert(args, conn, now_local, events)
            sent_count += int(sent)
        if args.mode in {"upcoming", "both"}:
            sent = run_upcoming_alert(args, conn, now_local, events)
            sent_count += int(sent)

    print(f"Complete. Emails sent: {sent_count}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())