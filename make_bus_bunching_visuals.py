#!/usr/bin/env python3
"""
Create presentation graphics from bus_bunching_events.csv.

Run the analyzer first:
  python bus_bunching_analysis.py

Then generate visuals:
  python make_bus_bunching_visuals.py
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError as exc:  # pragma: no cover - depends on local environment.
    raise SystemExit(
        "matplotlib is required for graphics. Install it with: pip install matplotlib"
    ) from exc


PRIMARY = "#2f5d8c"
SECONDARY = "#c75146"
ACCENT = "#4f8a5b"
GOLD = "#c99a2e"
GRID = "#d9d9d9"
TEXT = "#222222"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate bus bunching presentation visuals from event CSV output."
    )
    parser.add_argument(
        "--events-csv",
        default="bus_bunching_events.csv",
        help="Event CSV produced by bus_bunching_analysis.py. Default: %(default)s",
    )
    parser.add_argument(
        "--output-dir",
        default="bus_bunching_visuals",
        help="Folder for PNGs and summary CSVs. Default: %(default)s",
    )
    parser.add_argument(
        "--top-routes",
        type=int,
        default=15,
        help="Number of routes to include in route charts. Default: %(default)s",
    )
    parser.add_argument(
        "--top-stops",
        type=int,
        default=20,
        help="Number of stops to include in stop charts. Default: %(default)s",
    )
    return parser.parse_args()


def clean_events(events_csv: Path) -> pd.DataFrame:
    if not events_csv.exists():
        raise FileNotFoundError(
            f"{events_csv} not found. Run bus_bunching_analysis.py first."
        )
    df = pd.read_csv(events_csv, dtype={"route_label": str, "stop_id": str})
    if df.empty:
        raise ValueError(f"{events_csv} has no events to visualize.")

    required = [
        "route_label",
        "direction_id",
        "stop_id",
        "stop_name",
        "event_date",
        "event_hour",
        "stop_lat",
        "stop_lon",
    ]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{events_csv} is missing columns: {', '.join(missing)}")

    df["route_direction"] = (
        df["route_label"].astype(str) + " dir " + df["direction_id"].astype(str)
    )
    df["hour_num"] = df["event_hour"].astype(str).str.slice(0, 2).astype(int)
    df["stop_lat_num"] = pd.to_numeric(df["stop_lat"], errors="coerce")
    df["stop_lon_num"] = pd.to_numeric(df["stop_lon"], errors="coerce")
    return df


def style_axis(ax, title: str, xlabel: str = "", ylabel: str = "") -> None:
    ax.set_title(title, fontsize=16, weight="bold", color=TEXT, pad=14)
    ax.set_xlabel(xlabel, fontsize=11, color=TEXT)
    ax.set_ylabel(ylabel, fontsize=11, color=TEXT)
    ax.tick_params(colors=TEXT, labelsize=10)
    ax.grid(axis="x", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)
    ax.spines["left"].set_color("#888888")
    ax.spines["bottom"].set_color("#888888")


def save_barh(df: pd.DataFrame, label_col: str, value_col: str, title: str, path: Path) -> None:
    plot_df = df.sort_values(value_col, ascending=True)
    fig_height = max(5, len(plot_df) * 0.38)
    fig, ax = plt.subplots(figsize=(12, fig_height))
    bars = ax.barh(plot_df[label_col], plot_df[value_col], color=PRIMARY)
    style_axis(ax, title, xlabel="Bunching events")
    ax.bar_label(bars, padding=3, fontsize=9, color=TEXT)
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def save_top_route_charts(df: pd.DataFrame, output_dir: Path, top_routes: int) -> None:
    routes = (
        df.groupby("route_label", as_index=False)
        .agg(events=("route_label", "size"), stops=("stop_id", "nunique"))
        .sort_values("events", ascending=False)
        .head(top_routes)
    )
    save_barh(
        routes,
        "route_label",
        "events",
        "Routes With The Most Bus Bunching",
        output_dir / "top_routes.png",
    )

    route_dirs = (
        df.groupby("route_direction", as_index=False)
        .agg(events=("route_direction", "size"))
        .sort_values("events", ascending=False)
        .head(top_routes)
    )
    save_barh(
        route_dirs,
        "route_direction",
        "events",
        "Route Directions With The Most Bus Bunching",
        output_dir / "top_route_directions.png",
    )


def save_stop_chart(df: pd.DataFrame, output_dir: Path, top_stops: int) -> None:
    stops = (
        df.groupby(["stop_id", "stop_name"], as_index=False)
        .agg(events=("stop_id", "size"), routes=("route_label", "nunique"))
        .sort_values("events", ascending=False)
        .head(top_stops)
    )
    stops["stop_label"] = stops["stop_name"] + " [" + stops["stop_id"] + "]"
    save_barh(
        stops,
        "stop_label",
        "events",
        "Stops And Intersections With The Most Bus Bunching",
        output_dir / "top_stops.png",
    )


def save_hour_chart(df: pd.DataFrame, output_dir: Path) -> None:
    hourly = (
        df.groupby("hour_num", as_index=False)
        .agg(events=("route_label", "size"), routes=("route_label", "nunique"))
        .sort_values("hour_num")
    )
    fig, ax = plt.subplots(figsize=(12, 6))
    bars = ax.bar(hourly["hour_num"], hourly["events"], color=SECONDARY)
    style_axis(ax, "Bus Bunching By Hour Of Day", xlabel="Hour", ylabel="Events")
    ax.set_xticks(range(0, 24))
    ax.bar_label(bars, padding=3, fontsize=8, color=TEXT)
    fig.tight_layout()
    fig.savefig(output_dir / "bunching_by_hour.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def save_route_hour_heatmap(df: pd.DataFrame, output_dir: Path, top_routes: int) -> None:
    route_order = (
        df["route_label"].value_counts().head(top_routes).index.astype(str).tolist()
    )
    heat = (
        df[df["route_label"].isin(route_order)]
        .groupby(["route_label", "hour_num"])
        .size()
        .unstack(fill_value=0)
        .reindex(index=route_order, columns=range(0, 24), fill_value=0)
    )
    fig_height = max(6, len(route_order) * 0.42)
    fig, ax = plt.subplots(figsize=(14, fig_height))
    image = ax.imshow(heat.values, aspect="auto", cmap="YlOrRd")
    ax.set_title("When Top Routes Bunch Most", fontsize=16, weight="bold", color=TEXT, pad=14)
    ax.set_xlabel("Hour of day", fontsize=11, color=TEXT)
    ax.set_ylabel("Route", fontsize=11, color=TEXT)
    ax.set_xticks(range(0, 24))
    ax.set_yticks(range(len(route_order)))
    ax.set_yticklabels(route_order)
    ax.tick_params(colors=TEXT, labelsize=10)
    cbar = fig.colorbar(image, ax=ax)
    cbar.set_label("Bunching events")
    fig.tight_layout()
    fig.savefig(output_dir / "route_hour_heatmap.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def save_stop_location_scatter(df: pd.DataFrame, output_dir: Path, top_stops: int) -> None:
    geo = df.dropna(subset=["stop_lat_num", "stop_lon_num"])
    stops = (
        geo.groupby(["stop_id", "stop_name", "stop_lat_num", "stop_lon_num"], as_index=False)
        .agg(events=("stop_id", "size"), routes=("route_label", "nunique"))
        .sort_values("events", ascending=False)
        .head(top_stops)
    )
    if stops.empty:
        return

    sizes = 80 + (stops["events"] / stops["events"].max()) * 520
    fig, ax = plt.subplots(figsize=(10, 8))
    scatter = ax.scatter(
        stops["stop_lon_num"],
        stops["stop_lat_num"],
        s=sizes,
        c=stops["events"],
        cmap="viridis",
        alpha=0.82,
        edgecolor="white",
        linewidth=0.8,
    )
    for _, row in stops.head(10).iterrows():
        ax.annotate(
            str(row["stop_id"]),
            (row["stop_lon_num"], row["stop_lat_num"]),
            xytext=(4, 4),
            textcoords="offset points",
            fontsize=8,
            color=TEXT,
        )
    style_axis(
        ax,
        "Top Bus Bunching Locations",
        xlabel="Longitude",
        ylabel="Latitude",
    )
    ax.grid(axis="both", color=GRID, linewidth=0.8)
    cbar = fig.colorbar(scatter, ax=ax)
    cbar.set_label("Bunching events")
    fig.tight_layout()
    fig.savefig(output_dir / "top_stop_locations.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def write_route_summary(df: pd.DataFrame, output_dir: Path) -> None:
    rows = []
    for route_label, route_df in df.groupby("route_label"):
        top_stop = (
            route_df.groupby(["stop_id", "stop_name"], as_index=False)
            .size()
            .sort_values("size", ascending=False)
            .iloc[0]
        )
        top_hour = (
            route_df.groupby("event_hour", as_index=False)
            .size()
            .sort_values("size", ascending=False)
            .iloc[0]
        )
        rows.append(
            {
                "route_label": route_label,
                "events": len(route_df),
                "stops": route_df["stop_id"].nunique(),
                "top_stop_id": top_stop["stop_id"],
                "top_stop_name": top_stop["stop_name"],
                "top_stop_events": int(top_stop["size"]),
                "top_hour": top_hour["event_hour"],
                "top_hour_events": int(top_hour["size"]),
            }
        )
    summary = pd.DataFrame(rows).sort_values("events", ascending=False)
    summary.to_csv(output_dir / "per_route_where_when_summary.csv", index=False)


def main() -> int:
    args = parse_args()
    events_csv = Path(args.events_csv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = clean_events(events_csv)
    save_top_route_charts(df, output_dir, args.top_routes)
    save_stop_chart(df, output_dir, args.top_stops)
    save_hour_chart(df, output_dir)
    save_route_hour_heatmap(df, output_dir, args.top_routes)
    save_stop_location_scatter(df, output_dir, args.top_stops)
    write_route_summary(df, output_dir)

    print(f"Wrote visuals and summary tables to {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
