[README.md](https://github.com/user-attachments/files/30563394/README.md)
# IP3-FTA-Bus-Bunching# King County Metro Bus Bunching Analysis

This project detects King County Metro bus bunching, archives external operating-condition inputs, and tests whether those inputs are associated with bunching.

The intended Databricks workflow is:

1. Generate `bus_bunching_events.csv`.
2. Update parameter archives and run correlation analysis.
3. Generate presentation visuals.

## Repository Layout

Expected files and folders:

```text
bus_bunching_analysis.py
make_bus_bunching_visuals.py
update_parameter_archives.py
analyze_parameter_bunching_correlations.py
spark_parameter_bunching_correlations.py
google_transit/
Parameter Scripts/
```

`google_transit/` must contain the static GTFS files, including:

```text
agency.txt
routes.txt
stops.txt
trips.txt
stop_times.txt
calendar.txt
calendar_dates.txt
```

`Parameter Scripts/` should contain:

```text
kcm_alerts.py
seattle_fire_mvi.py
seattle_recent_weather.py
seattle_special_events.py
wsdot_alerts_kc.py
```

## Databricks Setup

Use a Databricks cluster with Spark available. Install Python dependencies on the cluster or in a notebook:

```python
%pip install pandas numpy requests lxml matplotlib
```

Optional dependencies:

```python
%pip install selenium google-generativeai
```

Those optional packages are only needed for `seattle_special_events.py`. That script also needs browser/driver support, so skip it initially unless the cluster is set up for Selenium.

Set the project root. Replace this path if your repo lives somewhere else:

```python
import os
os.environ["IP3_PROJECT_ROOT"] = "/Workspace/Users/manu.agnihotri@dot.gov"
```

The scripts also accept `--project-root`, so setting the environment variable is optional.

## Run Order

Run these commands from the project root in Databricks.

### 1. Generate Bus Bunching Events

```bash
python bus_bunching_analysis.py
```

This script uses Spark to read GTFS-RT records from Databricks, applies GTFS scheduled-headway thresholds, excludes terminal stops by default, and writes:

```text
bus_bunching_events.csv
bus_bunching_report.txt
```

The default Databricks version is configured for:

```text
volpe_ip3_dev.gtfsrt.stop_time_updates
```

Check `--days-back` if you want a different rolling window:

```bash
python bus_bunching_analysis.py --days-back 14
```

### 2. Update Parameter Archives and Run Correlations

```bash
python update_parameter_archives.py --engine spark --project-root /Workspace/Users/manu.agnihotri@dot.gov
```

This does two things.

First, it updates rolling archives for day-by-day inputs:

```text
KCM Input Data/Weather/seattle_weather_history.csv
KCM Input Data/MVI/seattle_fire_mvi_history.csv
```

Second, it calls `analyze_parameter_bunching_correlations.py`, which uses Spark when `--engine spark` is set.

Main outputs:

```text
parameter_bunching_correlation_output/bus_bunching_count_units.csv
parameter_bunching_correlation_output/hourly_parameter_dataset.csv
parameter_bunching_correlation_output/local_parameter_dataset.csv
parameter_bunching_correlation_output/correlation_summary.csv
parameter_bunching_correlation_output/parameter_lift_summary.csv
parameter_bunching_correlation_output/parameter_correlation_report.txt
```

By default, bunching is counted as `trip-pair-episode`, meaning the same two buses bunching across many stops is counted once per continuous run.

Other counting modes:

```bash
python analyze_parameter_bunching_correlations.py --engine spark --event-unit stop-event
python analyze_parameter_bunching_correlations.py --engine spark --event-unit trip-pair-day
python analyze_parameter_bunching_correlations.py --engine spark --event-unit trip-pair-episode
```

Use `stop-event` only if you intentionally want every stop-level bunching row counted separately.

### 3. Generate Visuals

```bash
python make_bus_bunching_visuals.py
```

This reads `bus_bunching_events.csv` and writes:

```text
bus_bunching_visuals/top_routes.png
bus_bunching_visuals/top_route_directions.png
bus_bunching_visuals/top_stops.png
bus_bunching_visuals/bunching_by_hour.png
bus_bunching_visuals/route_hour_heatmap.png
bus_bunching_visuals/top_stop_locations.png
bus_bunching_visuals/per_route_where_when_summary.csv
```

## Full Databricks Sequence

```bash
python bus_bunching_analysis.py
python update_parameter_archives.py --engine spark --project-root /Workspace/Users/manu.agnihotri@dot.gov
python make_bus_bunching_visuals.py
```

If your repo path differs:

```bash
python update_parameter_archives.py --engine spark --project-root /Workspace/Users/YOUR_USER/YOUR_REPO
```

## Parameter Data Strategy

Weather and Seattle Fire MVI inputs are day-by-day/current feeds. The workflow does not assume they can be backfilled reliably.

Run this job daily or hourly so the archives build up over time:

```bash
python update_parameter_archives.py --engine spark --project-root /Workspace/Users/manu.agnihotri@dot.gov
```

For a 14-day bus bunching CSV, the weather and MVI archives need to contain rows from the same 14-day period. If they do not overlap, the correlation report will warn instead of producing misleading correlations.

## Outputs to Review

Start with:

```text
parameter_bunching_correlation_output/parameter_correlation_report.txt
```

Then inspect:

```text
parameter_bunching_correlation_output/correlation_summary.csv
parameter_bunching_correlation_output/parameter_lift_summary.csv
parameter_bunching_correlation_output/bus_bunching_count_units.csv
```

`bus_bunching_count_units.csv` is useful for auditing how raw stop-level rows were collapsed into trip-pair episodes.

## Notes and Limitations

- API fetches still run on the driver. Spark is used for the analysis after data is in CSV/DataFrame form.
- WSDOT alerts are currently matched locally by text overlap unless usable coordinates are parsed from the WSDOT roadway-location fields.
- SFD MVI matching is approximate unless incident locations are geocoded.
- KCM service advisories are current snapshots. They are not historical correlations unless archived over time.
- `make_bus_bunching_visuals.py` uses Matplotlib and runs on the driver.

## Common Issues

If `Parameter Scripts` cannot be found on Databricks, check path casing. Databricks paths are case-sensitive.

If SFD MVI parsing fails, install `lxml`:

```python
%pip install lxml
```

If weather data does not appear in correlations, check that `seattle_weather_history.csv` overlaps the dates in `bus_bunching_events.csv`.

If Spark analysis is too slow or memory-heavy, start by lowering the bus-bunching date range with `--days-back`, or run the correlation step against a smaller event CSV while debugging.
