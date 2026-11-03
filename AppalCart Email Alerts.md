# AppalCart Email Alerts

This repo now includes `appalcart_email_alerts.py` to send:

1. A once-per-morning daily digest for all predicted AppalCart bunching events that day.
2. A one-hour-ahead alert for events predicted around 60 minutes from now.

## Data Source

Default CSV input:

`Outputs/AppalCart/appalcart_bunching_events.csv`

The script reads `first_predicted_arrival` and related route/stop fields from that CSV.

## Required Email Config

Set these environment variables in your scheduler/job runtime:

`APPALCART_ALERT_FROM`  
`APPALCART_ALERT_TO` (comma-separated recipients)  
`APPALCART_SMTP_HOST`  
`APPALCART_SMTP_PORT` (default `587`)  
`APPALCART_SMTP_USER` (optional if relay allows anonymous send)  
`APPALCART_SMTP_PASSWORD` (optional if relay allows anonymous send)

You can also pass equivalent CLI args instead of env vars.

## Dry Run

```bash
python appalcart_email_alerts.py --mode both --dry-run
```

## Production Commands

Daily digest (once each morning):

```bash
python appalcart_email_alerts.py --mode daily
```

One-hour-ahead alerts (run every 10-15 minutes):

```bash
python appalcart_email_alerts.py --mode upcoming --lead-minutes 60 --window-minutes 10
```

## Scheduler Setup Recommendation

1. Morning digest job:
   Run once daily after the prediction CSV is refreshed (example: 06:00 local).
2. Upcoming-alert job:
   Run every 10-15 minutes through service hours.

The script keeps dedupe state in:

`Outputs/AppalCart/appalcart_alert_state.db`

That prevents:

- Sending the daily digest more than once per day.
- Re-sending the same one-hour event alert on subsequent scheduler runs.

## Important Operational Notes

1. Run your AppalCart prediction pipeline before alert jobs, or alerts will reflect stale CSV data.
2. Keep the alert job timezone aligned with AppalCart operations (default `America/New_York`).
3. If you intentionally need a resend, use `--force`.
