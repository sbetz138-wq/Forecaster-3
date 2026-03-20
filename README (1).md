# Patient recommendation dashboard

This script rebuilds the Excel dashboard from a daily CSV export.

## Files
- `build_dashboard.py` — creates the workbook
- `requirements.txt` — Python dependencies
- `.github/workflows/build_dashboard.yml` — example GitHub Actions job

## Expected input
The script expects a CSV with these columns:

- `client id`
- `loc`
- `total_days`
- `ed_visits`
- `inpt_admissions`
- `mean_daily_risk`
- `max_daily_risk`
- `days_med_mgmt_zero`
- `max_consec_med_miss`
- `mean_bprs`
- `allocation`
- `annual_alloc`
- `episode_days`
- `exceeds`
- `excess`
- `recommendation`

## Local run
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python build_dashboard.py --input data/Patient_Summary.csv --output output/patient_recommendation_dashboard.xlsx
```

## What the workbook contains
- `Dashboard` sheet with KPI cards, charts, and top-priority patients
- `Raw_Data` sheet with the imported patient rows plus priority score and priority rank
- `Summary_Data` sheet with the supporting metrics for the dashboard

## Daily GitHub run
The included workflow runs once per day and also supports manual runs from the GitHub Actions tab.

GitHub cron schedules are in UTC. Adjust the cron line if you want a different Pacific time.
