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


## Auto-commit version
Use `.github/workflows/build_and_commit_dashboard.yml` if you want GitHub Actions to push the newly generated workbook back into the repository each day.

What this workflow does:
- rebuilds `output/patient_recommendation_dashboard.xlsx`
- commits the file only if it changed
- pushes the commit back to your repo
- still uploads the workbook as an Actions artifact

Repository settings to confirm:
1. In GitHub, open **Settings → Actions → General**.
2. Under **Workflow permissions**, set **Read and write permissions**.
3. Make sure your CSV lives at `data/Patient_Summary.csv`, or update the workflow path.

If you would rather keep historical versions, change the output filename to include the date, for example:
`output/patient_recommendation_dashboard_$(date +%F).xlsx`
