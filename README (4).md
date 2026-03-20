# Stepped Care Dashboard Automation

This package rebuilds the **stepped care tracking dashboard** from a daily CSV export and can run automatically in GitHub Actions.

## Expected repo structure

```text
your-repo/
├── data/
│   └── Stepped_Care_Tracking.csv
├── output/
├── build_dashboard.py
├── requirements.txt
└── .github/
    └── workflows/
```

## Expected input file

Place your daily CSV here:

```text
data/Stepped_Care_Tracking.csv
```

Required columns:

- client id
- total_ed
- first_ed
- last_ed
- LOC
- episode_days
- allocation
- annual_alloc
- exceeds
- excess
- recommendation

Column names are matched case-insensitively.

## Local run

```bash
pip install -r requirements.txt
python build_dashboard.py --input data/Stepped_Care_Tracking.csv --output output/stepped_care_dashboard.xlsx
```

## What the workbook contains

- **Dashboard**: KPI cards, charts, and top 10 clients for review
- **Summary_Data**: rollups used by the dashboard
- **Raw_Data**: cleaned source data plus review ranking

## Review ranking logic

The top review list is ordered by:

1. `Exceeds Allocation` = true first
2. higher `Excess`
3. higher `Total ED`
4. longer `Episode Days`

This keeps the ranking transparent and avoids creating an unsupported clinical score.

## GitHub Actions

Two workflows are included:

- `build_dashboard.yml` → builds the workbook daily and uploads it as an artifact
- `build_and_commit_dashboard.yml` → builds the workbook daily, commits it to `output/stepped_care_dashboard.xlsx` if changed, and pushes it back to the repo

## Important GitHub setting

For the auto-commit workflow, enable:

**Settings → Actions → General → Workflow permissions → Read and write permissions**
