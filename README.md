# Forecaster-3

Create a forecaster for ED visits and inpatient admissions for people affected by SMI using risk thresholds.

## Pipeline usage

Run the simulation using the provided CSV ("Clients_Weekly_withAcuity (3)") and write an enriched output.

```bash
python forecaster_pipeline.py --input "Clients_Weekly_withAcuity (3).csv" --output enriched_forecast.csv
```

The pipeline fits a Cox PH model for interpretability and a spline-based NVCTM approximation for nonlinear, time-varying effects, then simulates missing ED and inpatient events to populate the incomplete procedure columns.
