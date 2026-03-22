"""Forecaster pipeline for ED and inpatient risk simulation."""

import argparse
from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd
from lifelines import CoxPHFitter
from lifelines.utils import concordance_index
from patsy import dmatrix
from sklearn.linear_model import Ridge


@dataclass
class ColumnMap:
    patient_id: str = "Medical_ID"
    episode_begin: str = "Episode_Begin"
    locus_initial: str = "LOCUS_Initial"
    locus_assigned: str = "LOCUS_Assigned"
    behavior_score: str = "Behavior_Score"
    med_capacity_cols: tuple = ("Med_Capacity_1", "Med_Capacity_2", "Med_Capacity_3", "Med_Capacity_4")
    days_elapsed: str = "Days_Elapsed"
    ed_procedures: str = "ED_Procedures"
    ed_in: str = "ED_In"
    ed_out: str = "ED_Out"
    inpatient_procedures: str = "Inpt_Procedures"
    inpatient_admit: str = "Inpt_Admit"
    inpatient_discharge: str = "Inpt_Discharge"
    appointments: tuple = ("Case_Mgmt", "Community_Psych", "Spec", "Med_Mgmt")
    week_index: str = "Week_Index"
    week_start: str = "Week_Start"


@dataclass
class SimulationConfig:
    ed_proc_impute: int = 15
    horizon_weeks: int = 52
    simulations: int = 10_000
    ed_flag_threshold: float = 0.25
    inpatient_flag_threshold: float = 0.15


class NVCTMModel:
    """Spline-based variable-coefficient transformation model approximation."""

    def __init__(self, time_col: str, covariates: Iterable[str], spline_df: int = 4):
        self.time_col = time_col
        self.covariates = list(covariates)
        self.spline_df = spline_df
        self.model = Ridge(alpha=1.0)
        self.design_cols: list[str] = []

    def _design_matrix(self, frame: pd.DataFrame) -> pd.DataFrame:
        spline = dmatrix(
            f"bs({self.time_col}, df={self.spline_df}, include_intercept=False)",
            frame,
            return_type="dataframe",
        )
        features = [spline]
        for cov in self.covariates:
            for col in spline.columns:
                features.append(frame[[cov]].mul(spline[col], axis=0).rename(columns={cov: f"{cov}:{col}"}))
        design = pd.concat(features, axis=1)
        self.design_cols = design.columns.tolist()
        return design

    def fit(self, frame: pd.DataFrame, target: pd.Series) -> None:
        design = self._design_matrix(frame)
        self.model.fit(design, target)

    def predict_linear(self, frame: pd.DataFrame) -> np.ndarray:
        design = self._design_matrix(frame)
        return self.model.predict(design)


class ForecasterPipeline:
    def __init__(self, columns: ColumnMap, config: SimulationConfig):
        self.columns = columns
        self.config = config
        self.cox_model = CoxPHFitter(penalizer=0.1)
        self.nvctm: NVCTMModel | None = None

    def _build_features(self, frame: pd.DataFrame) -> pd.DataFrame:
        col = self.columns
        features = pd.DataFrame(index=frame.index)
        features["locus_initial"] = frame[col.locus_initial]
        features["locus_assigned"] = frame[col.locus_assigned]
        features["behavior_score"] = frame[col.behavior_score]
        features["med_capacity_mean"] = frame[list(col.med_capacity_cols)].mean(axis=1)
        features["days_elapsed"] = frame[col.days_elapsed]
        features["week_index"] = frame[col.week_index]
        for appt in col.appointments:
            features[f"{appt}_missed"] = (frame[appt].fillna(0) == 0).astype(int)
        features["missed_appt_streak"] = (
            features[[f"{appt}_missed" for appt in col.appointments]].sum(axis=1)
        )
        features["episode_phase"] = pd.cut(
            frame[col.days_elapsed],
            bins=[-1, 30, 90, 180, 365, np.inf],
            labels=["acute", "stabilizing", "maintenance", "long_term", "extended"],
        ).cat.codes
        return features

    def _prepare_survival_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        col = self.columns
        features = self._build_features(frame)
        features[col.patient_id] = frame[col.patient_id]
        features["duration"] = frame[col.days_elapsed] / 7
        features["event"] = frame[col.ed_procedures].fillna(0).astype(float) > 0
        features["event"] = features["event"].astype(int)
        return features

    def fit(self, frame: pd.DataFrame) -> dict:
        survival = self._prepare_survival_frame(frame)
        self.cox_model.fit(
            survival.drop(columns=[self.columns.patient_id]),
            duration_col="duration",
            event_col="event",
        )
        covariates = [
            "locus_initial",
            "locus_assigned",
            "behavior_score",
            "med_capacity_mean",
            "missed_appt_streak",
            "episode_phase",
        ]
        self.nvctm = NVCTMModel(time_col="duration", covariates=covariates)
        cox_partial = self.cox_model.predict_partial_hazard(survival).values
        self.nvctm.fit(survival.assign(duration=survival["duration"]), np.log1p(cox_partial))
        c_index = concordance_index(survival["duration"], -cox_partial, survival["event"])
        return {"cox_c_index": c_index, "n_rows": len(frame)}

    def predict_risk(self, frame: pd.DataFrame) -> pd.DataFrame:
        features = self._prepare_survival_frame(frame)
        cox_lp = self.cox_model.predict_log_partial_hazard(features)
        if self.nvctm is None:
            raise ValueError("NVCTM model not fit")
        nvctm_lp = self.nvctm.predict_linear(features.assign(duration=features["duration"]))
        combined = 0.7 * cox_lp.values + 0.3 * nvctm_lp
        return pd.DataFrame(
            {
                "cox_lp": cox_lp.values,
                "nvctm_lp": nvctm_lp,
                "combined_lp": combined,
            },
            index=frame.index,
        )

    def simulate_events(self, frame: pd.DataFrame) -> pd.Series:
        risks = self.predict_risk(frame)
        hazard = 1 - np.exp(-np.exp(risks["combined_lp"]))
        rng = np.random.default_rng(42)
        return pd.Series(rng.uniform(size=len(hazard)) < hazard, index=frame.index)

    def apply_simulation(self, frame: pd.DataFrame) -> pd.DataFrame:
        col = self.columns
        output = frame.copy()
        simulated_ed = self.simulate_events(frame)
        output.loc[simulated_ed & output[col.ed_procedures].isna(), col.ed_procedures] = (
            self.config.ed_proc_impute
        )
        output["simulated_ed"] = simulated_ed
        inpt_risk = simulated_ed & (output[col.locus_initial] >= 4)
        output.loc[inpt_risk & output[col.inpatient_procedures].isna(), col.inpatient_procedures] = (
            output[col.locus_initial].clip(upper=6) * 10
        )
        output["simulated_inpt"] = inpt_risk
        output.loc[inpt_risk & output[col.inpatient_admit].isna(), col.inpatient_admit] = (
            pd.to_datetime(output[col.week_start])
        )
        output.loc[inpt_risk & output[col.inpatient_discharge].isna(), col.inpatient_discharge] = (
            pd.to_datetime(output[col.week_start])
            + pd.to_timedelta(output[col.locus_initial].clip(upper=6) * 2, unit="D")
        )
        return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ED/inpatient risk simulation")
    parser.add_argument("--input", required=True, help="Path to Clients_Weekly_withAcuity CSV")
    parser.add_argument("--output", required=True, help="Path to write enriched CSV")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    frame = pd.read_csv(args.input)
    pipeline = ForecasterPipeline(columns=ColumnMap(), config=SimulationConfig())
    metrics = pipeline.fit(frame)
    enriched = pipeline.apply_simulation(frame)
    enriched.to_csv(args.output, index=False)
    print("Fit metrics:", metrics)


if __name__ == "__main__":
    main()
