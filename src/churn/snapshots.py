"""Point-in-time customer snapshots for temporal churn modelling."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from churn.common import REFERENCE_DATE, processed_dir, project_root, raw_dir


def load_config(root: Path | None = None) -> dict[str, Any]:
    root = root or project_root()
    return yaml.safe_load((root / "config" / "modeling.yml").read_text(encoding="utf-8"))


def load_inputs(root: Path | None = None) -> dict[str, pd.DataFrame]:
    root = root or project_root()
    raw = root / "data" / "raw" if root != project_root() else raw_dir()
    processed = root / "data" / "processed" if root != project_root() else processed_dir()
    return {
        "customers": pd.read_csv(raw / "customers.csv", parse_dates=["signup_date"]),
        "subscriptions": pd.read_csv(
            raw / "subscriptions.csv",
            parse_dates=["subscription_start_date", "subscription_end_date"],
        ),
        "usage": pd.read_csv(raw / "product_usage.csv", parse_dates=["usage_date"]),
        "payments": pd.read_csv(raw / "payments.csv", parse_dates=["payment_date"]),
        "account_month": pd.read_csv(
            processed / "account_month_economics.csv", parse_dates=["month"]
        ),
    }


def build_observation_grid(
    customers: pd.DataFrame,
    subscriptions: pd.DataFrame,
    account_month: pd.DataFrame,
    snapshot: pd.Timestamp,
    horizon_days: int,
    minimum_history_days: int,
) -> pd.DataFrame:
    """Create active month-end observations and availability-safe future labels."""
    base = customers.merge(subscriptions, on="customer_id", how="inner", validate="one_to_one")
    month_mrr = account_month[["customer_id", "month", "closing_mrr"]].copy()
    month_mrr["observation_date"] = month_mrr["month"] + pd.offsets.MonthEnd(0)
    month_mrr["observation_date"] = month_mrr["observation_date"].clip(upper=snapshot)
    grid = month_mrr.merge(base, on="customer_id", how="left", validate="many_to_one")
    active_at_observation = (
        grid["subscription_start_date"].le(grid["observation_date"])
        & (
            grid["subscription_end_date"].isna()
            | grid["subscription_end_date"].gt(grid["observation_date"])
        )
        & grid["closing_mrr"].gt(0)
    )
    grid = grid[active_at_observation].copy()
    grid["tenure_days"] = (grid["observation_date"] - grid["subscription_start_date"]).dt.days
    grid = grid[grid["tenure_days"] >= minimum_history_days].copy()
    grid["current_mrr"] = grid["closing_mrr"]
    grid["label_end_date"] = grid["observation_date"] + pd.to_timedelta(horizon_days, unit="D")
    grid["label_available"] = grid["label_end_date"].le(snapshot).astype(int)
    grid["churn_within_horizon"] = (
        grid["subscription_end_date"].notna()
        & grid["subscription_end_date"].gt(grid["observation_date"])
        & grid["subscription_end_date"].le(grid["label_end_date"])
    ).astype(int)
    grid.loc[grid["label_available"].eq(0), "churn_within_horizon"] = pd.NA
    return grid.reset_index(drop=True)


def _window_sums(
    observation_dates: np.ndarray,
    event_dates: np.ndarray,
    values: np.ndarray,
    window_days: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Return sums and counts for (observation-window, observation] intervals."""
    right = np.searchsorted(event_dates, observation_dates, side="right")
    lower_dates = observation_dates - np.timedelta64(window_days, "D")
    left = np.searchsorted(event_dates, lower_dates, side="right")
    cumulative = np.concatenate(([0.0], np.cumsum(values, dtype=float)))
    sums = cumulative[right] - cumulative[left]
    return sums, right - left


def add_usage_features(grid: pd.DataFrame, usage: pd.DataFrame) -> pd.DataFrame:
    out = grid.copy()
    feature_columns = [
        "recent_sessions_30d",
        "recent_sessions_90d",
        "usage_trend",
        "feature_adoption_score_recent",
        "support_tickets_30d",
        "support_tickets_90d",
        "nps_score_recent",
    ]
    for column in feature_columns:
        out[column] = 0.0

    usage_groups = {
        customer_id: group.sort_values("usage_date")
        for customer_id, group in usage.groupby("customer_id", sort=False)
    }
    for customer_id, observations in out.groupby("customer_id", sort=False):
        events = usage_groups.get(customer_id)
        if events is None or events.empty:
            continue
        indexes = observations.index.to_numpy()
        observation_dates = observations["observation_date"].to_numpy(dtype="datetime64[ns]")
        event_dates = events["usage_date"].to_numpy(dtype="datetime64[ns]")

        sessions = events["sessions"].to_numpy(dtype=float)
        session_30, count_30 = _window_sums(observation_dates, event_dates, sessions, 30)
        session_60, count_60 = _window_sums(observation_dates, event_dates, sessions, 60)
        session_90, _count_90 = _window_sums(observation_dates, event_dates, sessions, 90)
        recent_mean = np.divide(
            session_30,
            count_30,
            out=np.zeros_like(session_30),
            where=count_30 > 0,
        )
        prior_sum = session_60 - session_30
        prior_count = count_60 - count_30
        prior_mean = np.divide(
            prior_sum,
            prior_count,
            out=np.zeros_like(prior_sum),
            where=prior_count > 0,
        )

        tickets = events["support_tickets"].to_numpy(dtype=float)
        ticket_30, _ = _window_sums(observation_dates, event_dates, tickets, 30)
        ticket_90, _ = _window_sums(observation_dates, event_dates, tickets, 90)
        adoption_sum, adoption_count = _window_sums(
            observation_dates,
            event_dates,
            events["feature_adoption_score"].to_numpy(dtype=float),
            30,
        )
        nps_sum, nps_count = _window_sums(
            observation_dates,
            event_dates,
            events["nps_score"].to_numpy(dtype=float),
            90,
        )

        out.loc[indexes, "recent_sessions_30d"] = session_30
        out.loc[indexes, "recent_sessions_90d"] = session_90
        out.loc[indexes, "usage_trend"] = recent_mean - prior_mean
        out.loc[indexes, "feature_adoption_score_recent"] = np.divide(
            adoption_sum,
            adoption_count,
            out=np.zeros_like(adoption_sum),
            where=adoption_count > 0,
        )
        out.loc[indexes, "support_tickets_30d"] = ticket_30
        out.loc[indexes, "support_tickets_90d"] = ticket_90
        out.loc[indexes, "nps_score_recent"] = np.divide(
            nps_sum,
            nps_count,
            out=np.zeros_like(nps_sum),
            where=nps_count > 0,
        )
    return out


def add_payment_features(grid: pd.DataFrame, payments: pd.DataFrame) -> pd.DataFrame:
    out = grid.copy()
    out["failed_payments_90d"] = 0
    failed = payments.assign(is_failed=payments["payment_status"].eq("failed").astype(int))
    payment_groups = {
        customer_id: group.sort_values("payment_date")
        for customer_id, group in failed.groupby("customer_id", sort=False)
    }
    for customer_id, observations in out.groupby("customer_id", sort=False):
        events = payment_groups.get(customer_id)
        if events is None or events.empty:
            continue
        indexes = observations.index.to_numpy()
        failed_count, _ = _window_sums(
            observations["observation_date"].to_numpy(dtype="datetime64[ns]"),
            events["payment_date"].to_numpy(dtype="datetime64[ns]"),
            events["is_failed"].to_numpy(dtype=float),
            90,
        )
        out.loc[indexes, "failed_payments_90d"] = failed_count.astype(int)
    out["failed_payments_90d"] = out["failed_payments_90d"].astype(int)
    out["payment_failure_flag"] = out["failed_payments_90d"].gt(0).astype(int)
    return out


def add_renewal_feature(grid: pd.DataFrame) -> pd.DataFrame:
    out = grid.copy()
    flags: list[int] = []
    for row in out.itertuples(index=False):
        term_months = 12 if row.contract_type == "Annual" else 1
        start = pd.Timestamp(row.subscription_start_date)
        observation = pd.Timestamp(row.observation_date)
        elapsed_months = (observation.year - start.year) * 12 + observation.month - start.month
        completed_terms = max(elapsed_months // term_months, 0)
        renewal = start + pd.DateOffset(months=completed_terms * term_months)
        if renewal <= observation:
            renewal += pd.DateOffset(months=term_months)
        flags.append(int(0 <= (renewal - observation).days <= 45))
    out["renewal_near_flag"] = flags
    return out


def build_monthly_snapshots(
    inputs: dict[str, pd.DataFrame],
    config: dict[str, Any],
    snapshot: pd.Timestamp,
) -> pd.DataFrame:
    grid = build_observation_grid(
        inputs["customers"],
        inputs["subscriptions"],
        inputs["account_month"],
        snapshot,
        horizon_days=int(config["outcome_horizon_days"]),
        minimum_history_days=int(config["minimum_history_days"]),
    )
    grid = add_usage_features(grid, inputs["usage"])
    grid = add_payment_features(grid, inputs["payments"])
    grid = add_renewal_feature(grid)

    integer_columns = [
        "tenure_days",
        "recent_sessions_30d",
        "recent_sessions_90d",
        "support_tickets_30d",
        "support_tickets_90d",
        "failed_payments_90d",
        "payment_failure_flag",
        "renewal_near_flag",
        "label_available",
    ]
    grid[integer_columns] = grid[integer_columns].round().astype(int)
    float_columns = [
        "current_mrr",
        "usage_trend",
        "feature_adoption_score_recent",
        "nps_score_recent",
    ]
    grid[float_columns] = grid[float_columns].round(4)
    output_columns = [
        "customer_id",
        "observation_date",
        "label_end_date",
        "label_available",
        "churn_within_horizon",
        "segment",
        "region",
        "acquisition_channel",
        "plan_type",
        "tenure_days",
        "current_mrr",
        "recent_sessions_30d",
        "recent_sessions_90d",
        "usage_trend",
        "feature_adoption_score_recent",
        "support_tickets_30d",
        "support_tickets_90d",
        "nps_score_recent",
        "failed_payments_90d",
        "payment_failure_flag",
        "renewal_near_flag",
    ]
    return (
        grid[output_columns].sort_values(["observation_date", "customer_id"]).reset_index(drop=True)
    )


def main() -> None:
    config = load_config()
    snapshots = build_monthly_snapshots(load_inputs(), config, REFERENCE_DATE)
    output = processed_dir() / "customer_monthly_snapshots.csv"
    output.parent.mkdir(parents=True, exist_ok=True)
    snapshots.to_csv(output, index=False, date_format="%Y-%m-%d")
    labelled = int(snapshots["label_available"].sum())
    positives = int(pd.to_numeric(snapshots["churn_within_horizon"], errors="coerce").sum())
    print(
        f"Monthly snapshots built: rows={len(snapshots):,}, labelled={labelled:,}, "
        f"positive_90d_outcomes={positives:,}."
    )


if __name__ == "__main__":
    main()
