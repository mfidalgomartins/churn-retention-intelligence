"""Revenue movement and unit-economics marts.

The module reconciles contract-MRR events into a monthly bridge, then measures
retention-adjusted economics using explicit spend and direct service costs.
Revenue and gross margin remain recurring-value proxies, not accounting revenue.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from churn.common import (
    REFERENCE_DATE,
    last_complete_month_start,
    outputs_tables_dir,
    processed_dir,
    project_root,
    raw_dir,
)

MOVEMENT_TYPES = ("new", "expansion", "contraction", "reactivation", "churn")


def load_inputs(root: Path | None = None) -> dict[str, pd.DataFrame]:
    root = root or project_root()
    raw = root / "data" / "raw" if root != project_root() else raw_dir()
    return {
        "customers": pd.read_csv(raw / "customers.csv", parse_dates=["signup_date"]),
        "subscriptions": pd.read_csv(
            raw / "subscriptions.csv",
            parse_dates=["subscription_start_date", "subscription_end_date"],
        ),
        "movements": pd.read_csv(raw / "revenue_movements.csv", parse_dates=["effective_date"]),
        "spend": pd.read_csv(raw / "acquisition_spend.csv", parse_dates=["spend_month"]),
        "service_costs": pd.read_csv(raw / "service_costs.csv", parse_dates=["cost_month"]),
    }


def load_config(root: Path | None = None) -> dict[str, Any]:
    root = root or project_root()
    return yaml.safe_load((root / "config" / "economics.yml").read_text(encoding="utf-8"))


def build_account_month_ledger(
    customers: pd.DataFrame,
    subscriptions: pd.DataFrame,
    movements: pd.DataFrame,
    service_costs: pd.DataFrame,
    snapshot: pd.Timestamp,
) -> pd.DataFrame:
    """Expand event-level MRR movements to one row per customer and active calendar month."""
    required_movement_types = set(movements["movement_type"].unique())
    invalid_types = sorted(required_movement_types - set(MOVEMENT_TYPES))
    if invalid_types:
        raise ValueError(f"revenue_movements contains invalid movement types: {invalid_types}")
    if movements["movement_id"].duplicated().any():
        raise ValueError("revenue_movements.movement_id must be unique")

    base = customers[
        ["customer_id", "signup_date", "segment", "acquisition_channel", "plan_type"]
    ].merge(
        subscriptions[
            ["customer_id", "subscription_start_date", "subscription_end_date", "status"]
        ],
        on="customer_id",
        how="inner",
        validate="one_to_one",
    )
    grids: list[pd.DataFrame] = []
    for row in base.itertuples(index=False):
        end = (
            pd.Timestamp(row.subscription_end_date)
            if pd.notna(row.subscription_end_date)
            else snapshot
        )
        months = pd.period_range(row.subscription_start_date, end, freq="M").to_timestamp()
        grids.append(pd.DataFrame({"customer_id": row.customer_id, "month": months}))
    grid = pd.concat(grids, ignore_index=True)

    event = movements.copy()
    event["month"] = event["effective_date"].dt.to_period("M").dt.to_timestamp()
    pivot = (
        event.pivot_table(
            index=["customer_id", "month"],
            columns="movement_type",
            values="mrr_delta",
            aggfunc="sum",
            fill_value=0.0,
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )
    for movement_type in MOVEMENT_TYPES:
        if movement_type not in pivot:
            pivot[movement_type] = 0.0

    ledger = grid.merge(pivot, on=["customer_id", "month"], how="left", validate="one_to_one")
    ledger[list(MOVEMENT_TYPES)] = ledger[list(MOVEMENT_TYPES)].fillna(0.0)
    ledger["net_mrr_movement"] = ledger[list(MOVEMENT_TYPES)].sum(axis=1)
    ledger["closing_mrr"] = ledger.groupby("customer_id", sort=False)["net_mrr_movement"].cumsum()
    ledger["opening_mrr"] = (
        ledger.groupby("customer_id", sort=False)["closing_mrr"].shift().fillna(0.0)
    )
    ledger["mrr_revenue_proxy"] = (ledger["opening_mrr"] + ledger["closing_mrr"]) / 2
    ledger["opening_logo"] = ledger["opening_mrr"].gt(0).astype(int)
    ledger["closing_logo"] = ledger["closing_mrr"].gt(0).astype(int)
    ledger["churned_logo"] = ledger["churn"].lt(0).astype(int)

    costs = service_costs.rename(columns={"cost_month": "month"})
    ledger = ledger.merge(
        costs[["customer_id", "month", "direct_service_cost"]],
        on=["customer_id", "month"],
        how="left",
        validate="one_to_one",
    )
    ledger["direct_service_cost"] = ledger["direct_service_cost"].fillna(0.0)
    ledger["gross_profit_proxy"] = ledger["mrr_revenue_proxy"] - ledger["direct_service_cost"]
    ledger = ledger.merge(
        base[["customer_id", "signup_date", "segment", "acquisition_channel", "plan_type"]],
        on="customer_id",
        how="left",
        validate="many_to_one",
    )
    ledger["acquisition_cohort"] = ledger["signup_date"].dt.to_period("M").dt.to_timestamp()

    numeric = [
        *MOVEMENT_TYPES,
        "net_mrr_movement",
        "opening_mrr",
        "closing_mrr",
        "mrr_revenue_proxy",
        "direct_service_cost",
        "gross_profit_proxy",
    ]
    ledger[numeric] = ledger[numeric].round(2)
    return ledger.sort_values(["month", "customer_id"]).reset_index(drop=True)


def build_revenue_bridge(ledger: pd.DataFrame, include_reactivation: bool) -> pd.DataFrame:
    bridge = (
        ledger.groupby("month", as_index=False)
        .agg(
            opening_mrr=("opening_mrr", "sum"),
            new_mrr=("new", "sum"),
            expansion_mrr=("expansion", "sum"),
            contraction_mrr=("contraction", "sum"),
            reactivation_mrr=("reactivation", "sum"),
            churn_mrr=("churn", "sum"),
            closing_mrr=("closing_mrr", "sum"),
            opening_customers=("opening_logo", "sum"),
            closing_customers=("closing_logo", "sum"),
            churned_customers=("churned_logo", "sum"),
            mrr_revenue_proxy=("mrr_revenue_proxy", "sum"),
            direct_service_cost=("direct_service_cost", "sum"),
            gross_profit_proxy=("gross_profit_proxy", "sum"),
        )
        .sort_values("month")
    )
    included_reactivation = bridge["reactivation_mrr"] if include_reactivation else 0.0
    retained_closing = (
        bridge["opening_mrr"]
        + bridge["expansion_mrr"]
        + bridge["contraction_mrr"]
        + included_reactivation
        + bridge["churn_mrr"]
    )
    gross_retained = bridge["opening_mrr"] + bridge["contraction_mrr"] + bridge["churn_mrr"]
    denominator = bridge["opening_mrr"].replace(0, np.nan)
    bridge["nrr"] = retained_closing.div(denominator)
    bridge["grr"] = gross_retained.clip(lower=0).div(denominator)
    bridge["logo_churn_rate"] = bridge["churned_customers"].div(
        bridge["opening_customers"].replace(0, np.nan)
    )
    bridge["gross_margin_rate"] = bridge["gross_profit_proxy"].div(
        bridge["mrr_revenue_proxy"].replace(0, np.nan)
    )
    expected_closing = bridge["opening_mrr"] + bridge[
        ["new_mrr", "expansion_mrr", "contraction_mrr", "reactivation_mrr", "churn_mrr"]
    ].sum(axis=1)
    bridge["reconciliation_diff"] = bridge["closing_mrr"] - expected_closing
    money_columns = [
        "opening_mrr",
        "new_mrr",
        "expansion_mrr",
        "contraction_mrr",
        "reactivation_mrr",
        "churn_mrr",
        "closing_mrr",
        "mrr_revenue_proxy",
        "direct_service_cost",
        "gross_profit_proxy",
        "reconciliation_diff",
    ]
    bridge[money_columns] = bridge[money_columns].round(2)
    bridge[["nrr", "grr", "logo_churn_rate", "gross_margin_rate"]] = bridge[
        ["nrr", "grr", "logo_churn_rate", "gross_margin_rate"]
    ].round(6)
    bridge["month"] = bridge["month"].dt.date.astype(str)
    return bridge


def _survival_ltv(
    monthly_arpa: pd.Series,
    gross_margin_rate: pd.Series,
    monthly_churn_rate: pd.Series,
    horizon_months: int,
) -> pd.Series:
    churn = monthly_churn_rate.clip(lower=1e-6, upper=0.999999)
    survival_months = (1 - (1 - churn) ** horizon_months) / churn
    return monthly_arpa * gross_margin_rate.clip(lower=0) * survival_months


def build_unit_economics(
    ledger: pd.DataFrame,
    customers: pd.DataFrame,
    spend: pd.DataFrame,
    horizon_months: int,
    complete_month: pd.Timestamp,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    acquisition = customers.copy()
    acquisition["acquisition_cohort"] = (
        acquisition["signup_date"].dt.to_period("M").dt.to_timestamp()
    )
    spend_by_channel = spend.assign(total_spend=spend["marketing_spend"] + spend["sales_spend"])
    spend_by_channel = spend_by_channel.groupby("acquisition_channel", as_index=False).agg(
        marketing_spend=("marketing_spend", "sum"),
        sales_spend=("sales_spend", "sum"),
        total_acquisition_spend=("total_spend", "sum"),
    )

    completed_ledger = ledger[ledger["month"] <= complete_month].copy()
    operational = completed_ledger.groupby("acquisition_channel", as_index=False).agg(
        active_account_months=("opening_logo", "sum"),
        churn_events=("churned_logo", "sum"),
        opening_mrr=("opening_mrr", "sum"),
        mrr_revenue_proxy=("mrr_revenue_proxy", "sum"),
        direct_service_cost=("direct_service_cost", "sum"),
        gross_profit_proxy=("gross_profit_proxy", "sum"),
    )
    channel = (
        acquisition.groupby("acquisition_channel", as_index=False)
        .agg(acquired_customers=("customer_id", "nunique"))
        .merge(spend_by_channel, on="acquisition_channel", how="left", validate="one_to_one")
        .merge(operational, on="acquisition_channel", how="left", validate="one_to_one")
    )
    channel["cac"] = channel["total_acquisition_spend"] / channel["acquired_customers"]
    channel["monthly_logo_churn_rate"] = channel["churn_events"] / channel["active_account_months"]
    channel["average_mrr_per_active_account"] = (
        channel["opening_mrr"] / channel["active_account_months"]
    )
    channel["gross_margin_rate"] = channel["gross_profit_proxy"] / channel["mrr_revenue_proxy"]
    channel["monthly_gross_profit_per_account"] = (
        channel["average_mrr_per_active_account"] * channel["gross_margin_rate"]
    )
    channel["cac_payback_months"] = channel["cac"] / channel[
        "monthly_gross_profit_per_account"
    ].replace(0, np.nan)
    ltv_column = f"modelled_ltv_{horizon_months}m"
    ltv_to_cac_column = f"ltv_to_cac_{horizon_months}m"
    channel[ltv_column] = _survival_ltv(
        channel["average_mrr_per_active_account"],
        channel["gross_margin_rate"],
        channel["monthly_logo_churn_rate"],
        horizon_months,
    )
    channel[ltv_to_cac_column] = channel[ltv_column] / channel["cac"]

    channel_cac = channel.set_index("acquisition_channel")["cac"]
    account = completed_ledger.groupby("customer_id", as_index=False).agg(
        realized_revenue_proxy=("mrr_revenue_proxy", "sum"),
        realized_direct_service_cost=("direct_service_cost", "sum"),
        realized_gross_margin_ltv=("gross_profit_proxy", "sum"),
    )
    current_mrr = ledger.groupby("customer_id", as_index=False).agg(
        current_mrr=("closing_mrr", "last")
    )
    account = account.merge(current_mrr, on="customer_id", how="outer", validate="one_to_one")
    account = account.merge(
        acquisition[
            [
                "customer_id",
                "segment",
                "acquisition_channel",
                "plan_type",
                "acquisition_cohort",
            ]
        ],
        on="customer_id",
        how="left",
        validate="one_to_one",
    )
    account["allocated_cac"] = account["acquisition_channel"].map(channel_cac)
    account["realized_ltv_to_cac"] = account["realized_gross_margin_ltv"] / account["allocated_cac"]

    segment = account.groupby("segment", as_index=False).agg(
        customers=("customer_id", "nunique"),
        average_allocated_cac=("allocated_cac", "mean"),
        average_realized_gross_margin_ltv=("realized_gross_margin_ltv", "mean"),
        median_realized_gross_margin_ltv=("realized_gross_margin_ltv", "median"),
        average_realized_ltv_to_cac=("realized_ltv_to_cac", "mean"),
        current_mrr=("current_mrr", "sum"),
    )
    cohort = account.groupby("acquisition_cohort", as_index=False).agg(
        acquired_customers=("customer_id", "nunique"),
        allocated_acquisition_cost=("allocated_cac", "sum"),
        realized_revenue_proxy=("realized_revenue_proxy", "sum"),
        realized_gross_margin=("realized_gross_margin_ltv", "sum"),
        average_realized_gross_margin_ltv=("realized_gross_margin_ltv", "mean"),
        current_mrr=("current_mrr", "sum"),
    )
    cohort["realized_ltv_to_cac"] = (
        cohort["realized_gross_margin"] / cohort["allocated_acquisition_cost"]
    )

    numeric_channel = [column for column in channel.columns if column != "acquisition_channel"]
    channel[numeric_channel] = channel[numeric_channel].round(4)
    numeric_segment = [column for column in segment.columns if column != "segment"]
    segment[numeric_segment] = segment[numeric_segment].round(4)
    numeric_cohort = [column for column in cohort.columns if column != "acquisition_cohort"]
    cohort[numeric_cohort] = cohort[numeric_cohort].round(4)
    cohort["acquisition_cohort"] = cohort["acquisition_cohort"].dt.date.astype(str)
    return channel, segment, cohort, account


def build_summary(
    bridge: pd.DataFrame,
    channel: pd.DataFrame,
    account: pd.DataFrame,
    horizon_months: int,
    complete_month: pd.Timestamp,
) -> pd.DataFrame:
    mature_bridge = bridge[
        (pd.to_numeric(bridge["opening_customers"]) > 0)
        & (pd.to_datetime(bridge["month"]) <= complete_month)
    ]
    total_spend = float(channel["total_acquisition_spend"].sum())
    total_customers = int(channel["acquired_customers"].sum())
    overall_revenue = float(mature_bridge["mrr_revenue_proxy"].sum())
    overall_gross_profit = float(mature_bridge["gross_profit_proxy"].sum())
    metrics = [
        (
            "average_monthly_nrr",
            float(mature_bridge["nrr"].mean()),
            "ratio",
            "Mean monthly NRR; new MRR excluded and reactivation included per config.",
        ),
        (
            "average_monthly_grr",
            float(mature_bridge["grr"].mean()),
            "ratio",
            "Mean monthly GRR after contraction and churn; expansion and reactivation excluded.",
        ),
        (
            "gross_margin_rate",
            overall_gross_profit / overall_revenue if overall_revenue else 0.0,
            "ratio",
            "Recurring-value proxy less direct service cost, divided by recurring-value proxy.",
        ),
        (
            "blended_cac",
            total_spend / total_customers if total_customers else 0.0,
            "USD_per_customer",
            "Marketing plus sales spend divided by acquired customers.",
        ),
        (
            "average_realized_gross_margin_ltv",
            float(account["realized_gross_margin_ltv"].mean()),
            "USD_per_customer",
            "Observed cumulative gross-margin proxy; cohorts have unequal follow-up.",
        ),
        (
            f"median_modelled_ltv_{horizon_months}m",
            float(channel[f"modelled_ltv_{horizon_months}m"].median()),
            "USD_per_customer",
            f"Gross-profit survival model capped at {horizon_months} months.",
        ),
    ]
    return pd.DataFrame(metrics, columns=["metric", "value", "unit", "definition"]).assign(
        value=lambda frame: frame["value"].round(6)
    )


def main() -> None:
    inputs = load_inputs()
    config = load_config()
    ledger = build_account_month_ledger(
        inputs["customers"],
        inputs["subscriptions"],
        inputs["movements"],
        inputs["service_costs"],
        REFERENCE_DATE,
    )
    bridge = build_revenue_bridge(
        ledger,
        include_reactivation=bool(config["include_reactivation_in_nrr"]),
    )
    max_reconciliation_diff = float(bridge["reconciliation_diff"].abs().max())
    if max_reconciliation_diff > 0.01:
        raise ValueError(
            f"MRR bridge failed reconciliation: max_diff={max_reconciliation_diff:.4f}"
        )

    horizon_months = int(config["ltv_horizon_months"])
    complete_month = last_complete_month_start(REFERENCE_DATE)
    channel, segment, cohort, account = build_unit_economics(
        ledger,
        inputs["customers"],
        inputs["spend"],
        horizon_months,
        complete_month,
    )
    summary = build_summary(bridge, channel, account, horizon_months, complete_month)

    processed = processed_dir()
    tables = outputs_tables_dir()
    processed.mkdir(parents=True, exist_ok=True)
    tables.mkdir(parents=True, exist_ok=True)
    ledger.to_csv(processed / "account_month_economics.csv", index=False, date_format="%Y-%m-%d")
    bridge.to_csv(tables / "revenue_movement_bridge.csv", index=False)
    channel.to_csv(tables / "unit_economics_by_channel.csv", index=False)
    segment.to_csv(tables / "unit_economics_by_segment.csv", index=False)
    cohort.to_csv(tables / "cohort_unit_economics.csv", index=False)
    summary.to_csv(tables / "unit_economics_summary.csv", index=False)

    metadata = {
        "as_of_date": REFERENCE_DATE.date().isoformat(),
        "currency": config["currency"],
        "ltv_horizon_months": horizon_months,
        "revenue_basis": config["revenue_basis"],
        "latest_complete_month": complete_month.date().isoformat(),
        "max_bridge_reconciliation_diff": max_reconciliation_diff,
    }
    (tables / "unit_economics_metadata.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        f"Economics built: account_months={len(ledger):,}, "
        f"bridge_months={len(bridge)}, max_reconciliation_diff={max_reconciliation_diff:.2f}."
    )


if __name__ == "__main__":
    main()
