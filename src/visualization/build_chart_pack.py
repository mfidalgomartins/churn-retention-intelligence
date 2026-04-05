from __future__ import annotations

from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter


COLORS = {
    "primary": "#0B3C5D",
    "secondary": "#328CC1",
    "accent": "#D9B310",
    "danger": "#C0392B",
    "success": "#1E8449",
    "neutral": "#7F8C8D",
    "light": "#ECF0F1",
}

RISK_TIER_COLORS = {
    "critical": "#8E1B1B",
    "high": "#C0392B",
    "medium": "#D9B310",
    "low": "#2E86C1",
}


def pct_fmt(x: float, _: int | None = None) -> str:
    return f"{x * 100:.0f}%"


def currency_fmt(x: float, _: int | None = None) -> str:
    if abs(x) >= 1_000_000:
        return f"${x/1_000_000:.1f}M"
    if abs(x) >= 1_000:
        return f"${x/1_000:.0f}K"
    return f"${x:.0f}"


def count_fmt(x: float, _: int | None = None) -> str:
    if abs(x) >= 1_000_000:
        return f"{x/1_000_000:.1f}M"
    if abs(x) >= 1_000:
        return f"{x/1_000:.0f}K"
    return f"{x:.0f}"


def set_style() -> None:
    plt.rcParams.update(
        {
            "figure.dpi": 160,
            "savefig.dpi": 300,
            "font.size": 11,
            "axes.titlesize": 14,
            "axes.titleweight": "bold",
            "axes.labelsize": 11,
            "axes.grid": True,
            "grid.alpha": 0.25,
            "grid.linestyle": "--",
            "axes.spines.top": False,
            "axes.spines.right": False,
            "legend.frameon": False,
        }
    )


def save_fig(fig: plt.Figure, path: Path) -> None:
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def load_inputs(project_root: Path) -> dict[str, pd.DataFrame]:
    inputs = {
        "trend": pd.read_csv(project_root / "outputs" / "tables" / "overall_retention_trend_monthly.csv", parse_dates=["month"]),
        "cohort": pd.read_csv(project_root / "data" / "processed" / "cohort_retention_table.csv", parse_dates=["cohort_month", "observation_month"]),
        "churn_segment": pd.read_csv(project_root / "outputs" / "tables" / "churn_by_segment.csv"),
        "churn_channel": pd.read_csv(project_root / "outputs" / "tables" / "churn_by_acquisition_channel.csv"),
        "churn_plan": pd.read_csv(project_root / "outputs" / "tables" / "churn_by_plan_type.csv"),
        "features": pd.read_csv(project_root / "data" / "processed" / "customer_retention_features.csv"),
        "segment_risk": pd.read_csv(project_root / "outputs" / "tables" / "segment_revenue_risk_contribution.csv"),
        "risk_scores": pd.read_csv(project_root / "data" / "processed" / "customer_risk_scores.csv"),
        "risk_ranked": pd.read_csv(project_root / "data" / "processed" / "customer_risk_priority_ranked.csv"),
    }
    return inputs


def chart_customer_churn_trend(trend: pd.DataFrame, out_path: Path) -> tuple[str, str]:
    fig, ax = plt.subplots(figsize=(11, 5.5))
    t = trend.copy().sort_values("month")

    ax.plot(t["month"], t["customer_churn_rate"], color=COLORS["danger"], lw=2.2, label="Customer churn rate")
    ax.plot(
        t["month"],
        t["customer_churn_rate"].rolling(3, min_periods=1).mean(),
        color=COLORS["primary"],
        lw=2,
        linestyle="--",
        label="3-month average",
    )

    ax.set_title("Customer Churn Has Remained Elevated Despite Lower Recent Volatility")
    ax.set_ylabel("Customer Churn Rate")
    ax.yaxis.set_major_formatter(FuncFormatter(pct_fmt))
    ax.set_xlabel("Month")
    ax.legend(loc="upper right")

    save_fig(fig, out_path)
    return (
        "customer_churn_trend_over_time.png",
        "Tracks whether customer churn is improving or worsening over time and anchors overall retention health.",
    )


def chart_revenue_churn_trend(trend: pd.DataFrame, out_path: Path) -> tuple[str, str]:
    fig, ax = plt.subplots(figsize=(11, 5.5))
    t = trend.copy().sort_values("month")

    ax.plot(t["month"], t["revenue_churn_rate"], color=COLORS["primary"], lw=2.2, label="Revenue churn rate")
    ax.plot(
        t["month"],
        t["revenue_churn_rate"].rolling(3, min_periods=1).mean(),
        color=COLORS["accent"],
        lw=2,
        linestyle="--",
        label="3-month average",
    )

    ax.set_title("Revenue Churn Trend Shows Value Leakage Over Time")
    ax.set_ylabel("Revenue Churn Rate")
    ax.yaxis.set_major_formatter(FuncFormatter(pct_fmt))
    ax.set_xlabel("Month")
    ax.legend(loc="upper right")

    save_fig(fig, out_path)
    return (
        "revenue_churn_trend_over_time.png",
        "Shows how much recurring revenue is being lost over time and whether value loss is concentrated.",
    )


def _add_cohort_age(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["cohort_age_months"] = (
        (out["observation_month"].dt.year - out["cohort_month"].dt.year) * 12
        + (out["observation_month"].dt.month - out["cohort_month"].dt.month)
    )
    return out


def chart_cohort_retention_curves(cohort: pd.DataFrame, out_path: Path) -> tuple[str, str]:
    c = _add_cohort_age(cohort)
    c = c[c["cohort_age_months"].between(0, 18)]

    cohort_order = np.sort(c["cohort_month"].unique())
    if len(cohort_order) > 10:
        idx = np.linspace(0, len(cohort_order) - 1, 10, dtype=int)
        selected = cohort_order[idx]
    else:
        selected = cohort_order

    fig, ax = plt.subplots(figsize=(11.5, 6))
    color_map = plt.get_cmap("viridis")

    for i, cm in enumerate(selected):
        d = c[c["cohort_month"] == cm].sort_values("cohort_age_months")
        ax.plot(
            d["cohort_age_months"],
            d["retention_rate"],
            lw=2,
            color=color_map(i / max(len(selected) - 1, 1)),
            label=pd.Timestamp(cm).strftime("%Y-%m"),
        )

    ax.set_title("Signup Cohort Retention Curves Highlight Early-Lifecycle Drop-Off")
    ax.set_xlabel("Cohort Age (Months)")
    ax.set_ylabel("Retention Rate")
    ax.yaxis.set_major_formatter(FuncFormatter(pct_fmt))
    ax.legend(title="Cohort", ncols=2, fontsize=9)

    save_fig(fig, out_path)
    return (
        "cohort_retention_curves.png",
        "Compares customer survival patterns by signup cohort to assess onboarding and lifecycle quality.",
    )


def chart_revenue_retention_by_cohort(cohort: pd.DataFrame, out_path: Path) -> tuple[str, str]:
    c = _add_cohort_age(cohort)
    c = c[c["cohort_age_months"].between(0, 12)]

    latest_18 = np.sort(c["cohort_month"].unique())[-18:]
    c = c[c["cohort_month"].isin(latest_18)].copy()

    pivot = c.pivot_table(index="cohort_month", columns="cohort_age_months", values="revenue_retention")
    pivot = pivot.sort_index(ascending=True)

    fig, ax = plt.subplots(figsize=(12, 7))
    im = ax.imshow(pivot.values, aspect="auto", cmap="YlGnBu", vmin=0, vmax=1)

    ax.set_title("Revenue Retention by Cohort Reveals Monetization Durability")
    ax.set_xlabel("Cohort Age (Months)")
    ax.set_ylabel("Cohort Month")
    ax.set_xticks(np.arange(len(pivot.columns)))
    ax.set_xticklabels([str(x) for x in pivot.columns], rotation=0)
    ax.set_yticks(np.arange(len(pivot.index)))
    ax.set_yticklabels([pd.Timestamp(x).strftime("%Y-%m") for x in pivot.index])

    cbar = fig.colorbar(im, ax=ax, shrink=0.88)
    cbar.ax.yaxis.set_major_formatter(FuncFormatter(pct_fmt))
    cbar.set_label("Revenue Retention")

    save_fig(fig, out_path)
    return (
        "revenue_retention_by_cohort.png",
        "Evaluates which cohorts preserve the most revenue and where monetization erosion begins.",
    )


def _bar_churn_rate(df: pd.DataFrame, category_col: str, title: str, out_path: Path) -> tuple[str, str]:
    d = df.copy().sort_values("churn_rate", ascending=False)

    fig, ax = plt.subplots(figsize=(10.5, 5.5))
    bars = ax.bar(d[category_col], d["churn_rate"], color=COLORS["secondary"], edgecolor="white")
    ax.set_title(title)
    ax.set_ylabel("Churn Rate")
    ax.yaxis.set_major_formatter(FuncFormatter(pct_fmt))
    ax.set_xlabel("")
    ax.tick_params(axis="x", rotation=25)

    for bar, val in zip(bars, d["churn_rate"]):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.005, f"{val*100:.1f}%", ha="center", va="bottom", fontsize=9)

    save_fig(fig, out_path)

    fname = out_path.name
    if category_col == "segment":
        narrative = "Identifies which customer segments contribute disproportionately to churn volume."
    elif category_col == "acquisition_channel":
        narrative = "Highlights acquisition sources with weaker retention quality and potential CAC inefficiency."
    else:
        narrative = "Shows whether specific plans systematically underperform on retention."

    return fname, narrative


def chart_usage_trend_churned_vs_retained(features: pd.DataFrame, out_path: Path) -> tuple[str, str]:
    f = features.copy()
    labels = ["Retained", "Churned"]
    retained = f.loc[f["churn_flag"] == 0, "usage_trend"].to_numpy()
    churned = f.loc[f["churn_flag"] == 1, "usage_trend"].to_numpy()

    fig, ax = plt.subplots(figsize=(8.5, 5.5))
    box = ax.boxplot(
        [retained, churned],
        tick_labels=labels,
        patch_artist=True,
        showfliers=False,
        medianprops={"color": "black", "linewidth": 1.6},
    )

    for patch, color in zip(box["boxes"], [COLORS["success"], COLORS["danger"]]):
        patch.set_facecolor(color)
        patch.set_alpha(0.55)

    ax.axhline(0, color=COLORS["neutral"], lw=1.2, linestyle="--")
    ax.set_title("Churned Accounts Show Meaningfully Weaker Usage Momentum")
    ax.set_ylabel("Usage Trend (Recent 30d Avg Sessions - Prior 30d Avg Sessions)")

    save_fig(fig, out_path)
    return (
        "usage_trend_churned_vs_retained.png",
        "Demonstrates relationship between pre-churn behavior decay and realized churn.",
    )


def chart_support_tickets_churned_vs_retained(features: pd.DataFrame, out_path: Path) -> tuple[str, str]:
    f = features.copy()
    summary = (
        f.groupby("churn_flag", as_index=False)
        .agg(avg_support_tickets_90d=("support_tickets_90d", "mean"), p75_support_tickets_90d=("support_tickets_90d", lambda x: x.quantile(0.75)))
        .sort_values("churn_flag")
    )
    summary["group"] = summary["churn_flag"].map({0: "Retained", 1: "Churned"})

    fig, ax = plt.subplots(figsize=(8.8, 5.5))
    x = np.arange(len(summary))
    width = 0.35

    ax.bar(x - width / 2, summary["avg_support_tickets_90d"], width, label="Average", color=COLORS["secondary"])
    ax.bar(x + width / 2, summary["p75_support_tickets_90d"], width, label="75th percentile", color=COLORS["accent"])

    ax.set_xticks(x)
    ax.set_xticklabels(summary["group"])
    ax.set_ylabel("Support Tickets (90d)")
    ax.set_title("Churned Customers Carry Higher Support Burden")
    ax.legend(loc="upper left")

    save_fig(fig, out_path)
    return (
        "support_tickets_churned_vs_retained.png",
        "Supports the service-friction hypothesis by comparing support burden between churned and retained groups.",
    )


def chart_revenue_at_risk_by_segment(segment_risk: pd.DataFrame, out_path: Path) -> tuple[str, str]:
    s = segment_risk.copy().sort_values("total_revenue_loss_proxy", ascending=False)

    fig, ax = plt.subplots(figsize=(10.5, 5.8))
    ax.bar(s["segment"], s["at_risk_mrr"], color=COLORS["accent"], label="Future revenue at risk (at-risk MRR)")
    ax.bar(s["segment"], s["churned_revenue"], bottom=s["at_risk_mrr"], color=COLORS["danger"], label="Realized churned monthly value")

    ax.set_title("Revenue Exposure Is Concentrated in Specific Segments")
    ax.set_ylabel("Revenue")
    ax.yaxis.set_major_formatter(FuncFormatter(currency_fmt))
    ax.legend(loc="upper right")

    save_fig(fig, out_path)
    return (
        "revenue_at_risk_by_segment.png",
        "Quantifies where intervention should be concentrated to protect the largest revenue pools.",
    )


def chart_top_risk_drivers_distribution(risk_scores: pd.DataFrame, out_path: Path) -> tuple[str, str]:
    d = (
        risk_scores["main_risk_driver"]
        .value_counts()
        .rename_axis("main_risk_driver")
        .reset_index(name="customers")
        .sort_values("customers", ascending=False)
    )

    fig, ax = plt.subplots(figsize=(10.5, 5.6))
    bars = ax.barh(d["main_risk_driver"], d["customers"], color=COLORS["primary"])
    ax.invert_yaxis()
    ax.set_title("Support Burden and Usage Decay Dominate Risk Signal Distribution")
    ax.set_xlabel("Customers")
    ax.xaxis.set_major_formatter(FuncFormatter(count_fmt))

    for bar, val in zip(bars, d["customers"]):
        ax.text(bar.get_width() + max(d["customers"]) * 0.01, bar.get_y() + bar.get_height() / 2, f"{int(val):,}", va="center", fontsize=9)

    save_fig(fig, out_path)
    return (
        "top_risk_drivers_distribution.png",
        "Shows which operational issues are most prevalent, informing playbook staffing and ownership.",
    )


def chart_priority_customers_ranking(risk_ranked: pd.DataFrame, out_path: Path) -> tuple[str, str]:
    top_n = risk_ranked.head(20).copy().sort_values("retention_priority_score", ascending=True)

    fig, ax = plt.subplots(figsize=(11.5, 7.2))
    colors = [RISK_TIER_COLORS.get(t, COLORS["neutral"]) for t in top_n["risk_tier"]]

    bars = ax.barh(top_n["customer_id"], top_n["retention_priority_score"], color=colors)
    ax.set_title("Top Priority Customers for Immediate Retention Intervention")
    ax.set_xlabel("Retention Priority Score")
    ax.set_ylabel("Customer ID")

    for bar, score, mrr in zip(bars, top_n["retention_priority_score"], top_n["current_mrr"]):
        ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2, f"{score:.1f} | {currency_fmt(mrr)}", va="center", fontsize=8.8)

    save_fig(fig, out_path)
    return (
        "priority_customers_ranking_chart.png",
        "Provides action-ready target list for Customer Success and retention operations.",
    )


def write_chart_index(charts_dir: Path, index_rows: list[dict]) -> None:
    index_df = pd.DataFrame(index_rows)
    index_df.insert(0, "chart_number", np.arange(1, len(index_df) + 1))
    index_df.to_csv(charts_dir / "chart_index.csv", index=False)

    lines = [
        "# Chart Pack Narrative Support",
        "",
        f"Generated at: `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`",
        "",
        "This file maps each chart to the business narrative it supports.",
        "",
    ]

    for row in index_df.itertuples(index=False):
        lines.extend(
            [
                f"## {row.chart_number}. {row.chart_title}",
                f"- File: `outputs/charts/{row.file_name}`",
                f"- Supports narrative: {row.business_narrative_support}",
                "",
            ]
        )

    (charts_dir / "chart_narrative_support.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    project_root = Path(__file__).resolve().parents[2]
    charts_dir = project_root / "outputs" / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)

    set_style()
    data = load_inputs(project_root)

    index_rows: list[dict] = []

    # 1.
    file_name, narrative = chart_customer_churn_trend(data["trend"], charts_dir / "customer_churn_trend_over_time.png")
    index_rows.append({"file_name": file_name, "chart_title": "Customer Churn Trend Over Time", "business_narrative_support": narrative})

    # 2.
    file_name, narrative = chart_revenue_churn_trend(data["trend"], charts_dir / "revenue_churn_trend_over_time.png")
    index_rows.append({"file_name": file_name, "chart_title": "Revenue Churn Trend Over Time", "business_narrative_support": narrative})

    # 3.
    file_name, narrative = chart_cohort_retention_curves(data["cohort"], charts_dir / "cohort_retention_curves.png")
    index_rows.append({"file_name": file_name, "chart_title": "Cohort Retention Curves", "business_narrative_support": narrative})

    # 4.
    file_name, narrative = chart_revenue_retention_by_cohort(data["cohort"], charts_dir / "revenue_retention_by_cohort.png")
    index_rows.append({"file_name": file_name, "chart_title": "Revenue Retention by Cohort", "business_narrative_support": narrative})

    # 5.
    file_name, narrative = _bar_churn_rate(
        data["churn_segment"], "segment", "Startup and SMB Segments Carry the Highest Churn Rates", charts_dir / "churn_rate_by_segment.png"
    )
    index_rows.append({"file_name": file_name, "chart_title": "Churn Rate by Segment", "business_narrative_support": narrative})

    # 6.
    file_name, narrative = _bar_churn_rate(
        data["churn_channel"], "acquisition_channel", "Acquisition Channel Mix Drives Material Retention Differences", charts_dir / "churn_rate_by_acquisition_channel.png"
    )
    index_rows.append({"file_name": file_name, "chart_title": "Churn Rate by Acquisition Channel", "business_narrative_support": narrative})

    # 7.
    file_name, narrative = _bar_churn_rate(
        data["churn_plan"], "plan_type", "Plan Tier Performance Indicates Product-Market Fit and Pricing Retention Effects", charts_dir / "churn_rate_by_plan_type.png"
    )
    index_rows.append({"file_name": file_name, "chart_title": "Churn Rate by Plan Type", "business_narrative_support": narrative})

    # 8.
    file_name, narrative = chart_usage_trend_churned_vs_retained(data["features"], charts_dir / "usage_trend_churned_vs_retained.png")
    index_rows.append({"file_name": file_name, "chart_title": "Usage Trend for Churned vs Retained Customers", "business_narrative_support": narrative})

    # 9.
    file_name, narrative = chart_support_tickets_churned_vs_retained(data["features"], charts_dir / "support_tickets_churned_vs_retained.png")
    index_rows.append({"file_name": file_name, "chart_title": "Support Tickets for Churned vs Retained Customers", "business_narrative_support": narrative})

    # 10.
    file_name, narrative = chart_revenue_at_risk_by_segment(data["segment_risk"], charts_dir / "revenue_at_risk_by_segment.png")
    index_rows.append({"file_name": file_name, "chart_title": "Revenue at Risk by Segment", "business_narrative_support": narrative})

    # 11.
    file_name, narrative = chart_top_risk_drivers_distribution(data["risk_scores"], charts_dir / "top_risk_drivers_distribution.png")
    index_rows.append({"file_name": file_name, "chart_title": "Top Risk Drivers Distribution", "business_narrative_support": narrative})

    # 12.
    file_name, narrative = chart_priority_customers_ranking(data["risk_ranked"], charts_dir / "priority_customers_ranking_chart.png")
    index_rows.append({"file_name": file_name, "chart_title": "Priority Customers Ranking Chart", "business_narrative_support": narrative})

    write_chart_index(charts_dir, index_rows)

    print("Chart pack generation completed.")
    print("Charts generated:", len(index_rows))
    print("Index files:", charts_dir / "chart_index.csv", "and", charts_dir / "chart_narrative_support.md")


if __name__ == "__main__":
    main()
