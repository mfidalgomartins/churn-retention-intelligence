"""
Curated analytics chart set for Churn & Retention Intelligence.

Generates the executive-ready PNG chart pack and writes it to
outputs/graphs/. Run directly:

    python -m churn.graphs
"""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # headless — no display required

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

# ── Paths ──────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "outputs" / "graphs"
TABLES = ROOT / "outputs" / "tables"
PROC = ROOT / "data" / "processed"
OUT.mkdir(parents=True, exist_ok=True)

# ── Design system ──────────────────────────────────────────
LOSS = "#B83530"
LOSS_LT = "#E8A09A"
GAIN = "#1B6640"
SLATE = "#1E293B"
ACCENT = "#1B3A6B"
MUTED = "#94A3B8"
NEUTRAL = "#475569"
FIG_BG = "#FAFAFA"
AX_BG = "#FFFFFF"

SEG_COLORS = {
    "Enterprise": "#1B3A6B",
    "Mid-Market": "#2E6B9E",
    "SMB": "#B83530",
    "Startup": "#C9860A",
}

TIER_COLORS = {
    "critical": "#B83530",
    "high": "#D4715A",
    "medium": "#C9860A",
    "low": "#94A3B8",
}

matplotlib.rcParams.update(
    {
        "font.family": "sans-serif",
        "font.sans-serif": ["Helvetica Neue", "Helvetica", "Arial", "DejaVu Sans"],
        "axes.facecolor": AX_BG,
        "figure.facecolor": FIG_BG,
        "axes.edgecolor": "#CBD5E1",
        "axes.linewidth": 0.7,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "axes.grid.axis": "y",
        "grid.color": "#E2E8F0",
        "grid.linewidth": 0.55,
        "xtick.color": NEUTRAL,
        "ytick.color": NEUTRAL,
        "xtick.labelsize": 9.5,
        "ytick.labelsize": 9.5,
        "axes.labelsize": 10.5,
        "axes.labelcolor": SLATE,
        "axes.titlesize": 12.5,
        "axes.titleweight": "bold",
        "axes.titlecolor": SLATE,
        "axes.titlepad": 13,
        "figure.titlesize": 13.5,
        "figure.titleweight": "bold",
        "legend.fontsize": 9.5,
        "legend.framealpha": 0.0,
        "lines.linewidth": 2.0,
        "savefig.dpi": 180,
        "savefig.bbox": "tight",
        "savefig.facecolor": FIG_BG,
        "figure.constrained_layout.use": False,
        "text.parse_math": False,  # keep literal "$" in labels — never treat $...$ as math
    }
)


# Acronyms that .str.title() would otherwise mangle (Smb → SMB, Nps → NPS, …)
def fix_acronyms(s: pd.Series) -> pd.Series:
    out = s
    for bad, good in {
        "Smb": "SMB",
        "Nps": "NPS",
        "Latam": "LATAM",
        "Apac": "APAC",
        "Mrr": "MRR",
    }.items():
        out = out.str.replace(bad, good, regex=False)
    return out


PCT = mticker.FuncFormatter(lambda x, _: f"{x:.0f}%")
USD_K = mticker.FuncFormatter(lambda x, _: f"${x:,.0f}k")


def save(fig: plt.Figure, name: str) -> None:
    path = OUT / name
    fig.savefig(path, dpi=180, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"  → {path.relative_to(ROOT)}")


def label_hbar(ax: plt.Axes, bars, fmt: str = "{:.1f}%", pad: float = 0.3) -> None:
    """Add value labels to horizontal bars."""
    for bar in bars:
        w = bar.get_width()
        ax.text(
            w + pad,
            bar.get_y() + bar.get_height() / 2,
            fmt.format(w),
            va="center",
            fontsize=9,
            color=SLATE,
        )


# ──────────────────────────────────────────────────────────
# 1. Monthly churn rate trend — customer + revenue
# ──────────────────────────────────────────────────────────
def chart_churn_rate_trend() -> None:
    df = pd.read_csv(TABLES / "overall_retention_trend_monthly.csv", parse_dates=["month"])
    df = df[df["active_customers_start"] >= 50].copy()
    df["cust_pct"] = df["customer_churn_rate"] * 100
    df["rev_pct"] = df["revenue_churn_rate"] * 100

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(df["month"], df["cust_pct"], color=LOSS, lw=2.2, label="Customer churn rate", zorder=3)
    ax.fill_between(df["month"], df["cust_pct"], alpha=0.09, color=LOSS)
    ax.plot(
        df["month"],
        df["rev_pct"],
        color=SLATE,
        lw=1.6,
        ls="--",
        label="Revenue churn rate",
        zorder=3,
    )
    ax.set_title("Monthly Churn Rate: Customer vs Revenue")
    ax.set_ylabel("Churn rate (%)")
    ax.yaxis.set_major_formatter(PCT)
    ax.legend(loc="upper right")
    ax.margins(x=0.01)
    fig.tight_layout()
    save(fig, "churn_rate_trend.png")


# ──────────────────────────────────────────────────────────
# 2. Active MRR by month
# ──────────────────────────────────────────────────────────
def chart_mrr_trend() -> None:
    df = pd.read_csv(TABLES / "overall_retention_trend_monthly.csv", parse_dates=["month"])
    df = df[df["active_customers_start"] >= 50].copy()
    df["mrr_k"] = df["active_mrr_start"] / 1_000

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.bar(
        df["month"],
        df["mrr_k"],
        color=ACCENT,
        alpha=0.82,
        width=25,
        edgecolor="white",
        linewidth=0.3,
    )
    ax.set_title("Active MRR by Month")
    ax.set_ylabel("MRR ($000s)")
    ax.yaxis.set_major_formatter(USD_K)
    ax.margins(x=0.02)
    fig.tight_layout()
    save(fig, "mrr_trend.png")


# ──────────────────────────────────────────────────────────
# 3. Churn rate by segment
# ──────────────────────────────────────────────────────────
def chart_churn_by_segment() -> None:
    df = pd.read_csv(TABLES / "churn_by_segment.csv")
    df = df.sort_values("cumulative_churn_share", ascending=True)
    df["pct"] = df["cumulative_churn_share"] * 100
    colors = [SEG_COLORS.get(s, MUTED) for s in df["segment"]]

    fig, ax = plt.subplots(figsize=(9, 4))
    bars = ax.barh(
        df["segment"], df["pct"], color=colors, edgecolor="white", linewidth=0.3, height=0.5
    )
    label_hbar(ax, bars, "{:.1f}%", pad=0.5)
    ax.set_title("Historical Logo Churn Rate by Customer Segment")
    ax.set_xlabel("Historical logo churn rate (%)")
    ax.xaxis.set_major_formatter(PCT)
    ax.set_xlim(0, df["pct"].max() * 1.18)
    ax.grid(axis="x")
    ax.grid(axis="y", visible=False)
    fig.tight_layout()
    save(fig, "churn_by_segment.png")


# ──────────────────────────────────────────────────────────
# 4. Churn rate by acquisition channel
# ──────────────────────────────────────────────────────────
def chart_churn_by_channel() -> None:
    df = pd.read_csv(TABLES / "churn_by_acquisition_channel.csv")
    df = df.sort_values("cumulative_churn_share", ascending=True)
    df["pct"] = df["cumulative_churn_share"] * 100
    n = len(df)
    colors = [GAIN if i < n // 3 else (MUTED if i < 2 * n // 3 else LOSS) for i in range(n)]

    fig, ax = plt.subplots(figsize=(9, 4.5))
    bars = ax.barh(
        df["acquisition_channel"],
        df["pct"],
        color=colors,
        edgecolor="white",
        linewidth=0.3,
        height=0.5,
    )
    label_hbar(ax, bars, "{:.1f}%", pad=0.5)
    ax.set_title("Historical Logo Churn Rate by Acquisition Channel")
    ax.set_xlabel("Historical logo churn rate (%)")
    ax.xaxis.set_major_formatter(PCT)
    ax.set_xlim(0, df["pct"].max() * 1.18)
    ax.grid(axis="x")
    ax.grid(axis="y", visible=False)
    fig.tight_layout()
    save(fig, "churn_by_channel.png")


# ──────────────────────────────────────────────────────────
# 5. Churn rate by plan type
# ──────────────────────────────────────────────────────────
def chart_churn_by_plan() -> None:
    df = pd.read_csv(TABLES / "churn_by_plan_type.csv")
    df = df.sort_values("cumulative_churn_share", ascending=True)
    df["pct"] = df["cumulative_churn_share"] * 100
    n = len(df)
    colors = [GAIN if i < n // 2 else LOSS for i in range(n)]

    fig, ax = plt.subplots(figsize=(9, 4))
    bars = ax.barh(
        df["plan_type"], df["pct"], color=colors, edgecolor="white", linewidth=0.3, height=0.5
    )
    label_hbar(ax, bars, "{:.1f}%", pad=0.5)
    ax.set_title("Cumulative Churn Share by Plan Type")
    ax.set_xlabel("Cumulative churn share (%)")
    ax.xaxis.set_major_formatter(PCT)
    ax.set_xlim(0, df["pct"].max() * 1.18)
    ax.grid(axis="x")
    ax.grid(axis="y", visible=False)
    fig.tight_layout()
    save(fig, "churn_by_plan.png")


# ──────────────────────────────────────────────────────────
# 6. Risk tier breakdown — customers and MRR side by side
# ──────────────────────────────────────────────────────────
def chart_risk_tier_breakdown() -> None:
    df = pd.read_csv(TABLES / "risk_tier_summary.csv")
    order = ["critical", "high", "medium", "low"]
    df = df.set_index("risk_tier").loc[order].reset_index()
    df["mrr_k"] = df["total_current_mrr"] / 1_000
    colors = [TIER_COLORS[t] for t in df["risk_tier"]]
    tiers_title = df["risk_tier"].str.title()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.5))

    b1 = ax1.barh(
        tiers_title, df["customers"], color=colors, edgecolor="white", linewidth=0.3, height=0.5
    )
    for bar, val in zip(b1, df["customers"], strict=False):
        ax1.text(
            bar.get_width() + 5,
            bar.get_y() + bar.get_height() / 2,
            f"{val:,}",
            va="center",
            fontsize=9,
            color=SLATE,
        )
    ax1.set_title("Customers by Risk Tier")
    ax1.set_xlabel("Customers")
    ax1.grid(axis="x")
    ax1.grid(axis="y", visible=False)
    ax1.set_xlim(0, df["customers"].max() * 1.15)

    b2 = ax2.barh(
        tiers_title, df["mrr_k"], color=colors, edgecolor="white", linewidth=0.3, height=0.5
    )
    for bar, val in zip(b2, df["mrr_k"], strict=False):
        ax2.text(
            bar.get_width() + 1,
            bar.get_y() + bar.get_height() / 2,
            f"${val:,.0f}k",
            va="center",
            fontsize=9,
            color=SLATE,
        )
    ax2.set_title("MRR Exposure by Risk Tier")
    ax2.set_xlabel("Current MRR ($000s)")
    ax2.xaxis.set_major_formatter(USD_K)
    ax2.grid(axis="x")
    ax2.grid(axis="y", visible=False)
    ax2.set_xlim(0, df["mrr_k"].max() * 1.22)
    ax2.yaxis.set_visible(False)
    ax2.spines["left"].set_visible(False)

    fig.suptitle("Risk Tier Profile: Customer Count and MRR Exposure", y=1.01)
    fig.tight_layout()
    save(fig, "risk_tier_breakdown.png")


# ──────────────────────────────────────────────────────────
# 7. Revenue at risk and churned revenue by segment
# ──────────────────────────────────────────────────────────
def chart_revenue_at_risk_by_segment() -> None:
    df = pd.read_csv(TABLES / "segment_revenue_risk_contribution.csv")
    df = df.sort_values("current_mrr_at_risk", ascending=False)
    df["at_risk_k"] = df["current_mrr_at_risk"] / 1_000
    df["churned_k"] = df["churned_monthly_value_proxy"] / 1_000
    x = np.arange(len(df))
    w = 0.38

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(
        x - w / 2,
        df["at_risk_k"],
        width=w,
        color=LOSS,
        alpha=0.85,
        label="MRR at risk (current)",
        edgecolor="white",
        linewidth=0.3,
    )
    ax.bar(
        x + w / 2,
        df["churned_k"],
        width=w,
        color=ACCENT,
        alpha=0.85,
        label="Churned monthly-value proxy",
        edgecolor="white",
        linewidth=0.3,
    )
    ax.set_title("Revenue Exposure by Segment: Current At-Risk MRR vs Churned Monthly-Value Proxy")
    ax.set_ylabel("Revenue ($000s)")
    ax.set_xticks(x)
    ax.set_xticklabels(df["segment"])
    ax.yaxis.set_major_formatter(USD_K)
    ax.legend()
    ax.grid(axis="y")
    ax.grid(axis="x", visible=False)
    fig.tight_layout()
    save(fig, "revenue_at_risk_by_segment.png")


# ──────────────────────────────────────────────────────────
# 8. Churn rate lift by behavioral signal
# ──────────────────────────────────────────────────────────
def chart_behavioral_drivers() -> None:
    df = pd.read_csv(TABLES / "behavioral_churn_relationships.csv")
    df["label"] = (
        df["relationship"]
        .str.replace("_flag", "", regex=False)
        .str.replace("_", " ", regex=False)
        .str.title()
    )
    df["label"] = fix_acronyms(df["label"])
    df = df.sort_values("churn_rate_lift", ascending=True)
    colors = [LOSS if v >= 5.0 else MUTED for v in df["churn_rate_lift"]]

    fig, ax = plt.subplots(figsize=(10, 4.5))
    bars = ax.barh(
        df["label"],
        df["churn_rate_lift"],
        color=colors,
        edgecolor="white",
        linewidth=0.3,
        height=0.5,
    )
    for bar, val in zip(bars, df["churn_rate_lift"], strict=False):
        ax.text(
            bar.get_width() + 0.2,
            bar.get_y() + bar.get_height() / 2,
            f"{val:.1f}×",
            va="center",
            fontsize=9,
            color=SLATE,
        )
    ax.set_title(
        "Churn Rate Lift by Behavioral Signal\n"
        "(lift = churn rate inside signal ÷ churn rate outside signal)"
    )
    ax.set_xlabel("Churn rate lift (×)")
    ax.set_xlim(0, df["churn_rate_lift"].max() * 1.18)
    ax.grid(axis="x")
    ax.grid(axis="y", visible=False)
    # Reference line at 1× (no lift)
    ax.axvline(1.0, color=MUTED, lw=0.9, ls=":", zorder=0)
    ax.text(
        1.05,
        0.02,
        "No lift",
        transform=ax.get_xaxis_transform(),
        fontsize=8,
        color=MUTED,
        va="bottom",
    )
    fig.tight_layout()
    save(fig, "behavioral_churn_drivers.png")


# ──────────────────────────────────────────────────────────
# 9. Churn risk score distribution by tier
# ──────────────────────────────────────────────────────────
def chart_risk_score_distribution() -> None:
    df = pd.read_csv(PROC / "customer_risk_scores.csv")

    fig, ax = plt.subplots(figsize=(10, 5))
    for tier in ["low", "medium", "high", "critical"]:
        subset = df[df["risk_tier"] == tier]["churn_risk_score"]
        ax.hist(
            subset,
            bins=28,
            alpha=0.72,
            color=TIER_COLORS[tier],
            label=f"{tier.title()}  ({len(subset):,})",
            edgecolor="white",
            linewidth=0.3,
        )
    ax.set_title("Churn Risk Score Distribution by Tier")
    ax.set_xlabel("Churn risk score")
    ax.set_ylabel("Number of customers")
    ax.legend(loc="upper right")
    ax.grid(axis="y")
    ax.grid(axis="x", visible=False)
    fig.tight_layout()
    save(fig, "risk_score_distribution.png")


# ──────────────────────────────────────────────────────────
# 10. Cohort retention heatmap (last 24 cohorts × 12 months)
# ──────────────────────────────────────────────────────────
def chart_cohort_heatmap() -> None:
    df = pd.read_csv(
        PROC / "cohort_retention_table.csv", parse_dates=["cohort_month", "observation_month"]
    )
    df["age"] = (df["observation_month"].dt.year - df["cohort_month"].dt.year) * 12 + (
        df["observation_month"].dt.month - df["cohort_month"].dt.month
    )
    pivot = df.pivot_table(
        index="cohort_month", columns="age", values="retention_rate", aggfunc="mean"
    )
    pivot = pivot.loc[:, pivot.columns <= 12]

    # Keep the most recent 24 cohorts
    pivot = pivot.iloc[-24:]
    pivot.index = pd.DatetimeIndex(pivot.index).strftime("%Y-%m")

    nrows = len(pivot)
    fig, ax = plt.subplots(figsize=(14, max(5.5, nrows * 0.32 + 2.5)))
    im = ax.imshow(
        pivot.values, aspect="auto", cmap="RdYlGn", vmin=0, vmax=1, interpolation="nearest"
    )

    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels([f"M{c}" for c in pivot.columns], fontsize=9)
    ax.set_yticks(range(nrows))
    ax.set_yticklabels(pivot.index, fontsize=9)
    ax.set_xlabel("Months since acquisition")
    ax.set_ylabel("Cohort (acquisition month)")
    ax.set_title("Cohort Retention Rate (% of original cohort remaining, by month)")
    ax.spines[:].set_visible(False)
    ax.tick_params(length=0)

    for i in range(nrows):
        for j in range(len(pivot.columns)):
            val = pivot.values[i, j]
            if np.isnan(val):
                continue
            text_color = "black" if 0.25 < val < 0.82 else "white"
            ax.text(
                j,
                i,
                f"{val:.0%}",
                ha="center",
                va="center",
                fontsize=8,
                color=text_color,
                fontweight="normal",
            )

    cbar = fig.colorbar(im, ax=ax, fraction=0.018, pad=0.02)
    cbar.set_label("Retention rate")
    cbar.ax.yaxis.set_major_formatter(mticker.PercentFormatter(1.0, decimals=0))
    cbar.outline.set_visible(False)  # type: ignore[operator]  # Colorbar.outline under-typed in stubs

    fig.tight_layout()
    save(fig, "cohort_retention_heatmap.png")


# ──────────────────────────────────────────────────────────
# 11. Cohort retention curves (last 8 cohorts)
# ──────────────────────────────────────────────────────────
def chart_cohort_curves() -> None:
    df = pd.read_csv(
        PROC / "cohort_retention_table.csv", parse_dates=["cohort_month", "observation_month"]
    )
    df["age"] = (df["observation_month"].dt.year - df["cohort_month"].dt.year) * 12 + (
        df["observation_month"].dt.month - df["cohort_month"].dt.month
    )
    recent = sorted(df["cohort_month"].unique())[-8:]
    n = len(recent)

    # Blues gradient: older cohorts muted, most recent prominent
    blues = plt.cm.Blues(np.linspace(0.35, 0.90, n))  # type: ignore[attr-defined]  # colormap names not enumerated in stubs

    fig, ax = plt.subplots(figsize=(11, 5.5))
    for i, cm in enumerate(recent):
        sub = df[df["cohort_month"] == cm].sort_values("age")
        lw = 2.4 if i == n - 1 else (1.8 if i == n - 2 else 1.1)
        lbl = pd.Timestamp(cm).strftime("%Y-%m")
        ax.plot(
            sub["age"], sub["retention_rate"] * 100, color=blues[i], lw=lw, label=lbl, zorder=i + 1
        )

    ax.set_title("Customer Retention by Cohort Age (8 most recent cohorts)")
    ax.set_xlabel("Months since acquisition")
    ax.set_ylabel("Retention rate (%)")
    ax.yaxis.set_major_formatter(PCT)
    ax.set_ylim(0, 108)
    ax.legend(title="Cohort", loc="lower left", fontsize=9, ncol=2, title_fontsize=9)
    ax.grid(axis="y")
    ax.grid(axis="x", visible=False)
    fig.tight_layout()
    save(fig, "cohort_retention_curves.png")


# ──────────────────────────────────────────────────────────
# 12. Segment health comparison — churn, NPS, usage trend
# ──────────────────────────────────────────────────────────
def chart_segment_health() -> None:
    df = pd.read_csv(PROC / "segment_retention_summary.csv")
    df["churn_pct"] = df["cumulative_churn_share"] * 100
    seg_order = ["Enterprise", "Mid-Market", "SMB", "Startup"]
    df = (
        df.set_index("segment")
        .loc[[s for s in seg_order if s in df["segment"].values]]
        .reset_index()
    )
    colors = [SEG_COLORS.get(s, MUTED) for s in df["segment"]]
    x = np.arange(len(df))
    bar_w = 0.55

    fig, axes = plt.subplots(1, 3, figsize=(13, 4.5))
    fig.suptitle("Segment Health: Cumulative Churn, NPS, and Usage Trend")

    # Panel 1: Churn rate
    ax = axes[0]
    ax.bar(x, df["churn_pct"], color=colors, width=bar_w, edgecolor="white", linewidth=0.3)
    ax.set_title("Cumulative Churn Share (%)")
    ax.set_xticks(x)
    ax.set_xticklabels(df["segment"], rotation=15, ha="right", fontsize=9)
    ax.yaxis.set_major_formatter(PCT)
    ax.grid(axis="y")
    ax.grid(axis="x", visible=False)
    for xi, val in zip(x, df["churn_pct"], strict=False):
        ax.text(xi, val + 0.4, f"{val:.1f}%", ha="center", fontsize=8.5, color=SLATE)

    # Panel 2: NPS score
    ax = axes[1]
    ax.bar(x, df["avg_nps"], color=colors, width=bar_w, edgecolor="white", linewidth=0.3)
    ax.set_title("Average NPS Score")
    ax.set_xticks(x)
    ax.set_xticklabels(df["segment"], rotation=15, ha="right", fontsize=9)
    ax.grid(axis="y")
    ax.grid(axis="x", visible=False)
    for xi, val in zip(x, df["avg_nps"], strict=False):
        ax.text(xi, val + 0.3, f"{val:.1f}", ha="center", fontsize=8.5, color=SLATE)

    # Panel 3: Usage trend
    ax = axes[2]
    ut = df["avg_usage_trend"]
    bar_colors = [GAIN if v > 0 else LOSS for v in ut]
    ax.bar(x, ut, color=bar_colors, width=bar_w, edgecolor="white", linewidth=0.3)
    ax.axhline(0, color=SLATE, lw=0.7)
    ax.set_title("Avg Usage Trend (sessions Δ30d)")
    ax.set_xticks(x)
    ax.set_xticklabels(df["segment"], rotation=15, ha="right", fontsize=9)
    ax.grid(axis="y")
    ax.grid(axis="x", visible=False)
    for xi, val in zip(x, ut, strict=False):
        offset = 0.03 if val >= 0 else -0.1
        ax.text(xi, val + offset, f"{val:+.2f}", ha="center", fontsize=8.5, color=SLATE)

    fig.tight_layout()
    save(fig, "segment_health_comparison.png")


# ──────────────────────────────────────────────────────────
# 13. Churn by region (geography)
# ──────────────────────────────────────────────────────────
def chart_churn_by_region() -> None:
    df = pd.read_csv(TABLES / "churn_by_region.csv")
    df = df.sort_values("cumulative_churn_share", ascending=True)
    df["pct"] = df["cumulative_churn_share"] * 100
    base = 0.272286 * 100  # baseline churn share
    colors = [LOSS if v >= base else MUTED for v in df["pct"]]

    fig, ax = plt.subplots(figsize=(9, 4))
    bars = ax.barh(
        df["region"], df["pct"], color=colors, edgecolor="white", linewidth=0.3, height=0.5
    )
    label_hbar(ax, bars, "{:.1f}%", pad=0.5)
    ax.axvline(base, color=SLATE, lw=0.9, ls=":", zorder=0)
    ax.text(
        base + 0.3,
        0.02,
        f"Book average {base:.1f}%",
        transform=ax.get_xaxis_transform(),
        fontsize=8,
        color=SLATE,
        va="bottom",
    )
    ax.set_title("Cumulative Churn Share by Region")
    ax.set_xlabel("Cumulative churn share (%)")
    ax.xaxis.set_major_formatter(PCT)
    ax.set_xlim(0, df["pct"].max() * 1.22)
    ax.grid(axis="x")
    ax.grid(axis="y", visible=False)
    fig.tight_layout()
    save(fig, "churn_by_region.png")


# ──────────────────────────────────────────────────────────
# 14. Churn driver ranking — excess MRR association by driver
# ──────────────────────────────────────────────────────────
def chart_driver_ranking() -> None:
    df = pd.read_csv(TABLES / "main_analysis_churn_driver_ranking.csv")
    df = df.sort_values("excess_mrr_association_proxy", ascending=True).tail(11)
    df["mrr_k"] = df["excess_mrr_association_proxy"] / 1_000
    labels = (
        df["driver"]
        .str.replace("_flag", "", regex=False)
        .str.replace("_", " ", regex=False)
        .str.replace("acquisition channel=", "channel: ", regex=False)
        .str.replace("plan type=", "plan: ", regex=False)
        .str.replace("segment=", "segment: ", regex=False)
        .str.replace("region=", "region: ", regex=False)
        .str.title()
    )
    labels = fix_acronyms(labels)
    # Behavioural flags get the accent; structural attributes stay neutral
    behavioural = df["driver"].str.endswith("_flag")
    colors = [LOSS if b else NEUTRAL for b in behavioural]

    fig, ax = plt.subplots(figsize=(11, 6))
    bars = ax.barh(labels, df["mrr_k"], color=colors, edgecolor="white", linewidth=0.3, height=0.62)
    for bar, lift in zip(bars, df["churn_rate_lift"], strict=False):
        ax.text(
            bar.get_width() + df["mrr_k"].max() * 0.012,
            bar.get_y() + bar.get_height() / 2,
            f"${bar.get_width():,.0f}k   ({lift:.1f}× churn)",
            va="center",
            fontsize=8.5,
            color=SLATE,
        )
    ax.set_title(
        "Churn Driver Ranking by Excess MRR Association\n"
        "(monthly revenue tied to churned accounts carrying each signal, above baseline)"
    )
    ax.set_xlabel("Excess MRR association ($000s/month, proxy)")
    ax.xaxis.set_major_formatter(USD_K)
    ax.set_xlim(0, df["mrr_k"].max() * 1.32)
    ax.grid(axis="x")
    ax.grid(axis="y", visible=False)
    # Legend for the colour encoding
    from matplotlib.patches import Patch

    ax.legend(
        handles=[
            Patch(facecolor=LOSS, label="Behavioural signal"),
            Patch(facecolor=NEUTRAL, label="Structural attribute"),
        ],
        loc="lower right",
        frameon=False,
    )
    fig.tight_layout()
    save(fig, "churn_driver_ranking.png")


# ──────────────────────────────────────────────────────────
# 15. Revenue concentration of churn (Lorenz / Pareto)
# ──────────────────────────────────────────────────────────
def chart_revenue_concentration() -> None:
    df = pd.read_csv(PROC / "customer_retention_features.csv")
    churned = df[df["churn_flag"] == 1]["avg_monthly_revenue"].sort_values(ascending=False).values
    if churned.sum() == 0:
        return
    cum_rev = np.cumsum(churned) / churned.sum() * 100
    cum_cust = np.arange(1, len(churned) + 1) / len(churned) * 100

    fig, ax = plt.subplots(figsize=(9.5, 5.5))
    ax.plot(cum_cust, cum_rev, color=LOSS, lw=2.4, zorder=3)
    ax.fill_between(cum_cust, cum_rev, alpha=0.08, color=LOSS)
    # Line of perfect equality
    ax.plot([0, 100], [0, 100], color=MUTED, lw=1.0, ls="--", zorder=2)
    ax.text(72, 66, "Even distribution", color=MUTED, fontsize=8.5, rotation=33)

    # Annotate the 20% reference point
    idx20 = int(len(churned) * 0.20) - 1
    y20 = cum_rev[idx20]
    ax.scatter([20], [y20], color=SLATE, s=28, zorder=4)
    ax.plot([20, 20], [0, y20], color=SLATE, lw=0.8, ls=":", zorder=1)
    ax.plot([0, 20], [y20, y20], color=SLATE, lw=0.8, ls=":", zorder=1)
    ax.text(
        22,
        y20 - 7,
        f"Top 20% of churned accounts\ncarry {y20:.0f}% of lost monthly value",
        fontsize=9,
        color=SLATE,
    )

    ax.set_title("Concentration of Lost Monthly Value Across Churned Accounts")
    ax.set_xlabel("Cumulative share of churned accounts (%, ranked by value)")
    ax.set_ylabel("Cumulative share of lost monthly value (%)")
    ax.xaxis.set_major_formatter(PCT)
    ax.yaxis.set_major_formatter(PCT)
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.grid(True)
    fig.tight_layout()
    save(fig, "revenue_concentration.png")


# ──────────────────────────────────────────────────────────
# 16. Behavioural signal separation — in-group vs out-group churn
# ──────────────────────────────────────────────────────────
def chart_signal_separation() -> None:
    df = pd.read_csv(TABLES / "behavioral_churn_relationships.csv")
    df["label"] = (
        df["relationship"]
        .str.replace("_flag", "", regex=False)
        .str.replace("_", " ", regex=False)
        .str.title()
    )
    df["label"] = fix_acronyms(df["label"])
    df = df.sort_values("churn_rate_in_group", ascending=True)
    y = np.arange(len(df), dtype=float)
    inside = df["churn_rate_in_group"] * 100
    outside = df["churn_rate_out_group"] * 100

    fig, ax = plt.subplots(figsize=(10.5, 5))
    # Connector
    for yi, lo, hi in zip(y, outside, inside, strict=False):
        ax.plot([lo, hi], [yi, yi], color="#CBD5E1", lw=2.2, zorder=1)
    ax.scatter(outside, y, color=MUTED, s=70, zorder=2, label="Without signal")
    ax.scatter(inside, y, color=LOSS, s=80, zorder=3, label="With signal")
    for yi, hi in zip(y, inside, strict=False):
        ax.text(hi + 1.5, yi, f"{hi:.0f}%", va="center", fontsize=9, color=SLATE)
    for yi, lo in zip(y, outside, strict=False):
        ax.text(lo - 1.5, yi, f"{lo:.0f}%", va="center", ha="right", fontsize=8.5, color=NEUTRAL)

    ax.set_yticks(y)
    ax.set_yticklabels(df["label"])
    ax.set_title("Churn Rate With and Without Each Pre-Churn Signal")
    ax.set_xlabel("Churn rate within group (%)")
    ax.xaxis.set_major_formatter(PCT)
    ax.set_xlim(-8, 112)
    ax.legend(loc="lower right", frameon=False)
    ax.grid(axis="x")
    ax.grid(axis="y", visible=False)
    fig.tight_layout()
    save(fig, "signal_separation.png")


# ──────────────────────────────────────────────────────────
# 17. Intervention priorities — weighted MRR exposure by play
# ──────────────────────────────────────────────────────────
def chart_intervention_priorities() -> None:
    df = pd.read_csv(TABLES / "main_analysis_intervention_priorities.csv")
    df = df.sort_values("mrr_exposure_proxy", ascending=True)
    df["exp_k"] = df["mrr_exposure_proxy"] / 1_000
    df["scope_k"] = df["current_mrr_scope"] / 1_000

    fig, ax = plt.subplots(figsize=(10.5, 5))
    # Scope as light backdrop bar, weighted exposure as accent foreground
    ax.barh(df["opportunity"], df["scope_k"], color="#E2E8F0", height=0.55, label="MRR in scope")
    bars = ax.barh(
        df["opportunity"],
        df["exp_k"],
        color=LOSS,
        height=0.55,
        label="Weighted MRR exposure (proxy)",
    )
    for bar, cand, scope in zip(bars, df["candidate_customers"], df["scope_k"], strict=False):
        ax.text(
            scope + df["scope_k"].max() * 0.012,
            bar.get_y() + bar.get_height() / 2,
            f"${bar.get_width():,.0f}k weighted  ·  {cand:,} accounts  ·  ${scope:,.0f}k scope",
            va="center",
            fontsize=8.3,
            color=SLATE,
        )
    ax.set_title("Retention Plays Ranked by Weighted MRR Exposure")
    ax.set_xlabel("Monthly revenue ($000s)")
    ax.xaxis.set_major_formatter(USD_K)
    ax.set_xlim(0, df["scope_k"].max() * 1.5)
    ax.grid(axis="x")
    ax.grid(axis="y", visible=False)
    ax.legend(loc="lower right", frameon=False)
    fig.tight_layout()
    save(fig, "intervention_priorities.png")


# ──────────────────────────────────────────────────────────
# 18. Recent churn acceleration — last 12 months (before / after)
# ──────────────────────────────────────────────────────────
def chart_recent_acceleration() -> None:
    df = pd.read_csv(TABLES / "overall_retention_trend_monthly.csv", parse_dates=["month"])
    df = df[df["active_customers_start"] >= 50].tail(12).copy()
    df["pct"] = df["customer_churn_rate"] * 100
    df["mlabel"] = df["month"].dt.strftime("%b '%y")
    # Highlight the most recent three months where the rate breaks out
    thresh = df["pct"].iloc[:-3].mean() + df["pct"].iloc[:-3].std()
    colors = [LOSS if v > thresh else MUTED for v in df["pct"]]

    fig, ax = plt.subplots(figsize=(11, 5))
    bars = ax.bar(
        df["mlabel"], df["pct"], color=colors, edgecolor="white", linewidth=0.3, width=0.7
    )
    for bar, val in zip(bars, df["pct"], strict=False):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            val + 0.15,
            f"{val:.1f}%",
            ha="center",
            fontsize=8.5,
            color=SLATE,
        )
    base = df["pct"].iloc[:-3].mean()
    ax.axhline(base, color=SLATE, lw=0.9, ls=":", zorder=0)
    ax.text(0.2, base + 0.15, f"Prior 9-month average {base:.1f}%", fontsize=8.5, color=SLATE)
    ax.set_title("Monthly Customer Churn Rate — Last 12 Months")
    ax.set_ylabel("Customer churn rate (%)")
    ax.yaxis.set_major_formatter(PCT)
    ax.set_ylim(0, df["pct"].max() * 1.2)
    ax.grid(axis="y")
    ax.grid(axis="x", visible=False)
    fig.tight_layout()
    save(fig, "recent_churn_acceleration.png")


# ── Entry point ────────────────────────────────────────────
def main() -> None:
    print(f"Generating charts → {OUT.relative_to(ROOT)}/")
    chart_churn_rate_trend()
    chart_mrr_trend()
    chart_recent_acceleration()
    chart_churn_by_segment()
    chart_churn_by_channel()
    chart_churn_by_plan()
    chart_churn_by_region()
    chart_segment_health()
    chart_driver_ranking()
    chart_behavioral_drivers()
    chart_signal_separation()
    chart_risk_tier_breakdown()
    chart_risk_score_distribution()
    chart_revenue_at_risk_by_segment()
    chart_revenue_concentration()
    chart_intervention_priorities()
    chart_cohort_curves()
    chart_cohort_heatmap()
    print(f"\n{len(list(OUT.glob('*.png')))} charts written to {OUT.relative_to(ROOT)}/")


if __name__ == "__main__":
    main()
