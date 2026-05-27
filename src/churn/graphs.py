"""
Curated analytics chart set for Churn & Retention Intelligence.

Generates 12 executive-ready PNG charts and writes them to
outputs/Graphs/. Run directly:

    python -m churn.graphs
"""
from __future__ import annotations

import warnings
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # headless — no display required

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

# ── Paths ──────────────────────────────────────────────────
ROOT    = Path(__file__).resolve().parents[2]
OUT     = ROOT / "outputs" / "Graphs"
TABLES  = ROOT / "outputs" / "tables"
PROC    = ROOT / "data" / "processed"
OUT.mkdir(parents=True, exist_ok=True)

# ── Design system ──────────────────────────────────────────
LOSS    = "#B83530"
LOSS_LT = "#E8A09A"
GAIN    = "#1B6640"
SLATE   = "#1E293B"
ACCENT  = "#1B3A6B"
MUTED   = "#94A3B8"
NEUTRAL = "#475569"
FIG_BG  = "#FAFAFA"
AX_BG   = "#FFFFFF"

SEG_COLORS = {
    "Enterprise": "#1B3A6B",
    "Mid-Market": "#2E6B9E",
    "SMB":        "#B83530",
    "Startup":    "#C9860A",
}

TIER_COLORS = {
    "critical": "#B83530",
    "high":     "#D4715A",
    "medium":   "#C9860A",
    "low":      "#94A3B8",
}

matplotlib.rcParams.update({
    "font.family":        "sans-serif",
    "font.sans-serif":    ["Helvetica Neue", "Helvetica", "Arial", "DejaVu Sans"],
    "axes.facecolor":     AX_BG,
    "figure.facecolor":   FIG_BG,
    "axes.edgecolor":     "#CBD5E1",
    "axes.linewidth":     0.7,
    "axes.spines.top":    False,
    "axes.spines.right":  False,
    "axes.grid":          True,
    "axes.grid.axis":     "y",
    "grid.color":         "#E2E8F0",
    "grid.linewidth":     0.55,
    "xtick.color":        NEUTRAL,
    "ytick.color":        NEUTRAL,
    "xtick.labelsize":    9.5,
    "ytick.labelsize":    9.5,
    "axes.labelsize":     10.5,
    "axes.labelcolor":    SLATE,
    "axes.titlesize":     12.5,
    "axes.titleweight":   "bold",
    "axes.titlecolor":    SLATE,
    "axes.titlepad":      13,
    "figure.titlesize":   13.5,
    "figure.titleweight": "bold",
    "legend.fontsize":    9.5,
    "legend.framealpha":  0.0,
    "lines.linewidth":    2.0,
    "savefig.dpi":        180,
    "savefig.bbox":       "tight",
    "savefig.facecolor":  FIG_BG,
    "figure.constrained_layout.use": False,
})

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
            w + pad, bar.get_y() + bar.get_height() / 2,
            fmt.format(w), va="center", fontsize=9, color=SLATE,
        )


# ──────────────────────────────────────────────────────────
# 1. Monthly churn rate trend — customer + revenue
# ──────────────────────────────────────────────────────────
def chart_churn_rate_trend() -> None:
    df = pd.read_csv(TABLES / "overall_retention_trend_monthly.csv", parse_dates=["month"])
    df = df[df["active_customers_start"] >= 50].copy()
    df["cust_pct"] = df["customer_churn_rate"] * 100
    df["rev_pct"]  = df["revenue_churn_rate"]  * 100

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(df["month"], df["cust_pct"], color=LOSS, lw=2.2, label="Customer churn rate", zorder=3)
    ax.fill_between(df["month"], df["cust_pct"], alpha=0.09, color=LOSS)
    ax.plot(df["month"], df["rev_pct"],  color=SLATE, lw=1.6, ls="--", label="Revenue churn rate", zorder=3)
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
    ax.bar(df["month"], df["mrr_k"], color=ACCENT, alpha=0.82,
           width=25, edgecolor="white", linewidth=0.3)
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
    df = df.sort_values("churn_rate", ascending=True)
    df["pct"] = df["churn_rate"] * 100
    colors = [SEG_COLORS.get(s, MUTED) for s in df["segment"]]

    fig, ax = plt.subplots(figsize=(9, 4))
    bars = ax.barh(df["segment"], df["pct"], color=colors,
                   edgecolor="white", linewidth=0.3, height=0.5)
    label_hbar(ax, bars, "{:.1f}%", pad=0.5)
    ax.set_title("Churn Rate by Customer Segment")
    ax.set_xlabel("Churn rate (%)")
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
    df = df.sort_values("churn_rate", ascending=True)
    df["pct"] = df["churn_rate"] * 100
    n = len(df)
    colors = [GAIN if i < n // 3 else (MUTED if i < 2 * n // 3 else LOSS) for i in range(n)]

    fig, ax = plt.subplots(figsize=(9, 4.5))
    bars = ax.barh(df["acquisition_channel"], df["pct"], color=colors,
                   edgecolor="white", linewidth=0.3, height=0.5)
    label_hbar(ax, bars, "{:.1f}%", pad=0.5)
    ax.set_title("Churn Rate by Acquisition Channel")
    ax.set_xlabel("Churn rate (%)")
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
    df = df.sort_values("churn_rate", ascending=True)
    df["pct"] = df["churn_rate"] * 100
    n = len(df)
    colors = [GAIN if i < n // 2 else LOSS for i in range(n)]

    fig, ax = plt.subplots(figsize=(9, 4))
    bars = ax.barh(df["plan_type"], df["pct"], color=colors,
                   edgecolor="white", linewidth=0.3, height=0.5)
    label_hbar(ax, bars, "{:.1f}%", pad=0.5)
    ax.set_title("Churn Rate by Plan Type")
    ax.set_xlabel("Churn rate (%)")
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

    b1 = ax1.barh(tiers_title, df["customers"], color=colors,
                  edgecolor="white", linewidth=0.3, height=0.5)
    for bar, val in zip(b1, df["customers"], strict=False):
        ax1.text(bar.get_width() + 5, bar.get_y() + bar.get_height() / 2,
                 f"{val:,}", va="center", fontsize=9, color=SLATE)
    ax1.set_title("Customers by Risk Tier")
    ax1.set_xlabel("Customers")
    ax1.grid(axis="x")
    ax1.grid(axis="y", visible=False)
    ax1.set_xlim(0, df["customers"].max() * 1.15)

    b2 = ax2.barh(tiers_title, df["mrr_k"], color=colors,
                  edgecolor="white", linewidth=0.3, height=0.5)
    for bar, val in zip(b2, df["mrr_k"], strict=False):
        ax2.text(bar.get_width() + 1, bar.get_y() + bar.get_height() / 2,
                 f"${val:,.0f}k", va="center", fontsize=9, color=SLATE)
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
    df = df.sort_values("total_revenue_loss_proxy", ascending=False)
    df["at_risk_k"]  = df["at_risk_mrr"] / 1_000
    df["churned_k"]  = df["churned_revenue"] / 1_000
    x = np.arange(len(df))
    w = 0.38

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(x - w / 2, df["at_risk_k"],  width=w, color=LOSS,   alpha=0.85, label="MRR at risk (current)",
           edgecolor="white", linewidth=0.3)
    ax.bar(x + w / 2, df["churned_k"],  width=w, color=ACCENT, alpha=0.85, label="Revenue already churned",
           edgecolor="white", linewidth=0.3)
    ax.set_title("Revenue Exposure by Segment: At-Risk MRR vs Churned Revenue")
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
    df = df.sort_values("churn_rate_lift", ascending=True)
    colors = [LOSS if v >= 5.0 else MUTED for v in df["churn_rate_lift"]]

    fig, ax = plt.subplots(figsize=(10, 4.5))
    bars = ax.barh(df["label"], df["churn_rate_lift"], color=colors,
                   edgecolor="white", linewidth=0.3, height=0.5)
    for bar, val in zip(bars, df["churn_rate_lift"], strict=False):
        ax.text(bar.get_width() + 0.2, bar.get_y() + bar.get_height() / 2,
                f"{val:.1f}×", va="center", fontsize=9, color=SLATE)
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
    ax.text(1.05, 0.02, "No lift", transform=ax.get_xaxis_transform(),
            fontsize=8, color=MUTED, va="bottom")
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
            subset, bins=28, alpha=0.72, color=TIER_COLORS[tier],
            label=f"{tier.title()}  ({len(subset):,})",
            edgecolor="white", linewidth=0.3,
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
    df = pd.read_csv(PROC / "cohort_retention_table.csv",
                     parse_dates=["cohort_month", "observation_month"])
    df["age"] = (
        (df["observation_month"].dt.year  - df["cohort_month"].dt.year) * 12
        + (df["observation_month"].dt.month - df["cohort_month"].dt.month)
    )
    pivot = df.pivot_table(index="cohort_month", columns="age",
                           values="retention_rate", aggfunc="mean")
    pivot = pivot.loc[:, pivot.columns <= 12]

    # Keep the most recent 24 cohorts
    pivot = pivot.iloc[-24:]
    pivot.index = pd.DatetimeIndex(pivot.index).strftime("%Y-%m")

    nrows = len(pivot)
    fig, ax = plt.subplots(figsize=(14, max(5.5, nrows * 0.32 + 2.5)))
    im = ax.imshow(pivot.values, aspect="auto", cmap="RdYlGn",
                   vmin=0, vmax=1, interpolation="nearest")

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
            ax.text(j, i, f"{val:.0%}", ha="center", va="center",
                    fontsize=8, color=text_color, fontweight="normal")

    cbar = fig.colorbar(im, ax=ax, fraction=0.018, pad=0.02)
    cbar.set_label("Retention rate")
    cbar.ax.yaxis.set_major_formatter(mticker.PercentFormatter(1.0, decimals=0))
    cbar.outline.set_visible(False)

    fig.tight_layout()
    save(fig, "cohort_retention_heatmap.png")


# ──────────────────────────────────────────────────────────
# 11. Cohort retention curves (last 8 cohorts)
# ──────────────────────────────────────────────────────────
def chart_cohort_curves() -> None:
    df = pd.read_csv(PROC / "cohort_retention_table.csv",
                     parse_dates=["cohort_month", "observation_month"])
    df["age"] = (
        (df["observation_month"].dt.year  - df["cohort_month"].dt.year) * 12
        + (df["observation_month"].dt.month - df["cohort_month"].dt.month)
    )
    recent = sorted(df["cohort_month"].unique())[-8:]
    n = len(recent)

    # Blues gradient: older cohorts muted, most recent prominent
    blues = plt.cm.Blues(np.linspace(0.35, 0.90, n))

    fig, ax = plt.subplots(figsize=(11, 5.5))
    for i, cm in enumerate(recent):
        sub = df[df["cohort_month"] == cm].sort_values("age")
        lw  = 2.4 if i == n - 1 else (1.8 if i == n - 2 else 1.1)
        lbl = pd.Timestamp(cm).strftime("%Y-%m")
        ax.plot(sub["age"], sub["retention_rate"] * 100,
                color=blues[i], lw=lw, label=lbl, zorder=i + 1)

    ax.set_title("Customer Retention by Cohort Age (8 most recent cohorts)")
    ax.set_xlabel("Months since acquisition")
    ax.set_ylabel("Retention rate (%)")
    ax.yaxis.set_major_formatter(PCT)
    ax.set_ylim(0, 108)
    ax.legend(title="Cohort", loc="lower left", fontsize=9, ncol=2,
              title_fontsize=9)
    ax.grid(axis="y")
    ax.grid(axis="x", visible=False)
    fig.tight_layout()
    save(fig, "cohort_retention_curves.png")


# ──────────────────────────────────────────────────────────
# 12. Segment health comparison — churn, NPS, usage trend
# ──────────────────────────────────────────────────────────
def chart_segment_health() -> None:
    df = pd.read_csv(PROC / "segment_retention_summary.csv")
    df["churn_pct"] = df["churn_rate"] * 100
    seg_order = ["Enterprise", "Mid-Market", "SMB", "Startup"]
    df = df.set_index("segment").loc[[s for s in seg_order if s in df["segment"].values]].reset_index()
    colors = [SEG_COLORS.get(s, MUTED) for s in df["segment"]]
    x = np.arange(len(df))
    bar_w = 0.55

    fig, axes = plt.subplots(1, 3, figsize=(13, 4.5))
    fig.suptitle("Segment Health: Churn Rate, NPS, and Usage Trend")

    # Panel 1: Churn rate
    ax = axes[0]
    ax.bar(x, df["churn_pct"], color=colors, width=bar_w, edgecolor="white", linewidth=0.3)
    ax.set_title("Churn Rate (%)")
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


# ── Entry point ────────────────────────────────────────────
def main() -> None:
    print(f"Generating charts → {OUT.relative_to(ROOT)}/")
    chart_churn_rate_trend()
    chart_mrr_trend()
    chart_churn_by_segment()
    chart_churn_by_channel()
    chart_churn_by_plan()
    chart_risk_tier_breakdown()
    chart_revenue_at_risk_by_segment()
    chart_behavioral_drivers()
    chart_risk_score_distribution()
    chart_cohort_heatmap()
    chart_cohort_curves()
    chart_segment_health()
    print(f"\n{len(list(OUT.glob('*.png')))} charts written to {OUT.relative_to(ROOT)}/")


if __name__ == "__main__":
    main()
