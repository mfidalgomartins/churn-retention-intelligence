"""
Analytical report builder for Churn & Retention Intelligence.

Assembles the multi-page narrative PDF in outputs/reports/ from the processed
data, the analytical tables, and the static chart pack in outputs/graphs/.
Run directly:

    python -m churn.report

The report is a single deliverable: flowing prose, charts inline, a cover page,
a linked table of contents, and a running footer. Every figure is read from the
governed pipeline artifacts so the report stays in lockstep with the dashboard.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import pandas as pd
from matplotlib import font_manager
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import cm
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen.canvas import Canvas
from reportlab.platypus import (
    BaseDocTemplate,
    Frame,
    Image,
    KeepTogether,
    NextPageTemplate,
    PageBreak,
    PageTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)
from reportlab.platypus.tableofcontents import TableOfContents

from churn.common import REFERENCE_DATE as SNAPSHOT_DATE

# ── Paths ──────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[2]
GRAPHS = ROOT / "outputs" / "graphs"
TABLES = ROOT / "outputs" / "tables"
PROC = ROOT / "data" / "processed"
OUT = ROOT / "outputs" / "reports"
OUT.mkdir(parents=True, exist_ok=True)

REPORT_PATH = OUT / "churn-retention-intelligence-report.pdf"

# ── Editorial design system ───────────────────────────────
MIDNIGHT = colors.HexColor("#071824")
INK = colors.HexColor("#152431")
SLATE = colors.HexColor("#394955")
MUTED = colors.HexColor("#60727F")
HAIR = colors.HexColor("#CED6DC")
PALE = colors.HexColor("#E7ECEF")
MIST = colors.HexColor("#F2F4F6")
PAPER = colors.HexColor("#FFFFFF")
CYAN = colors.HexColor("#13A8D3")
TEAL = colors.HexColor("#00A88F")
LIME = colors.HexColor("#A8D400")
VIOLET = colors.HexColor("#5A2A83")
NAVY = MIDNIGHT
LOSS = colors.HexColor("#A82D73")
GAIN = TEAL

PAGE_W, PAGE_H = A4
LMARGIN = 1.45 * cm
RMARGIN = 1.45 * cm
TMARGIN = 1.55 * cm
BMARGIN = 1.65 * cm
CONTENT_W = PAGE_W - LMARGIN - RMARGIN


def _register_report_fonts() -> None:
    """Embed a stable serif/sans pair so the PDF renders consistently everywhere."""
    font_specs = {
        "ReportSans": font_manager.findfont("DejaVu Sans"),
        "ReportSans-Bold": font_manager.findfont(
            font_manager.FontProperties(family="DejaVu Sans", weight="bold")
        ),
        "ReportSerif": font_manager.findfont("DejaVu Serif"),
        "ReportSerif-Italic": font_manager.findfont(
            font_manager.FontProperties(family="DejaVu Serif", style="italic")
        ),
        "ReportMono": font_manager.findfont("DejaVu Sans Mono"),
    }
    for alias, path in font_specs.items():
        if alias not in pdfmetrics.getRegisteredFontNames():
            pdfmetrics.registerFont(TTFont(alias, path))


_register_report_fonts()

REFERENCE_DATE = f"{SNAPSHOT_DATE.day} {SNAPSHOT_DATE.strftime('%B %Y')}"
BUILD_DATE = os.getenv("CHURN_REPORT_DATE", REFERENCE_DATE)


# ════════════════════════════════════════════════════════════
# Metrics — pulled live from the pipeline artifacts
# ════════════════════════════════════════════════════════════
def _csv(name: str, where: Path = TABLES) -> pd.DataFrame:
    return pd.read_csv(where / name)


def load_metrics() -> dict:
    """Read every number the narrative cites, so prose tracks the data."""
    feats = pd.read_csv(PROC / "customer_retention_features.csv")
    risk = pd.read_csv(PROC / "customer_risk_scores.csv")
    seg = _csv("churn_by_segment.csv").set_index("segment")
    chan = _csv("churn_by_acquisition_channel.csv").set_index("acquisition_channel")
    plan = _csv("churn_by_plan_type.csv").set_index("plan_type")
    reg = _csv("churn_by_region.csv").set_index("region")
    beh = _csv("behavioral_churn_relationships.csv").set_index("relationship")
    tier = _csv("risk_tier_summary.csv").set_index("risk_tier")
    drv = _csv("main_analysis_churn_driver_ranking.csv")
    itv = _csv("main_analysis_intervention_priorities.csv")
    segr = _csv("segment_revenue_risk_contribution.csv").set_index("segment")
    economics = _csv("unit_economics_summary.csv").set_index("metric")
    model = _csv("model_performance.csv").set_index("split")
    incrementality = _csv("intervention_incrementality.csv").set_index("metric")
    balance = _csv("intervention_balance.csv")
    monitoring = _csv("monitoring_summary.csv").iloc[0]
    trend = pd.read_csv(TABLES / "overall_retention_trend_monthly.csv", parse_dates=["month"])
    trend = trend[trend["active_customers_start"] >= 50].copy()
    val = pd.read_csv(TABLES / "final_validation_checks.csv")
    gov = [
        [cat, f"{int((g['status'] == 'PASS').sum())} / {len(g)}"]
        for cat, g in val.groupby("category")
    ]
    gov_total = len(val)
    gov_pass = int((val["status"] == "PASS").sum())

    total = len(feats)
    churned = int(feats["churn_flag"].sum())
    active = total - churned

    # Revenue concentration of lost monthly value
    lost = (
        feats.loc[feats["churn_flag"] == 1, "avg_monthly_revenue"]
        .sort_values(ascending=False)
        .reset_index(drop=True)
    )
    cum = lost.cumsum() / lost.sum()

    def conc(q):
        k = max(int(len(lost) * q) - 1, 0)
        return float(cum.iloc[k]) * 100

    last = trend.iloc[-1]
    prior9 = trend["customer_churn_rate"].iloc[-12:-3].mean() * 100
    last3 = trend.tail(3)
    last3_churn = last3["customer_churn_rate"].mean() * 100
    last3_rev_churn = last3["revenue_churn_rate"].mean() * 100
    prior9_rev_churn = trend["revenue_churn_rate"].iloc[-12:-3].mean() * 100
    t12 = trend.tail(12)
    trend12 = [
        [
            r["month"].strftime("%b %Y"),
            f"{int(r['active_customers_start']):,}",
            usd(r["active_mrr_start"], k=True),
            f"{int(r['churned_customers']):,}",
            f"{r['customer_churn_rate'] * 100:.1f}%",
            f"{r['revenue_churn_rate'] * 100:.1f}%",
        ]
        for _, r in t12.iterrows()
    ]

    cohort = pd.read_csv(
        PROC / "cohort_retention_table.csv", parse_dates=["cohort_month", "observation_month"]
    )
    cohort["age_months"] = (
        (cohort["observation_month"].dt.year - cohort["cohort_month"].dt.year) * 12
        + cohort["observation_month"].dt.month
        - cohort["cohort_month"].dt.month
    )

    def cohort_age_delta(age: int) -> dict:
        aged = cohort[cohort["age_months"] == age].sort_values("cohort_month")
        if len(aged) < 6:
            return {
                "age": age,
                "n": len(aged),
                "early_logo": float("nan"),
                "recent_logo": float("nan"),
                "logo_delta": float("nan"),
                "early_rev": float("nan"),
                "recent_rev": float("nan"),
                "rev_delta": float("nan"),
            }
        w = min(6, len(aged) // 2)
        early, recent = aged.head(w), aged.tail(w)
        return {
            "age": age,
            "n": len(aged),
            "early_logo": float(early["retention_rate"].mean()) * 100,
            "recent_logo": float(recent["retention_rate"].mean()) * 100,
            "logo_delta": (
                float(recent["retention_rate"].mean()) - float(early["retention_rate"].mean())
            )
            * 100,
            "early_rev": float(early["revenue_retention"].mean()) * 100,
            "recent_rev": float(recent["revenue_retention"].mean()) * 100,
            "rev_delta": (
                float(recent["revenue_retention"].mean()) - float(early["revenue_retention"].mean())
            )
            * 100,
        }

    cohort_deltas = {age: cohort_age_delta(age) for age in [3, 6, 9, 12]}

    weak_channels = chan.loc[["Affiliate", "Paid Search"]]
    quality_channels = chan.loc[["Partner", "Referral"]]
    low_end_segments = seg.loc[["Startup", "SMB"]]
    durable_segments = seg.loc[["Mid-Market", "Enterprise"]]
    low_plans = plan.loc[["Basic", "Growth"]]
    premium_plans = plan.loc[["Pro", "Enterprise"]]
    high_crit = tier.loc[["critical", "high"]]

    active_feats = feats[feats["churn_flag"] == 0].copy()
    active_current_mrr = float(active_feats["current_mrr"].sum())
    signal_flags = pd.DataFrame(
        {
            "payment_failure": active_feats["payment_failure_flag"] == 1,
            "usage_decline": active_feats["usage_trend"] < 0,
            "low_nps": (
                active_feats["nps_score_recent"] <= feats["nps_score_recent"].quantile(0.25)
            ),
            "high_support": (
                active_feats["support_tickets_90d"] >= feats["support_tickets_90d"].quantile(0.75)
            ),
            "low_adoption": (
                active_feats["feature_adoption_score_recent"]
                <= feats["feature_adoption_score_recent"].quantile(0.25)
            ),
        },
        index=active_feats.index,
    )
    active_feats["distress_signal_count"] = signal_flags.sum(axis=1)
    multi_signal = active_feats["distress_signal_count"] >= 2

    def share(n, d):
        return float(n / d * 100) if d else 0.0

    return {
        "total": total,
        "active": active,
        "churned": churned,
        "cum_churn_pct": churned / total * 100,
        "active_mrr": active_current_mrr,
        "active_current_mrr": active_current_mrr,
        "cum_revenue_loss_pct": share(lost.sum(), feats["avg_monthly_revenue"].sum()),
        "window_start": trend["month"].min().strftime("%b %Y"),
        "window_end": trend["month"].max().strftime("%b %Y"),
        "avg_cust_churn": trend["customer_churn_rate"].mean() * 100,
        "avg_rev_churn": trend["revenue_churn_rate"].mean() * 100,
        "last_cust_churn": float(last["customer_churn_rate"]) * 100,
        "last_rev_churn": float(last["revenue_churn_rate"]) * 100,
        "prior9_churn": prior9,
        "last3_churn": last3_churn,
        "last3_rev_churn": last3_rev_churn,
        "prior9_rev_churn": prior9_rev_churn,
        "last3_churn_delta_pp": last3_churn - prior9,
        "last3_rev_churn_delta_pp": last3_rev_churn - prior9_rev_churn,
        "trend12": trend12,
        "gov": gov,
        "gov_total": gov_total,
        "gov_pass": gov_pass,
        # segments
        "seg": seg,
        "chan": chan,
        "plan": plan,
        "reg": reg,
        "beh": beh,
        "tier": tier,
        "drv": drv,
        "itv": itv,
        "segr": segr,
        "risk_mean": float(risk["churn_risk_score"].mean()),
        "scored": len(risk),
        "rec_counts": risk["recommended_action"].value_counts().to_dict(),
        "driver_counts": risk["main_risk_driver"].value_counts().to_dict(),
        # concentration
        "conc5": conc(0.05),
        "conc10": conc(0.10),
        "conc20": conc(0.20),
        "conc30": conc(0.30),
        "conc50": conc(0.50),
        "lost_mrr_total": float(lost.sum()),
        # cohort and mix diagnostics
        "cohort_deltas": cohort_deltas,
        "weak_channel_acct_share": share(weak_channels["customers"].sum(), total),
        "weak_channel_churn_share": share(weak_channels["churned_customers"].sum(), churned),
        "weak_channel_loss_share": share(
            weak_channels["churned_revenue"].sum(),
            seg["churned_revenue"].sum(),
        ),
        "quality_channel_acct_share": share(quality_channels["customers"].sum(), total),
        "quality_channel_churn_share": share(
            quality_channels["churned_customers"].sum(),
            churned,
        ),
        "low_end_acct_share": share(low_end_segments["customers"].sum(), total),
        "low_end_churn_share": share(low_end_segments["churned_customers"].sum(), churned),
        "durable_acct_share": share(durable_segments["customers"].sum(), total),
        "durable_churn_share": share(durable_segments["churned_customers"].sum(), churned),
        "low_plan_acct_share": share(low_plans["customers"].sum(), total),
        "low_plan_churn_share": share(low_plans["churned_customers"].sum(), churned),
        "premium_plan_acct_share": share(premium_plans["customers"].sum(), total),
        "premium_plan_churn_share": share(premium_plans["churned_customers"].sum(), churned),
        "multi_signal_active": int(multi_signal.sum()),
        "multi_signal_active_mrr": float(active_feats.loc[multi_signal, "current_mrr"].sum()),
        "zero_signal_active": int((active_feats["distress_signal_count"] == 0).sum()),
        "zero_signal_active_mrr": float(
            active_feats.loc[active_feats["distress_signal_count"] == 0, "current_mrr"].sum()
        ),
        # tier exposure
        "crit_n": int(tier.loc["critical", "customers"]),
        "high_n": int(tier.loc["high", "customers"]),
        "med_n": int(tier.loc["medium", "customers"]),
        "crit_mrr": float(tier.loc["critical", "total_current_mrr"]),
        "high_mrr": float(tier.loc["high", "total_current_mrr"]),
        "med_mrr": float(tier.loc["medium", "total_current_mrr"]),
        "high_crit_count_share": share(high_crit["customers"].sum(), tier["customers"].sum()),
        "high_crit_mrr_share": share(
            high_crit["total_current_mrr"].sum(),
            tier["total_current_mrr"].sum(),
        ),
        "play_scope_mrr": float(itv["current_mrr_scope"].sum()),
        "play_weighted_exposure": float(itv["mrr_exposure_proxy"].sum()),
        "average_monthly_nrr": float(economics.loc["average_monthly_nrr", "value"]),
        "gross_margin_rate": float(economics.loc["gross_margin_rate", "value"]),
        "blended_cac": float(economics.loc["blended_cac", "value"]),
        "model_roc_auc": float(model.loc["test", "roc_auc"]),
        "model_average_precision": float(model.loc["test", "average_precision"]),
        "model_brier_score": float(model.loc["test", "brier_score"]),
        "experiment_eligible": int(
            incrementality.loc["churned_90d", "treatment_n"]
            + incrementality.loc["churned_90d", "control_n"]
        ),
        "experiment_treatment": int(incrementality.loc["churned_90d", "treatment_n"]),
        "experiment_control": int(incrementality.loc["churned_90d", "control_n"]),
        "simulated_saved_mrr": float(incrementality.loc["lost_mrr_90d", "incremental_saved_mrr"]),
        "simulated_saved_mrr_ci_lower": float(
            -incrementality.loc["lost_mrr_90d", "ci_95_upper"]
            * incrementality.loc["lost_mrr_90d", "treatment_n"]
        ),
        "simulated_saved_mrr_ci_upper": float(
            -incrementality.loc["lost_mrr_90d", "ci_95_lower"]
            * incrementality.loc["lost_mrr_90d", "treatment_n"]
        ),
        "maximum_experiment_smd": float(
            balance.loc[
                balance["statistic_name"] == "standardized_mean_difference",
                "balance_statistic",
            ]
            .abs()
            .max()
        ),
        "monitoring_alerts": int(monitoring["open_alerts"]),
    }


def usd(x: float, k: bool = False) -> str:
    sign = "-" if x < 0 else ""
    if k:
        return f"{sign}${abs(x) / 1000:,.0f}k"
    return f"{sign}${abs(x):,.0f}"


_ONES = ["zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"]


def floor_multiple(x: float) -> str:
    """Floor a lift ratio to a whole multiple, spelled out under ten to match report prose style."""
    n = int(x)
    return _ONES[n] if 0 <= n < 10 else str(n)


def code(text: str) -> str:
    """Mark a literal field, file, or path name so it reads as a system
    identifier rather than prose, wherever it appears inline or in a table."""
    return f'<font face="ReportMono" size="7.2">{text}</font>'


# ════════════════════════════════════════════════════════════
# Styles
# ════════════════════════════════════════════════════════════
def build_styles() -> dict:
    body = ParagraphStyle(
        "Body",
        fontName="ReportSans",
        fontSize=8.7,
        leading=12.7,
        textColor=INK,
        alignment=TA_LEFT,
        spaceAfter=7,
        firstLineIndent=0,
    )
    styles = {
        "body": body,
        "lead": ParagraphStyle(
            "Lead",
            parent=body,
            fontName="ReportSerif",
            fontSize=13.4,
            leading=17.8,
            spaceAfter=12,
            textColor=SLATE,
        ),
        "h1": ParagraphStyle(
            "H1",
            fontName="ReportSans-Bold",
            fontSize=22.5,
            leading=26,
            textColor=PAPER,
            backColor=MIDNIGHT,
            borderPadding=(13, 14, 13, 14),
            spaceBefore=2,
            spaceAfter=0,
        ),
        "h1num": ParagraphStyle(
            "H1num",
            fontName="ReportSans-Bold",
            fontSize=7.8,
            leading=10,
            textColor=CYAN,
            backColor=MIDNIGHT,
            borderPadding=(6, 14, 1, 14),
            spaceAfter=0,
        ),
        "h2": ParagraphStyle(
            "H2",
            fontName="ReportSerif",
            fontSize=15.3,
            leading=19,
            textColor=INK,
            spaceBefore=15,
            spaceAfter=6,
        ),
        "h3": ParagraphStyle(
            "H3",
            fontName="ReportSans-Bold",
            fontSize=9.8,
            leading=13,
            textColor=SLATE,
            spaceBefore=10,
            spaceAfter=4,
        ),
        "caption": ParagraphStyle(
            "Caption",
            fontName="ReportSans",
            fontSize=7.2,
            leading=10,
            textColor=MUTED,
            alignment=TA_LEFT,
            spaceBefore=5,
            spaceAfter=15,
        ),
        "pull": ParagraphStyle(
            "Pull",
            fontName="ReportSerif",
            fontSize=13.2,
            leading=18,
            textColor=INK,
            alignment=TA_LEFT,
        ),
        "kicker": ParagraphStyle(
            "Kicker",
            fontName="ReportSans-Bold",
            fontSize=7.8,
            leading=10,
            textColor=CYAN,
            spaceAfter=3,
        ),
        "toc1": ParagraphStyle(
            "TOC1",
            fontName="ReportSans-Bold",
            fontSize=7.4,
            leading=10.5,
            textColor=INK,
        ),
        "toc2": ParagraphStyle(
            "TOC2",
            fontName="ReportSans",
            fontSize=6.5,
            leading=8,
            textColor=SLATE,
            leftIndent=16,
        ),
        # cover
        "cover_kick": ParagraphStyle(
            "CK",
            fontName="ReportSans-Bold",
            fontSize=8.2,
            leading=12,
            textColor=CYAN,
            alignment=TA_LEFT,
        ),
        "cover_title": ParagraphStyle(
            "CT",
            fontName="ReportSans-Bold",
            fontSize=33,
            leading=36,
            textColor=PAPER,
            alignment=TA_LEFT,
            spaceBefore=10,
            spaceAfter=8,
        ),
        "cover_sub": ParagraphStyle(
            "CS",
            fontName="ReportSerif",
            fontSize=14.5,
            leading=20,
            textColor=colors.HexColor("#E7EEF2"),
            alignment=TA_LEFT,
        ),
        "cover_meta": ParagraphStyle(
            "CM",
            fontName="ReportSans",
            fontSize=7.8,
            leading=12,
            textColor=colors.HexColor("#B9C7CF"),
            alignment=TA_LEFT,
        ),
        "tbl": ParagraphStyle(
            "Tbl",
            fontName="ReportSans",
            fontSize=7.4,
            leading=10.2,
            textColor=INK,
        ),
        "tbl_r": ParagraphStyle(
            "TblR",
            fontName="ReportSans",
            fontSize=7.4,
            leading=10.2,
            textColor=INK,
            alignment=TA_RIGHT,
        ),
        "tblh": ParagraphStyle(
            "Tblh",
            fontName="ReportSans-Bold",
            fontSize=7.2,
            leading=9.5,
            textColor=PAPER,
        ),
        "tblh_r": ParagraphStyle(
            "TblhR",
            fontName="ReportSans-Bold",
            fontSize=7.2,
            leading=9.5,
            textColor=PAPER,
            alignment=TA_RIGHT,
        ),
    }
    return styles


# ════════════════════════════════════════════════════════════
# Flowable helpers
# ════════════════════════════════════════════════════════════
class SectionHeading(Paragraph):
    """A Heading1/Heading2 paragraph that registers itself in the TOC."""

    def __init__(self, text, style, level, key):
        super().__init__(text, style)
        self._toc_level = level
        self._toc_key = key
        self._toc_text = text


def fig(name: str, caption: str, styles: dict, width_frac: float = 1.0, max_h: float = 11.5 * cm):
    """Inline figure scaled to the content width, with a numbered caption."""
    path = GRAPHS / name
    iw, ih = ImageReader(str(path)).getSize()
    w = CONTENT_W * width_frac - 10
    h = w * ih / iw
    if h > max_h:
        h = max_h
        w = h * iw / ih
    img = Image(str(path), width=w, height=h)
    img.hAlign = "CENTER"
    panel = Table([[img]], colWidths=[w + 10])
    panel.hAlign = "CENTER"
    panel.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), MIST),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
            ]
        )
    )
    cap = Paragraph(caption, styles["caption"])
    return KeepTogether([Spacer(1, 5), panel, cap])


def rule(color=HAIR, thickness=0.6, space_before=2, space_after=8, width=None):
    t = Table([[""]], colWidths=[width or CONTENT_W], rowHeights=[0.1])
    t.setStyle(
        TableStyle(
            [
                ("LINEABOVE", (0, 0), (-1, 0), thickness, color),
                ("TOPPADDING", (0, 0), (-1, -1), 0),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
            ]
        )
    )
    return KeepTogether([Spacer(1, space_before), t, Spacer(1, space_after)])


def pull_quote(text: str, styles: dict) -> KeepTogether:
    """A callout box for a single load-bearing sentence, used sparingly to
    break up dense text pages and signal which line matters most."""
    mark = Paragraph(
        "“",
        ParagraphStyle(
            "QuoteMark",
            fontName="ReportSerif",
            fontSize=27,
            leading=25,
            textColor=CYAN,
        ),
    )
    p = Paragraph(text, styles["pull"])
    t = Table([[mark, p]], colWidths=[1.05 * cm, CONTENT_W - 1.05 * cm])
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), MIST),
                ("LINEABOVE", (0, 0), (-1, 0), 1.4, CYAN),
                ("LINEBELOW", (0, -1), (-1, -1), 0.5, HAIR),
                ("TOPPADDING", (0, 0), (-1, -1), 11),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 11),
                ("LEFTPADDING", (0, 0), (0, -1), 12),
                ("RIGHTPADDING", (0, 0), (0, -1), 0),
                ("LEFTPADDING", (1, 0), (1, -1), 2),
                ("RIGHTPADDING", (1, 0), (1, -1), 14),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]
        )
    )
    return KeepTogether([Spacer(1, 6), t, Spacer(1, 10)])


def stat_band(items, styles):
    """A row of headline statistics used in the executive summary."""
    cells = []
    for value, label in items:
        v = Paragraph(
            value,
            ParagraphStyle(
                "sv",
                fontName="ReportSans-Bold",
                fontSize=18,
                leading=20,
                textColor=PAPER,
                alignment=TA_CENTER,
            ),
        )
        lab = Paragraph(
            label,
            ParagraphStyle(
                "sl",
                fontName="ReportSans-Bold",
                fontSize=6.7,
                leading=9,
                textColor=colors.HexColor("#A9DCE9"),
                alignment=TA_CENTER,
            ),
        )
        inner = Table([[v], [lab]], colWidths=[CONTENT_W / len(items) - 6])
        inner.setStyle(
            TableStyle(
                [
                    ("TOPPADDING", (0, 0), (-1, -1), 2),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ]
            )
        )
        cells.append(inner)
    t = Table([cells], colWidths=[CONTENT_W / len(items)] * len(items))
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), MIDNIGHT),
                ("LINEABOVE", (0, 0), (-1, 0), 2.2, CYAN),
                ("LINEBEFORE", (1, 0), (-1, -1), 0.35, colors.HexColor("#49606D")),
                ("TOPPADDING", (0, 0), (-1, -1), 12),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 12),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]
        )
    )
    return KeepTogether([Spacer(1, 6), t, Spacer(1, 14)])


_NUMERIC_CELL = re.compile(r"^-?\$?[\d,]+(\.\d+)?\s?(%|×|pp|k)?$")


def _is_numeric_column(rows: list, col_idx: int) -> bool:
    """A column reads as a magnitude to compare, not a label, when every
    non-blank cell is a bare number, currency, percentage, or multiple."""
    values = [str(r[col_idx]).strip() for r in rows]
    non_empty = [v for v in values if v]
    return bool(non_empty) and all(_NUMERIC_CELL.match(v) for v in non_empty)


def data_table(header, rows, styles, col_widths):
    # Numeric columns are magnitudes to compare down the column, so they read
    # better right-aligned; label columns stay left-aligned. Alignment has to
    # be set on the cell Paragraph's own style: a Table's ALIGN command only
    # positions a flowable within its cell, and a Paragraph already fills the
    # full column width, so it would have no visible effect here.
    numeric_col = [bool(rows) and _is_numeric_column(rows, i) for i in range(len(header))]
    head = [
        Paragraph(h, styles["tblh_r"] if numeric_col[i] else styles["tblh"])
        for i, h in enumerate(header)
    ]
    body = [
        [
            Paragraph(str(c), styles["tbl_r"] if numeric_col[i] else styles["tbl"])
            for i, c in enumerate(r)
        ]
        for r in rows
    ]
    t = Table([head, *body], colWidths=col_widths, repeatRows=1)
    style = [
        ("BACKGROUND", (0, 0), (-1, 0), MIDNIGHT),
        ("LINEABOVE", (0, 0), (-1, 0), 1.8, CYAN),
        ("TOPPADDING", (0, 0), (-1, -1), 5.5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5.5),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LINEBELOW", (0, 0), (-1, -1), 0.35, HAIR),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [PAPER, MIST]),
    ]
    t.setStyle(TableStyle(style))
    return KeepTogether([Spacer(1, 4), t, Spacer(1, 14)])


# ════════════════════════════════════════════════════════════
# Page furniture (cover frame + content frame + footer)
# ════════════════════════════════════════════════════════════
class ReportDoc(BaseDocTemplate):
    def __init__(self, path, **kw):
        super().__init__(path, **kw)
        self.allowSplitting = 1
        cover = PageTemplate(
            id="cover",
            frames=[
                Frame(
                    LMARGIN,
                    BMARGIN,
                    CONTENT_W,
                    PAGE_H - TMARGIN - BMARGIN,
                    id="cf",
                    leftPadding=0,
                    rightPadding=0,
                    topPadding=0,
                    bottomPadding=0,
                )
            ],
            onPage=self._cover_bg,
        )
        content = PageTemplate(
            id="content",
            frames=[
                Frame(
                    LMARGIN,
                    BMARGIN,
                    CONTENT_W,
                    PAGE_H - TMARGIN - BMARGIN,
                    id="nf",
                    leftPadding=0,
                    rightPadding=0,
                    topPadding=0,
                    bottomPadding=0,
                )
            ],
            onPage=self._footer,
        )
        self.addPageTemplates([cover, content])

    def afterFlowable(self, flowable):
        if isinstance(flowable, SectionHeading):
            self.notify(
                "TOCEntry", (flowable._toc_level, flowable._toc_text, self.page, flowable._toc_key)
            )
            self.canv.bookmarkPage(flowable._toc_key)
            self.canv.addOutlineEntry(
                _strip_tags(flowable._toc_text),
                flowable._toc_key,
                level=flowable._toc_level,
                closed=(flowable._toc_level > 0),
            )

    def _cover_bg(self, canvas, doc):
        canvas.saveState()
        canvas.setFillColor(MIDNIGHT)
        canvas.rect(0, 0, PAGE_W, PAGE_H, fill=1, stroke=0)

        # Layered vector fields echo the reference's photographic light without
        # importing third-party imagery into the analytical deliverable.
        canvas.setFillAlpha(0.72)
        canvas.setFillColor(colors.HexColor("#004A75"))
        canvas.circle(2.2 * cm, 1.1 * cm, 8.5 * cm, fill=1, stroke=0)
        canvas.setFillAlpha(0.48)
        canvas.setFillColor(CYAN)
        canvas.circle(0.5 * cm, 3.0 * cm, 5.7 * cm, fill=1, stroke=0)
        canvas.setFillAlpha(0.38)
        canvas.setFillColor(VIOLET)
        canvas.circle(8.3 * cm, 0.8 * cm, 6.6 * cm, fill=1, stroke=0)
        canvas.setFillAlpha(0.22)
        canvas.setFillColor(TEAL)
        canvas.circle(17.7 * cm, 13.3 * cm, 8.0 * cm, fill=1, stroke=0)
        canvas.setFillAlpha(1)

        # Asymmetric editorial frame with the report's four accent roots.
        x0, y0 = 1.05 * cm, 18.15 * cm
        canvas.setLineWidth(5.5)
        canvas.setStrokeColor(CYAN)
        canvas.line(x0, y0, x0, 23.2 * cm)
        canvas.line(x0, 23.2 * cm, 5.3 * cm, 24.0 * cm)
        canvas.setStrokeColor(TEAL)
        canvas.line(5.3 * cm, 24.0 * cm, 9.3 * cm, 24.7 * cm)
        canvas.setStrokeColor(LIME)
        canvas.line(9.3 * cm, 24.7 * cm, 19.45 * cm, 25.35 * cm)
        canvas.line(19.45 * cm, 25.35 * cm, 19.45 * cm, 18.2 * cm)
        canvas.setFillColor(CYAN)
        for idx in range(4):
            canvas.rect(x0 + idx * 0.52 * cm, 17.45 * cm, 0.24 * cm, 0.24 * cm, fill=1, stroke=0)

        canvas.setFont("ReportSans-Bold", 6.8)
        canvas.setFillColor(colors.HexColor("#B9C7CF"))
        canvas.drawString(1.05 * cm, 1.0 * cm, "RETENTION ANALYTICS  /  EXECUTIVE DECISION SUPPORT")
        canvas.restoreState()

    def _footer(self, canvas, doc):
        canvas.saveState()
        canvas.setFillAlpha(1)
        canvas.setFillColor(PAPER)
        canvas.rect(0, 0, PAGE_W, PAGE_H, fill=1, stroke=0)
        # Consulting-style chapter rail: quiet identity at left, four evidence
        # phases at right, and a stable full-width footer band.
        header_y = PAGE_H - 0.68 * cm
        canvas.setFont("ReportSans-Bold", 6.4)
        canvas.setFillColor(MUTED)
        canvas.drawString(LMARGIN, header_y, "RETENTION INTELLIGENCE")
        rail_x = PAGE_W - RMARGIN - 7.3 * cm
        rail_w = 7.3 * cm / 4
        rail_colors = [CYAN, colors.HexColor("#168FC3"), TEAL, LIME]
        for idx, rail_color in enumerate(rail_colors):
            x = rail_x + idx * rail_w
            canvas.setStrokeColor(rail_color)
            canvas.setLineWidth(1.2)
            canvas.line(x, header_y + 1, x + rail_w, header_y + 1)
            canvas.setFillColor(PAPER)
            canvas.circle(x + rail_w, header_y + 1, 3.2, fill=1, stroke=1)

        band_h = 0.72 * cm
        canvas.setFillColor(MIDNIGHT)
        canvas.rect(0, 0, PAGE_W, band_h, fill=1, stroke=0)
        segment_w = PAGE_W / 4
        for idx, rail_color in enumerate(rail_colors):
            canvas.setFillColor(rail_color)
            canvas.rect(idx * segment_w, band_h, segment_w, 1.6, fill=1, stroke=0)
        canvas.setFont("ReportSans", 6.7)
        canvas.setFillColor(colors.HexColor("#D7E1E6"))
        canvas.drawString(LMARGIN, 0.27 * cm, "Churn & Retention Intelligence")
        canvas.setFillColor(CYAN)
        canvas.drawRightString(PAGE_W - RMARGIN, 0.27 * cm, f"Retention Review  /  {doc.page:02d}")
        canvas.restoreState()


def _strip_tags(s: str) -> str:
    return re.sub(r"<[^>]+>", "", s)


# ════════════════════════════════════════════════════════════
# Story assembly
# ════════════════════════════════════════════════════════════
def P(text, styles, key="body"):
    return Paragraph(text, styles[key])


def H1(text, num, key, styles, story):
    story.append(Paragraph(num, styles["h1num"]))
    story.append(SectionHeading(text, styles["h1"], 0, key))
    story.append(rule(CYAN, 2.2, 0, 13))


def H2(text, key, styles, story):
    story.append(SectionHeading(text, styles["h2"], 1, key))


def build_story(styles: dict, M: dict) -> list:
    story: list = []
    seg, chan, plan, reg = M["seg"], M["chan"], M["plan"], M["reg"]
    beh, drv, itv, segr = M["beh"], M["drv"], M["itv"], M["segr"]

    # ── COVER ──────────────────────────────────────────────
    story.append(Spacer(1, 4.4 * cm))
    story.append(P("RETENTION INTELLIGENCE REVIEW", styles, "cover_kick"))
    story.append(P("Churn &amp; Retention<br/>Intelligence", styles, "cover_title"))
    story.append(
        P(
            "Where the subscription book is losing customers and revenue, "
            "why those losses cluster where they do, and which accounts to "
            "defend first.",
            styles,
            "cover_sub",
        )
    )
    story.append(Spacer(1, 1.0 * cm))
    story.append(rule(colors.HexColor("#6D8490"), 0.6, 0, 12))
    story.append(
        P(
            f"Analytical period {M['window_start']} to {M['window_end']}  ·  "
            f"snapshot reference date {REFERENCE_DATE}<br/>"
            f"Book of {M['total']:,} lifetime accounts  ·  "
            f"{M['active']:,} active at snapshot  ·  "
            f"{usd(M['active_mrr'])} active monthly recurring revenue",
            styles,
            "cover_meta",
        )
    )
    story.append(Spacer(1, 7.85 * cm))
    story.append(rule(colors.HexColor("#6D8490"), 0.6, 0, 8))
    story.append(
        P(
            "Retention Analytics  ·  "
            "Companion deliverable to the self-contained executive dashboard "
            f"({code('executive-retention-command-center.html')}).<br/>"
            "Findings are decision-support only, not audited financial results.",
            styles,
            "cover_meta",
        )
    )
    story.append(NextPageTemplate("content"))
    story.append(PageBreak())

    # ── TABLE OF CONTENTS ──────────────────────────────────
    story.append(P("Contents", styles, "h1"))
    story.append(rule(CYAN, 2.2, 0, 14))
    toc = TableOfContents()
    toc.levelStyles = [styles["toc1"], styles["toc2"]]
    toc.dotsMinLevel = 0
    story.append(toc)
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════
    # 1. EXECUTIVE SUMMARY
    # ════════════════════════════════════════════════════════
    H1("Executive summary", "SECTION 01", "s1", styles, story)
    story.append(
        P(
            f"The book carries {M['total']:,} accounts. As of the {REFERENCE_DATE} "
            f"snapshot, {M['churned']:,} have churned and {M['active']:,} remain active, "
            f"billing {usd(M['active_mrr'])} of monthly recurring revenue. The headline "
            f"customer churn rate is {M['cum_churn_pct']:.1f} percent; the monthly-value "
            f"loss share is materially lower at {M['cum_revenue_loss_pct']:.1f} percent. "
            "That spread is the story. The book is losing too many logos, but the loss "
            "is skewed toward lower-value accounts. The response should therefore be "
            "selective, not universal.",
            styles,
        )
    )

    story.append(
        stat_band(
            [
                (f"{M['cum_churn_pct']:.1f}%", "CUMULATIVE CUSTOMER CHURN"),
                (f"{M['cum_revenue_loss_pct']:.1f}%", "CUMULATIVE REVENUE-LOSS SHARE"),
                (f"{M['high_n'] + M['crit_n']:,}", "CRITICAL + HIGH-RISK ACCOUNTS"),
                (usd(itv["mrr_exposure_proxy"].sum(), k=True), "WEIGHTED MRR EXPOSURE"),
            ],
            styles,
        )
    )

    story.append(
        P(
            "Management has two jobs. First, protect the open accounts where behavioural "
            f"risk and revenue matter: {M['high_n'] + M['crit_n']:,} named critical and "
            f"high-risk accounts holding {usd(M['crit_mrr'] + M['high_mrr'])} of MRR. "
            "Second, reduce the inflow of fragile demand. Affiliate and Paid Search "
            f"generate {M['weak_channel_acct_share']:.1f} percent of all accounts but "
            f"{M['weak_channel_churn_share']:.1f} percent of churned accounts. Customer "
            "Success can work the first problem. Revenue leadership owns the second.",
            styles,
        )
    )

    story.append(
        pull_quote(
            "Customer Success can work the first problem. Revenue leadership owns the second.",
            styles,
        )
    )

    story.append(
        P(
            "Three findings drive the recommendation. First, churn is concentrated in "
            "the low-commitment end of the book. Startup accounts churn at "
            f"{seg.loc['Startup', 'cumulative_churn_share'] * 100:.1f} percent versus "
            f"{seg.loc['Enterprise', 'cumulative_churn_share'] * 100:.1f} percent for "
            "Enterprise. Affiliate-sourced accounts churn at "
            f"{chan.loc['Affiliate', 'cumulative_churn_share'] * 100:.1f} percent versus "
            f"{chan.loc['Partner', 'cumulative_churn_share'] * 100:.1f} percent for "
            "Partner. Basic-plan accounts churn at "
            f"{plan.loc['Basic', 'cumulative_churn_share'] * 100:.1f} percent versus "
            f"{plan.loc['Enterprise', 'cumulative_churn_share'] * 100:.1f} percent for "
            "Enterprise plans. These descriptive cuts make acquisition mix and entry-tier "
            "economics the first commercial hypotheses to test; they do not isolate "
            "causal effects.",
            styles,
        )
    )

    story.append(
        P(
            "Second, pre-churn account health separates leavers from stayers with "
            "commercially useful clarity. Accounts carrying a low net promoter score "
            "churn at "
            f"{beh.loc['low_nps_flag', 'churn_rate_in_group'] * 100:.0f} percent against "
            f"{beh.loc['low_nps_flag', 'churn_rate_out_group'] * 100:.0f} percent for "
            "accounts without that flag. A heavy recent support burden lifts churn to "
            f"{beh.loc['high_support_ticket_flag', 'churn_rate_in_group'] * 100:.0f} "
            "percent. Failed payments, weak feature adoption, and declining usage each "
            "mark out populations that leave at several times the book rate. These "
            "signals surface before the account closes. That is what makes them useful "
            "for prevention, not just for post-mortem accounting.",
            styles,
        )
    )

    story.append(
        P(
            "Third, the revenue exposure is concentrated enough for named-account "
            f"management. The top 5 percent of churned accounts by value account for "
            f"{M['conc5']:.0f} percent of lost monthly revenue; the top 20 percent "
            f"account for {M['conc20']:.0f} percent. On the forward book, the critical "
            f"and high tiers hold {usd(M['crit_mrr'] + M['high_mrr'])} of MRR. Four "
            "intervention queues cover the material exposure: Renewal Save Desk, Payment "
            "Rescue, Adoption Reactivation, and Service Recovery. Sequence them by "
            "weighted exposure; payment rescue can run in parallel as a distinct billing "
            "workflow.",
            styles,
        )
    )

    story.append(
        data_table(
            ["Executive decision", "Evidence", "Required move"],
            [
                [
                    "Do not run a base-wide save campaign",
                    f"{M['high_n'] + M['crit_n']:,} high/critical accounts hold "
                    f"{usd(M['crit_mrr'] + M['high_mrr'])} of MRR.",
                    "Assign named owners to the high-risk queue and protect capacity from "
                    "low-signal accounts.",
                ],
                [
                    "Treat acquisition mix as a retention lever",
                    f"Affiliate + Paid Search: {M['weak_channel_acct_share']:.1f}% of "
                    f"accounts, {M['weak_channel_churn_share']:.1f}% of churned accounts.",
                    "Test a marginal channel-mix shift and retain it only if live cohort "
                    "retention improves without weakening acquisition economics.",
                ],
                [
                    "Separate human and automated motions",
                    f"{M['multi_signal_active']:,} active accounts carry multiple distress "
                    f"signals; {M['zero_signal_active']:,} carry none.",
                    "Use human outreach for multi-signal value accounts; use scaled nudges "
                    "for single-signal usage decline.",
                ],
                [
                    "Govern by outcomes, not activity",
                    f"Intervention queues cover {usd(M['play_scope_mrr'])} of MRR in scope.",
                    "Track saved MRR versus control, recovered payment MRR, and 6-month "
                    "retention by source.",
                ],
            ],
            styles,
            [4.0 * cm, 5.5 * cm, CONTENT_W - 9.5 * cm],
        )
    )

    story.append(
        fig(
            "intervention_priorities.png",
            "Figure 1. Retention plays ranked by weighted MRR exposure. The "
            "shaded bar is the monthly revenue in scope for each play; the "
            "solid bar is that revenue weighted by the historical churn rate "
            "of the accounts it targets.",
            styles,
        )
    )

    story.append(
        P(
            "The sequence follows weighted exposure and operational separability. Stand "
            "up the renewal save desk first: "
            "it carries the largest weighted exposure and the accounts are identifiable "
            "now. Fix the payment-failure path at the same time: the queue is small, the "
            "implementation is low-complexity, and affected accounts churn at more than "
            f"{floor_multiple(beh.loc['failed_payment_flag', 'churn_rate_lift'])} times the "
            "rate of accounts that pay cleanly. Next, test whether a marginal channel-mix "
            "shift away from Affiliate and Paid Search improves retention after controlling "
            "for segment, plan, and acquisition economics.",
            styles,
        )
    )

    story.append(
        P(
            "The weekly operating view should "
            "track new critical and high-risk accounts, payment recoveries, usage-trend "
            "recoveries, and queue aging. The monthly executive view should track two "
            "portfolio levers: lower weighted exposure in the open-account queue and a "
            "lower weak-channel share in new cohorts. Held-out outcomes, not activity "
            "volume, should determine whether either intervention scales.",
            styles,
        )
    )

    story.append(
        P(
            "One caveat governs everything that follows. This analysis runs on "
            "deterministic synthetic data. The transparent policy score prioritises "
            "operations, while a separate calibrated model estimates 90-day churn "
            "probability on held-out time periods. Both are internally consistent; their "
            "magnitudes remain illustrative until live outcomes replace the simulation. "
            "Section 10 states the limitations in full.",
            styles,
        )
    )
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════
    # 2. CONTEXT AND OBJECTIVES
    # ════════════════════════════════════════════════════════
    H1("Context and objectives", "SECTION 02", "s2", styles, story)
    story.append(
        P(
            "This review gives revenue leadership and customer success a common retention "
            "baseline: the scale and timing of loss, the populations where it concentrates, "
            "the observable signals that precede it, and a value-adjusted queue of open "
            "accounts. A reconciled MRR movement ledger, acquisition spend, direct service "
            "cost, calibrated probabilities, and randomized holdouts extend that baseline "
            "into unit economics and controlled measurement. The values remain analytical "
            "proxies rather than audited accounts or observed treatment effects.",
            styles,
        )
    )

    H2("The book under review", "s2a", styles, story)
    story.append(
        P(
            f"The dataset is the complete account history of a business-to-business "
            f"software company: {M['total']:,} accounts acquired across four customer "
            "segments (Enterprise, Mid-Market, SMB, and Startup), four regions (North "
            "America, Europe, APAC, and LATAM), six acquisition channels, and four plan "
            f"tiers. Monthly activity runs from {M['window_start']} to {M['window_end']}. "
            f"At the snapshot date, {M['active']:,} accounts are active and "
            f"{M['churned']:,} have closed. The active book bills {usd(M['active_mrr'])} "
            "per month. Each account carries the behavioural signals a real customer "
            "success team would have in front of it: recent product usage and its trend, "
            "feature adoption, support ticket volume, net promoter score, and payment "
            "history.",
            styles,
        )
    )

    H2("The decisions this report supports", "s2b", styles, story)
    story.append(
        P(
            "This work answers five questions a retention owner has to make calls on. "
            "It does not produce a general description of the data:",
            styles,
        )
    )
    for q in [
        "<b>How is the book trending?</b> Are monthly customer and revenue churn "
        "rising, flat, or falling, and is the loss weighted toward high-value or "
        "low-value accounts?",
        "<b>Where does churn concentrate?</b> Which segments, plans, channels, and "
        "regions carry a disproportionate share of the loss, and are recent cohorts "
        "retaining better or worse than older ones?",
        "<b>What separates leavers from stayers?</b> Which observable account-health "
        "signals are most strongly associated with churn, and by how much do they "
        "lift the rate?",
        "<b>Where is revenue exposed?</b> How much current MRR sits behind each "
        "pattern, and is the loss concentrated in a small set of high-value accounts "
        "or spread thinly across many small ones?",
        "<b>What should be done first?</b> Which intervention queues carry the "
        "largest weighted exposure, and in what order should they be resourced?",
    ]:
        story.append(P("•&nbsp;&nbsp;" + q, styles))
    story.append(
        P(
            "Everything in the findings maps back to one of these five questions. Where "
            "the evidence is associative rather than causal, the report says so plainly "
            "instead of dressing a correlation as a cause.",
            styles,
        )
    )
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════
    # 3. DATA AND METHODOLOGY
    # ════════════════════════════════════════════════════════
    H1("Data and methodology", "SECTION 03", "s3", styles, story)
    story.append(
        P(
            "This section describes where the numbers come from and how each metric is "
            "defined, so that any figure in the report can be traced back to a rule "
            "rather than a judgement call. The pipeline is deterministic: it runs from a "
            "fixed seed, every artifact is reconstructible, and the analytical tables "
            "are governed by data contracts that are checked before any output is "
            "released.",
            styles,
        )
    )

    H2("Data sources", "s3a", styles, story)
    story.append(
        P(
            "Four raw tables feed the analysis. A customer table holds the acquisition "
            "attributes (segment, region, channel, signup date). A subscription table "
            "holds the plan, the monthly recurring revenue, and the start and end dates "
            "that define whether and when an account churned. A weekly product-usage "
            "table records sessions and feature interactions. A payment table records "
            "billing events and failures. These are joined into one per-account feature "
            "row measured either at the account's churn date or, for active accounts, at "
            "the snapshot reference date, so that every account is observed at a "
            "comparable point in its own life rather than at an arbitrary calendar date.",
            styles,
        )
    )

    story.append(
        data_table(
            ["Layer", "Artifact", "Grain", "Role in this report"],
            [
                [
                    "Raw",
                    "customers, subscriptions, usage, payments",
                    "event / account",
                    "Source of truth for all downstream tables",
                ],
                [
                    "Features",
                    code("customer_retention_features.csv"),
                    "one row per account",
                    "Behavioural signals at churn or snapshot",
                ],
                [
                    "Analysis",
                    code("main_analysis_*, churn_by_*"),
                    "aggregate",
                    "Trends, driver ranking, revenue at risk",
                ],
                [
                    "Risk",
                    code("customer_risk_scores.csv"),
                    "active account",
                    "Tiered priority queue and recommended action",
                ],
                [
                    "Cohort",
                    code("cohort_retention_table.csv"),
                    "cohort × month",
                    "Retention curves and heatmap",
                ],
            ],
            styles,
            [2.0 * cm, 6.1 * cm, 2.7 * cm, CONTENT_W - 10.8 * cm],
        )
    )

    H2("How the synthetic data is built", "s3b", styles, story)
    story.append(
        P(
            "The data is generated, not collected, and it matters that the reader knows "
            "what that means for the findings. The generator assigns each account "
            "a latent churn propensity driven by its segment, plan, channel, and a set of "
            "behavioural processes, then simulates usage, support, payment, and survey "
            "events consistent with that propensity. The result is a book in which the "
            "relationships between account health and churn are real and stable but "
            "designed, not discovered. The report treats the magnitudes as illustrative. "
            "What transfers to a real book is the metric definitions, the analytical "
            "framework, and the prioritisation logic.",
            styles,
        )
    )

    H2("Metric definitions", "s3c", styles, story)
    story.append(
        P(
            "Several numbers in this report look similar and are easily confused, so each "
            "is pinned to an explicit definition here and used consistently throughout.",
            styles,
        )
    )
    story.append(
        data_table(
            ["Metric", "Definition"],
            [
                [
                    "Cumulative customer churn",
                    "Closed accounts as a share of all accounts ever acquired. A lifetime, "
                    f"stock measure. Reads {M['cum_churn_pct']:.0f} percent here.",
                ],
                [
                    "Monthly customer churn rate",
                    "Accounts that close in a month divided by accounts active at the start "
                    "of that month. A flow measure, averaging "
                    f"{M['avg_cust_churn']:.1f} percent over the window.",
                ],
                [
                    "Cumulative churn share (by group)",
                    "Within a segment, plan, channel, or region, the share of that group's "
                    "accounts that have closed.",
                ],
                [
                    "MRR at risk",
                    "Current monthly recurring revenue billed by active accounts that carry "
                    "the at-risk flag at the snapshot.",
                ],
                [
                    "Churn rate lift",
                    "Churn rate inside a signal divided by churn rate outside it. A multiple, "
                    "not a percentage.",
                ],
                [
                    "Weighted MRR exposure",
                    "Revenue in an intervention queue multiplied by the historical churn rate "
                    "of the accounts it targets. A heuristic for sizing, not a forecast.",
                ],
                [
                    "Retention priority score",
                    "A transparent index combining behavioural risk and customer value, used "
                    "only to rank the queue.",
                ],
            ],
            styles,
            [4.6 * cm, CONTENT_W - 4.6 * cm],
        )
    )
    story.append(
        P(
            "Two of these deserve emphasis. Cumulative customer churn and the monthly "
            "churn rate are different lenses on the same book and will not match: the "
            "first asks what share of everyone ever signed has left, the second asks how "
            "fast the active book is losing accounts right now. Both appear in this "
            "report and neither is wrong. Revenue loss throughout uses a monthly-value "
            "proxy, the account's recurring monthly revenue at the point it left, rather "
            "than a full contract or lifetime value, which the synthetic data does not "
            "model.",
            styles,
        )
    )

    H2("Governance and release gates", "s3d", styles, story)
    story.append(
        P(
            "No figure in this report or the dashboard is released until the pipeline "
            "passes its governance gates. The artifacts are checked against data "
            "contracts that assert the shape, types, ranges, and referential integrity "
            "of every table, and against a final validation suite covering data quality, "
            "metric correctness, analytical integrity, the dashboard render, and release "
            f"policy. At the current build, all {M['gov_pass']} of {M['gov_total']} checks "
            "pass. That is what licenses decision-support claims instead of "
            "screening-grade ones. The gate breakdown follows. The full check log lives "
            f"in {code('outputs/tables')} and regenerates on every run.",
            styles,
        )
    )
    story.append(
        data_table(
            ["Validation gate", "Checks passing"],
            M["gov"],
            styles,
            [CONTENT_W - 4.5 * cm, 4.5 * cm],
        )
    )
    story.append(
        P(
            "The release policy is deliberately conservative about what these passes "
            "entitle the analysis to claim. Clearing every gate makes the output "
            "technically valid and analytically acceptable for decision support with "
            "explicit caveats. It does not make it committee-grade, because the "
            "synthetic-data limitation in Section 10 is an unresolved caveat by "
            "construction. The governance layer encodes that distinction rather than "
            "leaving it to the reader's goodwill.",
            styles,
        )
    )

    H2("Evidence standard applied in the body", "s3e", styles, story)
    story.append(
        P(
            "The body of the report uses three levels of evidence. Descriptive cuts are "
            "decision-useful when the same pattern repeats across segment, plan, channel, "
            "and value. Behavioural signals are decision-useful when they separate "
            "leavers from stayers and also identify active accounts that can still be "
            "worked. Recommendations are decision-useful only when they combine both: a "
            "population the business can identify, a revenue stake large enough to "
            "matter, and a metric that can validate whether the intervention changed "
            "outcomes rather than activity.",
            styles,
        )
    )
    story.append(
        data_table(
            ["Evidence question", "Report test", "What would weaken the read"],
            [
                [
                    "Is the pattern real?",
                    "The finding repeats across rate, revenue, and mix cuts rather than "
                    "appearing in one chart only.",
                    "A single small segment, a right-censored period, or a mix effect that "
                    "disappears when read at fixed cohort age.",
                ],
                [
                    "Is the pattern actionable?",
                    "The affected accounts can be named before churn and routed to a clear "
                    "commercial owner.",
                    "A signal that is visible only after churn, or a population too broad "
                    "for the team to treat differently.",
                ],
                [
                    "Is the value material?",
                    "The queue carries current MRR or lost monthly value large enough to "
                    "change prioritisation.",
                    "High churn rate with negligible account count or revenue scope.",
                ],
                [
                    "Can the action be validated?",
                    "The recommendation names a success measure and admits where a control "
                    "or holdout is needed.",
                    "ROI asserted from historical association without an experiment or "
                    "before/after design.",
                ],
            ],
            styles,
            [3.8 * cm, 6.4 * cm, CONTENT_W - 10.2 * cm],
        )
    )
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════
    # 4. ANALYTICAL FRAMEWORK
    # ════════════════════════════════════════════════════════
    H1("Analytical framework", "SECTION 04", "s4", styles, story)
    story.append(
        P(
            "The analysis moves through six lenses, each narrowing the question. The "
            "trend shows whether the book is getting better or worse in aggregate. "
            "Decomposing the loss by commercial attribute and by acquisition cohort "
            "shows where it lives. Account-level behaviour reveals what the leaving "
            "accounts had in common before they left. Attaching revenue and a priority "
            "score to each pattern turns the diagnosis into a ranked operating queue. A "
            "reconciled MRR bridge adds retention-adjusted economics; a calibrated model, "
            "randomized holdout, and transition monitor add prediction, measurement, and "
            "operational control.",
            styles,
        )
    )

    H2("Associative, not causal", "s4a", styles, story)
    story.append(
        P(
            "Every relationship in this report is associative. When the report says that "
            "accounts with a low net promoter score churn at a far higher rate, it means "
            "the two co-occur in the data, not that a low score causes the departure. The "
            "low score and the departure may both be downstream of a third thing, a poor "
            "onboarding or a product gap, and the data here cannot separate those. "
            "Establishing cause requires controlled experiments, holding out a population "
            "from an intervention and measuring the difference. The reference pipeline "
            "implements that assignment and estimator, but its forward outcomes remain "
            "simulated. The driver ranking tells the operator what to test, weighted "
            "exposure sets priority, and live holdout outcomes must settle efficacy.",
            styles,
        )
    )

    story.append(
        P(
            "The decomposition step is not cosmetic. A single book-wide churn rate can "
            "move in the opposite direction to every one of its parts if the mix of "
            "those parts is shifting, the trap known as Simpson's paradox. A business "
            "whose every segment is improving can post a worsening aggregate simply by "
            "acquiring more of its weakest segment. Cutting the rate by segment, plan, "
            "channel, and region guards against reading a mix shift as a performance "
            "change, and it is why the cohort and concentration findings are presented "
            "next to the aggregate trend rather than in place of it.",
            styles,
        )
    )

    H2("The risk index", "s4b", styles, story)
    story.append(
        P(
            "Active accounts are scored on two axes and ranked on their product. The "
            "first axis is behavioural risk, built from the same pre-churn signals "
            "examined in Section 08: usage trend, feature adoption, support burden, net "
            "promoter score, and payment failures. The second axis is customer value, "
            "built from current monthly revenue and tenure. An account is worth working "
            "when it scores high on both, a high-value account showing real distress, "
            f"rather than on either alone. The mean risk score across the {M['scored']:,} "
            "scored accounts is "
            f"{M['risk_mean']:.1f} on a 0-to-100 scale, which is deliberately low: most "
            "of the book carries no material distress signal, and the index exists to "
            "surface the minority that does. Accounts fall into four tiers, critical, "
            "high, medium, and low, and each carries a single recommended action so the "
            "output is a worklist rather than a chart.",
            styles,
        )
    )
    story.append(
        P(
            "The index is transparent by design. Every input is a named, inspectable "
            "signal and the combination rule is a documented formula, not a black-box "
            "model. An operator can inspect why an account surfaced and challenge the "
            "underlying signal. Predictive accuracy has not been estimated here, so the "
            "index cannot be compared with a trained classifier until both are tested "
            "on time-separated outcomes.",
            styles,
        )
    )
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════
    # 5. FINDINGS — RETENTION HEALTH AND TREND
    # ════════════════════════════════════════════════════════
    H1("Findings: retention health and trend", "SECTION 05", "s5", styles, story)
    story.append(
        P(
            f"Across the analytical window the book lost an average of "
            f"{M['avg_cust_churn']:.1f} percent of its active accounts per month and "
            f"{M['avg_rev_churn']:.1f} percent of its active revenue per month. Revenue "
            "churn running below customer churn is the important signal: the average "
            "departing account is worth less than the average retained account. The book "
            "is losing the low end faster than the revenue spine. That is a better loss "
            "shape than the reverse, but it still creates avoidable acquisition demand. "
            "The later sections locate that low-end pressure.",
            styles,
        )
    )

    story.append(
        fig(
            "churn_rate_trend.png",
            "Figure 2. Monthly customer and revenue churn rates across the "
            "analytical window. The revenue line sits consistently below the "
            "customer line, the signature of low-value accounts leading the "
            "losses.",
            styles,
        )
    )

    story.append(
        P(
            "The average level is manageable; the recent direction needs attention. "
            "Monthly customer "
            f"churn in the final observed month reads {M['last_cust_churn']:.1f} percent "
            f"against a prior nine-month average of {M['prior9_churn']:.1f} percent. The "
            "last three months sit above the trend that preceded them. Two readings are "
            "possible, and both matter. Operationally, the most recent quarter may be "
            "losing accounts faster than the settled baseline. From a measurement "
            "perspective, accounts closing near the snapshot date are mechanically "
            "over-represented because the window is right-censored, so the final months "
            "are not fully comparable to settled history. Section 10 returns to this. "
            "The right posture is weekly monitoring, not a broad emergency response.",
            styles,
        )
    )

    story.append(
        P(
            f"The quantified acceleration is material on both count and value. The final "
            f"three observed months average {M['last3_churn']:.1f} percent customer churn, "
            f"{M['last3_churn_delta_pp']:.1f} percentage points above the prior nine "
            f"months. Revenue churn averages {M['last3_rev_churn']:.1f} percent, "
            f"{M['last3_rev_churn_delta_pp']:.1f} points above its prior nine-month "
            "baseline. Because the revenue increase is smaller than the customer-count "
            "increase, the recent spike is still dominated by lower-value accounts. That "
            "keeps the response focused: monitor the spike weekly, but do not let it "
            "pull scarce save-desk capacity away from the high-value account tail.",
            styles,
        )
    )

    story.append(
        fig(
            "recent_churn_acceleration.png",
            "Figure 3. Monthly customer churn rate over the last twelve "
            "months. The final three months (highlighted) sit well above the "
            "prior nine-month average shown by the dotted line.",
            styles,
            width_frac=0.82,
        )
    )

    story.append(
        P(
            "Underneath the rates, the active book has grown steadily in revenue terms "
            f"to {usd(M['active_mrr'])} per month at the snapshot, so the business is "
            "adding more than it is losing. That makes this a question of efficiency, "
            "not survival: every point of avoidable churn is a point the acquisition "
            "engine has to refill before the book grows, and the next sections show "
            "that a large share of the current loss is concentrated in places where "
            "management has practical levers.",
            styles,
        )
    )

    story.append(
        fig(
            "mrr_trend.png",
            "Figure 4. Active monthly recurring revenue by month. The book "
            "has compounded steadily; the retention task is to protect the "
            "base, not to rescue a business in decline.",
            styles,
            width_frac=0.82,
        )
    )

    H2("The shape of the loss", "s5a", styles, story)
    story.append(
        P(
            f"The gap between customer churn ({M['avg_cust_churn']:.1f} percent a month "
            f"on average) and revenue churn ({M['avg_rev_churn']:.1f} percent) is worth "
            "reading deliberately, because its sign decides the whole posture of a "
            "retention programme. When revenue churn runs below customer churn, the "
            "average departing account is worth less than the average retained one, and "
            "the book is upgrading its mix even as it loses count. That is the case "
            "here. The dangerous pattern is the reverse: a few large accounts leave, "
            "customer churn stays low, and revenue churn spikes. That pattern would "
            "demand immediate top-account defence. This book calls for a more segmented "
            "operating model: scaled treatment for the low-value tail, named-account "
            "coverage for high-value exceptions.",
            styles,
        )
    )
    story.append(
        P(
            "The gap does not establish unit economics. This dataset has no acquisition "
            "cost, gross margin, expansion, contraction, or remaining contract value, so "
            "it cannot estimate lifetime value or the economic benefit of retaining a "
            "specific account. It does show why logo and revenue retention must be read "
            "together: a stable revenue view can coexist with a material customer-count "
            "loss. The cohort and concentration findings that follow locate where that "
            "difference sits in the book.",
            styles,
        )
    )
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════
    # 6. FINDINGS — COHORT RETENTION
    # ════════════════════════════════════════════════════════
    H1("Findings: cohort retention", "SECTION 06", "s6", styles, story)
    story.append(
        P(
            "Aggregate churn rates hide whether the business is getting better at keeping "
            "the customers it acquires. Cohort analysis answers that by following each "
            "monthly intake of new accounts across its own life and comparing intakes at "
            "the same age. A fixed-age gap identifies a change in cohort quality; it does "
            "not distinguish acquisition mix, onboarding, product fit, or random variation "
            "without additional evidence.",
            styles,
        )
    )

    story.append(
        fig(
            "cohort_retention_curves.png",
            "Figure 5. Retention by cohort age for the eight most recent "
            "monthly cohorts. Each line follows one acquisition month as its "
            "accounts age; the most recent cohorts are drawn most prominently.",
            styles,
        )
    )

    story.append(
        P(
            "Read at a fixed age, the curves bend the wrong way. Early cohorts hold "
            "almost all of their accounts through the first half-year. More recent "
            "cohorts start to separate downward at the same ages. The deterioration is "
            "modest in absolute terms, six-month retention still reads near 98 to 99 "
            "percent. The most recent cohorts are right-censored: they "
            "have not yet lived long enough to be compared fairly at the longer horizons, "
            "so the long-run gap should be read as a leading indicator to monitor rather "
            "than a settled fact.",
            styles,
        )
    )

    story.append(
        fig(
            "cohort_retention_heatmap.png",
            "Figure 6. Cohort retention heatmap. Each row is an acquisition "
            "month, each column a month of age, and each cell the share of "
            "the original cohort still active. Green is higher retention, "
            "red is loss; the gradient down the early columns is where "
            "onboarding quality shows up.",
            styles,
        )
    )

    story.append(
        P(
            "The heatmap makes the pattern legible at a glance. The strongest retention "
            "sits in the upper rows, the older cohorts, and the early-age columns. As the "
            "eye moves down toward the recent cohorts, the early-age cells lose a little "
            "of their green. This is the same story the curves tell, shown as a surface. "
            "The chart supports monitoring a possible cohort-quality shift; it does not "
            "diagnose acquisition mix or onboarding as the cause. That question connects "
            "to the channel and "
            "segment findings in the next section.",
            styles,
        )
    )
    story.append(
        P(
            "The fixed-age read is the stricter test. It compares cohorts at the same "
            "age, so it does not penalise recent cohorts for not yet having long lives. "
            "On that basis the signal is not a single noisy month: recent cohorts trail "
            "early cohorts at month three, six, nine, and twelve. The logo gap is larger "
            "than the revenue gap at every age, confirming that the deterioration is "
            "concentrated in smaller accounts. The table below is why this pattern "
            "counts as a real operating signal, not yet a settled long-run outcome.",
            styles,
        )
    )
    story.append(
        data_table(
            [
                "Age",
                "Early logo ret.",
                "Recent logo ret.",
                "Logo delta",
                "Early rev. ret.",
                "Recent rev. ret.",
                "Rev. delta",
            ],
            [
                [
                    f"{age}m",
                    f"{M['cohort_deltas'][age]['early_logo']:.1f}%",
                    f"{M['cohort_deltas'][age]['recent_logo']:.1f}%",
                    f"{M['cohort_deltas'][age]['logo_delta']:.1f} pp",
                    f"{M['cohort_deltas'][age]['early_rev']:.1f}%",
                    f"{M['cohort_deltas'][age]['recent_rev']:.1f}%",
                    f"{M['cohort_deltas'][age]['rev_delta']:.1f} pp",
                ]
                for age in [3, 6, 9, 12]
            ],
            styles,
            [1.4 * cm, 2.5 * cm, 2.7 * cm, 2.2 * cm, 2.5 * cm, 2.7 * cm, CONTENT_W - 14.0 * cm],
        )
    )
    story.append(
        P(
            "One distinction matters for how this finding is used. Logo retention, the "
            "share of accounts still active, deteriorates a little faster than revenue "
            "retention, the share of original cohort revenue still billing, because the "
            "accounts leaving the recent cohorts are disproportionately the smaller ones. "
            "Six-month revenue retention holds near 99 percent even where logo retention "
            "slips below it. That is the same favourable shape seen in the aggregate "
            "trend. The cohort drift is a problem of small-account volume, not an "
            "erosion of the cohorts' revenue spine. The fix belongs upstream in "
            "acquisition and onboarding, not in a high-touch save motion aimed at the "
            "recent cohorts.",
            styles,
        )
    )
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════
    # 7. FINDINGS — WHERE CHURN CONCENTRATES
    # ════════════════════════════════════════════════════════
    H1("Findings: where churn concentrates", "SECTION 07", "s7", styles, story)
    story.append(
        P(
            "This section turns the aggregate churn rate into an operating target. The "
            "book-wide rate averages together populations with very different economics, "
            "purchase intent, and support needs. Segment, plan, channel, and region cuts "
            "show where the leaving happens and whether the same weak populations appear "
            "from multiple angles.",
            styles,
        )
    )

    H2("By customer segment", "s7a", styles, story)
    story.append(
        P(
            f"Segment is the sharpest single cut. Startup accounts have lost "
            f"{seg.loc['Startup', 'cumulative_churn_share'] * 100:.1f} percent of their "
            f"number and SMB accounts {seg.loc['SMB', 'cumulative_churn_share'] * 100:.1f} "
            f"percent, against {seg.loc['Mid-Market', 'cumulative_churn_share'] * 100:.1f} "
            f"percent for Mid-Market and just "
            f"{seg.loc['Enterprise', 'cumulative_churn_share'] * 100:.1f} percent for "
            "Enterprise. The gradient is monotonic in account size: the smaller the "
            "customer, the more likely it is to leave. The same gradient runs through "
            "average revenue, where Enterprise accounts bill "
            f"{usd(seg.loc['Enterprise', 'avg_monthly_revenue'])} a month against "
            f"{usd(seg.loc['Startup', 'avg_monthly_revenue'])} for Startup, and through "
            "the health signals examined below. Small accounts churn more and are worth "
            "less individually. That is why the revenue line sits below the customer "
            "line in the trend.",
            styles,
        )
    )

    story.append(
        fig(
            "churn_by_segment.png",
            "Figure 7. Cumulative churn share by customer segment. The "
            "gradient is monotonic in account size, from Enterprise at the "
            "low end to Startup at the high end.",
            styles,
            width_frac=0.92,
        )
    )

    story.append(
        P(
            "The segment story goes beyond churn rates. It is about the whole health "
            "profile. The comparison below puts cumulative churn next to average net "
            "promoter score and the average usage trend for each segment. Enterprise "
            "accounts churn least, score highest on net promoter, and show usage that is "
            "still growing. Startup accounts invert all three. The signals move together "
            "rather than firing independently. That is what makes a single "
            "segment-level health read meaningful.",
            styles,
        )
    )

    story.append(
        fig(
            "segment_health_comparison.png",
            "Figure 8. Segment health on three axes: cumulative churn share, "
            "average net promoter score, and average usage trend. A segment "
            "that is losing accounts is also the segment with weak sentiment "
            "and declining usage.",
            styles,
        )
    )

    H2("By plan type", "s7b", styles, story)
    story.append(
        P(
            f"Plan tier tells the same story in pricing terms. Basic-plan accounts churn "
            f"at {plan.loc['Basic', 'cumulative_churn_share'] * 100:.1f} percent, nearly ten "
            f"times the {plan.loc['Enterprise', 'cumulative_churn_share'] * 100:.1f} percent "
            "rate of Enterprise-plan accounts, with Growth and Pro stepping down in "
            "between. Lower-priced plans behave like lower-commitment purchases in this "
            "book. The commercial implication is not to abandon the entry tier: it feeds "
            "the funnel. It is to treat a Basic-plan signup as a different operating "
            "asset from a Pro-plan signup, with a faster activation path and earlier "
            "risk monitoring.",
            styles,
        )
    )

    story.append(
        fig(
            "churn_by_plan.png",
            "Figure 9. Cumulative churn share by plan type. Churn falls "
            "steeply as plan tier rises.",
            styles,
            width_frac=0.92,
        )
    )

    H2("By acquisition channel", "s7c", styles, story)
    story.append(
        P(
            f"Channel is the clearest link between the retention read and spend decisions. "
            f"Affiliate-sourced accounts churn at "
            f"{chan.loc['Affiliate', 'cumulative_churn_share'] * 100:.1f} percent and "
            f"Paid Search at {chan.loc['Paid Search', 'cumulative_churn_share'] * 100:.1f} "
            f"percent, against {chan.loc['Partner', 'cumulative_churn_share'] * 100:.1f} "
            f"percent for Partner and "
            f"{chan.loc['Referral', 'cumulative_churn_share'] * 100:.1f} percent for "
            "Referral. Paid and affiliate demand is less durable than partner and "
            "referral demand in this run. The next step is a live cohort test that controls "
            "for segment, plan, CAC, and volume before changing channel budgets.",
            styles,
        )
    )

    story.append(
        fig(
            "churn_by_channel.png",
            "Figure 10. Cumulative churn share by acquisition channel. "
            "Paid and affiliate channels (red) deliver the least durable "
            "accounts; partner and referral channels (green) the most "
            "durable.",
            styles,
            width_frac=0.92,
        )
    )

    H2("By region", "s7d", styles, story)
    story.append(
        P(
            f"Region is the weakest of the four cuts but not a flat one. LATAM accounts "
            f"churn at {reg.loc['LATAM', 'cumulative_churn_share'] * 100:.1f} percent and "
            f"APAC at {reg.loc['APAC', 'cumulative_churn_share'] * 100:.1f} percent, both "
            f"above the {M['cum_churn_pct']:.1f} percent book average, while North "
            f"America sits below it at "
            f"{reg.loc['North America', 'cumulative_churn_share'] * 100:.1f} percent. The "
            "spread is narrower than for segment or channel, and a good part of it "
            "probably reflects which segments and channels dominate each region rather "
            "than geography itself. Region is therefore a place to watch and to read "
            "alongside the other cuts, not a primary lever on its own.",
            styles,
        )
    )

    story.append(
        fig(
            "churn_by_region.png",
            "Figure 11. Cumulative churn share by region against the book "
            "average. LATAM and APAC run hot; North America runs cool.",
            styles,
            width_frac=0.92,
        )
    )

    story.append(
        P(
            "Taken together, the four cuts describe an overlapping population rather "
            "than four unrelated problems. The accounts that leave are smaller, sit on "
            "cheaper plans, arrive more often through paid and affiliate channels, and "
            "are somewhat more concentrated outside North America. That overlap explains "
            "why revenue churn sits below customer churn and why recent cohorts have "
            "started to drift. The next section asks what those accounts looked like in "
            "the weeks before they left.",
            styles,
        )
    )
    story.append(
        P(
            "One word of caution carries over from the framework in Section 04. Because "
            "the four cuts overlap, they cannot simply be added. Affiliate traffic skews "
            "toward Startup and SMB accounts on Basic plans, so part of the channel "
            "effect is really the segment and plan effect wearing a channel label, and "
            "part of the regional spread is which segments and channels dominate each "
            "region. Separating the independent contribution of each attribute would "
            "take a model that holds the others constant. Budget reallocation should wait "
            "for that multivariate read and a controlled channel test.",
            styles,
        )
    )
    story.append(
        P(
            "The mix test below shows why this is still an executive issue rather than a "
            "statistical footnote. The weak commercial populations contribute more churn "
            "than their account share would imply, while the durable populations do the "
            "opposite. This is a screening signal for an upstream mix problem, not proof "
            "that channel or segment independently causes the difference.",
            styles,
        )
    )
    story.append(
        data_table(
            ["Population", "Account share", "Churn share", "Read"],
            [
                [
                    "Startup + SMB",
                    f"{M['low_end_acct_share']:.1f}%",
                    f"{M['low_end_churn_share']:.1f}%",
                    "Low-end segments over-index in the churn pool.",
                ],
                [
                    "Mid-Market + Enterprise",
                    f"{M['durable_acct_share']:.1f}%",
                    f"{M['durable_churn_share']:.1f}%",
                    "Durable segments under-index despite higher value.",
                ],
                [
                    "Basic + Growth plans",
                    f"{M['low_plan_acct_share']:.1f}%",
                    f"{M['low_plan_churn_share']:.1f}%",
                    "Entry and mid tiers carry most logo churn.",
                ],
                [
                    "Pro + Enterprise plans",
                    f"{M['premium_plan_acct_share']:.1f}%",
                    f"{M['premium_plan_churn_share']:.1f}%",
                    "Premium tiers are the revenue spine to defend.",
                ],
                [
                    "Affiliate + Paid Search",
                    f"{M['weak_channel_acct_share']:.1f}%",
                    f"{M['weak_channel_churn_share']:.1f}%",
                    "Paid-intent sources bring disproportionate churn.",
                ],
                [
                    "Partner + Referral",
                    f"{M['quality_channel_acct_share']:.1f}%",
                    f"{M['quality_channel_churn_share']:.1f}%",
                    "Trust-led sources bring more durable accounts.",
                ],
            ],
            styles,
            [4.6 * cm, 2.5 * cm, 2.4 * cm, CONTENT_W - 9.5 * cm],
        )
    )
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════
    # 8. FINDINGS — BEHAVIOURAL CHURN DRIVERS
    # ════════════════════════════════════════════════════════
    H1("Findings: behavioural churn drivers", "SECTION 08", "s8", styles, story)
    story.append(
        P(
            "Commercial attributes say which accounts are structurally fragile. "
            "Behavioural signals say which accounts show current distress. The separation "
            "is wider than a live operating dataset would usually produce, which is a "
            "synthetic-data caveat, but the signals themselves are practical: recent net "
            "promoter score, support tickets, failed payments, feature adoption, and "
            "usage trend.",
            styles,
        )
    )

    story.append(
        fig(
            "signal_separation.png",
            "Figure 12. Churn rate for accounts with and without each "
            "pre-churn signal. The gap between the two dots is the decision "
            "signal: these flags split the book into populations with very "
            "different observed churn rates.",
            styles,
        )
    )

    story.append(
        P(
            f"The separation is stark. Accounts carrying a low net promoter score churn "
            f"at {beh.loc['low_nps_flag', 'churn_rate_in_group'] * 100:.0f} percent against "
            f"{beh.loc['low_nps_flag', 'churn_rate_out_group'] * 100:.0f} percent for "
            "accounts without it. Accounts with a heavy support burden churn at "
            f"{beh.loc['high_support_ticket_flag', 'churn_rate_in_group'] * 100:.0f} percent "
            f"against {beh.loc['high_support_ticket_flag', 'churn_rate_out_group'] * 100:.0f} "
            "percent. Failed payments mark a population that churns at "
            f"{beh.loc['failed_payment_flag', 'churn_rate_in_group'] * 100:.0f} percent, "
            "weak feature adoption "
            f"{beh.loc['low_feature_adoption_flag', 'churn_rate_in_group'] * 100:.0f} "
            "percent, and declining usage "
            f"{beh.loc['usage_decline_flag', 'churn_rate_in_group'] * 100:.0f} percent. "
            "Expressed as a multiple, a low net promoter score lifts the churn rate by "
            f"more than {floor_multiple(beh.loc['low_nps_flag', 'churn_rate_lift'])} times and "
            f"a heavy support burden by more than "
            f"{floor_multiple(beh.loc['high_support_ticket_flag', 'churn_rate_lift'])} times relative "
            "to accounts without the flag.",
            styles,
        )
    )

    story.append(
        P(
            "These multiples are large partly because the comparison group, accounts "
            "without any distress flag, almost never churns in this book. That is a "
            "property of synthetic data and the report does not lean on the exact "
            "magnitudes. What survives the caveat is the ranking and the practical "
            "consequence: sentiment and support signals identify a small, high-risk "
            "population, while usage decline identifies a larger, lower-certainty "
            "population. The operating response should match that difference: human save "
            "motions for high-value accounts with severe signals, scaled nudges for the "
            "broad usage-decline tail.",
            styles,
        )
    )

    story.append(
        P(
            "Expressed as a multiple against accounts without each flag, the same five "
            "signals rank in a stable order. Low sentiment and high support burden sit "
            "far out at the top, payment failure in the middle, and weak adoption and "
            "usage decline closer to the no-lift line, though still well above it. The "
            "lift view and the absolute-separation view agree on the ranking and "
            "disagree only on emphasis. That makes the ordering more credible: it is not "
            "an artifact of one charting choice.",
            styles,
        )
    )
    story.append(
        fig(
            "behavioral_churn_drivers.png",
            "Figure 13. Churn rate lift by behavioural signal, the ratio of "
            "the churn rate inside each signal to the rate outside it. Bars "
            "past the dotted no-lift line mark signals that raise the rate; "
            "the strongest sit several times above it.",
            styles,
            width_frac=0.95,
        )
    )

    H2("Ranking the drivers by money, not just by rate", "s8a", styles, story)
    story.append(
        P(
            "A high lift on a tiny population is less important than a moderate lift on a "
            "large and valuable one. The ranking below therefore scores each driver by "
            "its excess MRR association: the monthly revenue tied to churned accounts "
            "carrying the signal, above what the book-average churn rate would predict. "
            "This reorders the list. Support burden and low sentiment stay near the top "
            "because they combine a high rate with real revenue. Usage decline rises "
            "sharply for a different reason: its lift is the smallest of the five "
            "behavioural signals, but it touches the most accounts and the most money.",
            styles,
        )
    )

    story.append(
        fig(
            "churn_driver_ranking.png",
            "Figure 14. Churn drivers ranked by excess MRR association. "
            "Behavioural signals (red) dominate the structural attributes "
            "(slate); the label shows each driver's churn-rate multiple.",
            styles,
        )
    )

    drv_top = drv.sort_values("excess_mrr_association_proxy", ascending=False).head(5)
    story.append(
        data_table(
            ["Driver", "Accounts", "Churn rate", "Lift", "Excess MRR assoc."],
            [
                [
                    r["driver"]
                    .replace("_flag", "")
                    .replace("_", " ")
                    .title()
                    .replace("Nps", "NPS"),
                    f"{int(r['impacted_customers']):,}",
                    f"{r['churn_rate'] * 100:.0f}%",
                    f"{r['churn_rate_lift']:.1f}×",
                    usd(r["excess_mrr_association_proxy"]),
                ]
                for _, r in drv_top.iterrows()
            ],
            styles,
            [5.4 * cm, 2.2 * cm, 2.4 * cm, 1.8 * cm, CONTENT_W - 11.8 * cm],
        )
    )
    story.append(
        P(
            "The five behavioural signals at the top of this table are the foundation of "
            "the risk index in Section 04 and of the intervention queues in Section 11. "
            "They define the eligible populations for the controlled intervention layer: "
            "each one maps to a plausible treatment and a revenue stake large enough to "
            "justify preserving a holdout.",
            styles,
        )
    )
    story.append(
        P(
            "The signals also compound. An account paying late and filing support "
            "tickets is in a different state from one carrying a single flag, and the "
            "risk index in Section 04 captures that by scoring on the full signal set. "
            "The save desk should work one ranked list, not five overlapping lists by "
            "signal. Multi-signal value accounts rise to the top; single-flag usage "
            "decline stays in the scaled-motion layer.",
            styles,
        )
    )
    story.append(
        P(
            f"On the open book, {M['multi_signal_active']:,} accounts carry two or more "
            f"distress signals, representing {usd(M['multi_signal_active_mrr'])} of "
            "current MRR. By contrast, "
            f"{M['zero_signal_active']:,} active accounts carry none of the five "
            f"distress signals and account for {usd(M['zero_signal_active_mrr'])} of "
            "current MRR. That split is important operationally. A broad outreach "
            "campaign would spend most of its effort on accounts with no current signal. "
            "A tiered queue reserves human coverage for the multi-signal tail and leaves "
            "low-signal accounts alone. The score's value is not that it finds a new "
            "signal; it prevents over-treatment.",
            styles,
        )
    )
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════
    # 9. FINDINGS — REVENUE AT RISK
    # ════════════════════════════════════════════════════════
    H1("Findings: revenue at risk and concentration", "SECTION 09", "s9", styles, story)
    story.append(
        P(
            "Counting accounts tells you how many customers you are losing. It does not "
            "tell you how much money is leaving, or whether the loss is worth a "
            "targeted defence. This section attaches revenue to the risk and asks the "
            "question that informs "
            "the operating model: is the exposure concentrated enough to defend account "
            "by account, or so diffuse that only a broad programme could touch it?",
            styles,
        )
    )

    story.append(
        P(
            "The answer is that it is highly concentrated. Ranking churned accounts by "
            f"value, the top tenth carries {M['conc10']:.0f} percent of all lost monthly "
            f"revenue, the top fifth carries {M['conc20']:.0f} percent, and the top third "
            f"carries {M['conc30']:.0f} percent. By the halfway point of churned accounts "
            f"by value, {M['conc50']:.0f} percent of all lost monthly revenue is already "
            "accounted for. A small set of departures carries most of the lost monthly value. "
            "That concentration is the operating design point: a focused save desk "
            "working a few hundred named accounts can address the majority of the "
            "monthly-value exposure.",
            styles,
        )
    )

    story.append(
        pull_quote("A small set of departures carries most of the lost monthly value.", styles)
    )

    story.append(
        P(
            f"In dollars, the historical churn pool represents {usd(M['lost_mrr_total'])} "
            "of lost monthly value. The first 5 percent of churned accounts by value "
            f"carry {M['conc5']:.0f} percent of that loss, before the long tail even "
            "enters the discussion. This is why the response should be value-weighted. "
            "A logo-count target would push the team toward many small saves; a "
            "value-weighted target pushes it toward the accounts where one successful "
            "intervention changes the monthly run-rate.",
            styles,
        )
    )

    story.append(
        fig(
            "revenue_concentration.png",
            "Figure 15. Concentration of lost monthly value across churned "
            "accounts. The curve bows far above the line of even "
            "distribution; the marked point shows the top fifth of accounts "
            "carrying the majority of the loss.",
            styles,
        )
    )

    H2("Where the at-risk revenue sits", "s9a", styles, story)
    story.append(
        P(
            "On the forward book, the same concentration appears across segments. The "
            "comparison below sets each segment's current at-risk MRR against the "
            "monthly value it has already lost to churn. SMB carries the largest current "
            f"at-risk MRR at {usd(segr.loc['SMB', 'current_mrr_at_risk'])}, reflecting its "
            "size, while Enterprise, despite churning the fewest accounts, still carries "
            f"{usd(segr.loc['Enterprise', 'current_mrr_at_risk'])} of at-risk MRR because "
            "each Enterprise account is worth so much that even a handful in distress "
            "represents real money. This is why value has to sit alongside risk in the "
            "prioritisation: a single wavering Enterprise account can outweigh a dozen "
            "Startup accounts.",
            styles,
        )
    )

    story.append(
        fig(
            "revenue_at_risk_by_segment.png",
            "Figure 16. Current at-risk MRR against churned monthly value by "
            "segment. SMB leads on current exposure; Enterprise punches above "
            "its account count on value per account.",
            styles,
        )
    )

    srows = []
    for s in ["Enterprise", "Mid-Market", "SMB", "Startup"]:
        if s in segr.index:
            r = segr.loc[s]
            srows.append(
                [
                    s,
                    f"{int(r['at_risk_customers']):,}",
                    usd(r["current_mrr_at_risk"]),
                    f"{int(r['churned_customers']):,}",
                    usd(r["churned_monthly_value_proxy"]),
                ]
            )
    story.append(
        data_table(
            ["Segment", "At-risk accts", "At-risk MRR", "Churned accts", "Churned monthly value"],
            srows,
            styles,
            [3.4 * cm, 2.8 * cm, 2.8 * cm, 2.8 * cm, CONTENT_W - 11.8 * cm],
        )
    )

    H2("The risk tiers", "s9b", styles, story)
    story.append(
        P(
            f"The risk index sorts the {M['scored']:,} active accounts into four tiers. "
            f"The critical tier holds {M['crit_n']:,} accounts billing "
            f"{usd(M['crit_mrr'])} a month, the high tier {M['high_n']:,} accounts "
            f"billing {usd(M['high_mrr'])}, and the medium tier {M['med_n']:,} accounts "
            f"billing {usd(M['med_mrr'])}. The critical and high tiers together, "
            f"{M['crit_n'] + M['high_n']:,} accounts and "
            f"{usd(M['crit_mrr'] + M['high_mrr'])} of MRR, are small enough for a team to "
            "work by hand and large enough to matter. They are the spine of the priority "
            "queue.",
            styles,
        )
    )

    story.append(
        P(
            f"These two upper tiers represent only {M['high_crit_count_share']:.1f} "
            f"percent of scored accounts but {M['high_crit_mrr_share']:.1f} percent of "
            "scored-book MRR. That is not a perfect concentration ratio, but it is "
            "enough to justify named-account management. The more important point is "
            "capacity: two hundred twenty-three accounts can be assigned, worked, and "
            "tracked without turning the programme into a mass campaign. The medium "
            "tier should be monitored and largely automated; the low tier should be "
            "protected from unnecessary outreach so customer-facing teams do not create "
            "work where the data shows no current problem.",
            styles,
        )
    )

    story.append(
        fig(
            "risk_tier_breakdown.png",
            "Figure 17. Account count and MRR exposure by risk tier. The "
            "critical and high tiers are a workable number of accounts "
            "holding a material share of exposed revenue.",
            styles,
        )
    )

    story.append(
        P(
            "The score distribution behind those tiers confirms that the book is "
            "low-risk in aggregate and the risk is a tail, not a bulk. The low tier dominates "
            "the count and clusters at the bottom of the score range; the critical and "
            "high tiers are a thin, well-separated tail at the top. That shape is what a "
            "prioritisation index should produce: it avoids labelling half the book as "
            "at risk and isolates the minority that needs review.",
            styles,
        )
    )

    story.append(
        fig(
            "risk_score_distribution.png",
            "Figure 18. Distribution of churn risk scores by tier. The mass "
            "sits in the low-risk tier; the at-risk tiers form a distinct "
            "upper tail.",
            styles,
        )
    )
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════
    # 10. RISKS, LIMITATIONS, CAVEATS
    # ════════════════════════════════════════════════════════
    H1("Risks, limitations, and caveats", "SECTION 10", "s10", styles, story)
    story.append(
        P(
            "A report that hides its weaknesses is less useful than one that names them, "
            "because the reader cannot judge how far to trust a finding without knowing "
            "what could undermine it. Six limitations bound everything above and each "
            "changes how a specific finding should be read.",
            styles,
        )
    )

    H2("The data is synthetic", "s10a", styles, story)
    story.append(
        P(
            "The book is generated from a fixed seed, not collected from a live business. "
            "Its relationships are designed and deterministic, so the magnitudes are "
            "illustrative rather than externally observed. The behavioural lifts in Section 08 are larger "
            "than a real book would show, because the comparison group of low-signal "
            "accounts almost never churns here. The method, the pipeline, the metric "
            "definitions, and the prioritisation logic are reusable starting points for "
            "real data, but source mappings, thresholds, validation rules, and outcome "
            "calibration would need to be fitted to the operating environment.",
            styles,
        )
    )

    H2("Recent acceleration is not yet a trend", "s10b", styles, story)
    story.append(
        P(
            "The last three complete months have a higher realised churn rate than the "
            f"prior nine-month baseline, with the final month at {M['last_cust_churn']:.1f} "
            "percent. Three observations are not enough to separate a persistent shift "
            "from ordinary variation or a change in customer mix. The appropriate response "
            "is to monitor the next complete periods and decompose the change by segment, "
            "channel, plan, and tenure rather than declare a churn crisis.",
            styles,
        )
    )

    H2("Economics use recurring-value and direct-cost proxies", "s10c", styles, story)
    story.append(
        P(
            "The MRR bridge reconciles contract-value movements, direct service costs, "
            "acquisition spend, payback, and capped 24-month LTV. These measures are "
            "applied consistently and support retention-adjusted comparisons, but they "
            "are not GAAP revenue recognition, audited cost allocation, remaining contract "
            "value, or a perpetuity. Absolute exposure remains a monthly run-rate proxy.",
            styles,
        )
    )

    H2("Relationships are associative", "s10d", styles, story)
    story.append(
        P(
            "No finding in this report establishes cause. A low net promoter score does "
            "not necessarily cause a departure; it co-occurs with it. The intervention "
            "queues are therefore assigned with an explicit randomized holdout. The "
            "bundled forward outcomes are simulated to demonstrate the estimator and must "
            "not be presented as observed treatment efficacy. Weighted exposure sizes the "
            "scope; only live controlled outcomes can establish incremental value.",
            styles,
        )
    )

    H2("Tiering depends on the chosen thresholds", "s10e", styles, story)
    story.append(
        P(
            "The value tiers and risk cut-points are quantile-based and would need "
            "recalibration on a real distribution. Moving a threshold moves accounts "
            "between tiers and changes the headline counts. The ranking is stable to "
            "these choices; the exact tier populations are not, and any operational "
            "commitment to a tier size should be set against real data.",
            styles,
        )
    )

    H2("The operating history is simulated", "s10f", styles, story)
    story.append(
        P(
            "The system now retains monthly customer probabilities, tier transitions, "
            "calibration outcomes, and drift. That resolves the single-snapshot engineering "
            "gap, but the history is still generated rather than observed in operations. "
            "A live deployment needs enough complete monthly intervals to distinguish "
            "seasonality, recovery, definition changes, and persistent deterioration before "
            "alert thresholds or retraining cadence are treated as stable.",
            styles,
        )
    )

    story.append(PageBreak())
    H2("What would change the conclusion", "s10g", styles, story)
    story.append(
        P(
            "The limitations above do not invalidate the recommendations, but they define "
            "the tests that would change them. The table below is the report's decision "
            "control: if one of these checks fails on real or refreshed data, the "
            "operating response should change before more budget is committed.",
            styles,
        )
    )
    story.append(
        data_table(
            ["Conclusion at risk", "Check to run", "Decision trigger"],
            [
                [
                    "Low-end acquisition is the main churn issue",
                    "Hold segment, plan, and region constant and re-estimate channel "
                    "retention on live cohorts.",
                    "If Affiliate and Paid Search no longer over-index after controls, "
                    "shift from budget reallocation to onboarding/product fixes.",
                ],
                [
                    "Recent cohorts are deteriorating",
                    "Refresh the cohort read after the next three complete months and "
                    "compare at fixed ages.",
                    "If the fixed-age gap closes, treat the spike as right-censoring noise "
                    "rather than a structural mix shift.",
                ],
                [
                    "Behavioural signals are intervention-ready",
                    "Run held-out tests for payment rescue, adoption reactivation, and "
                    "service recovery.",
                    "If treated accounts do not outperform controls, downgrade the driver "
                    "from an intervention lever to a monitoring signal.",
                ],
                [
                    "The save desk should be value-weighted",
                    "Compare saved MRR, saved logos, and cost-to-serve by queue.",
                    "If small-account saves have materially better economics, rebalance "
                    "capacity toward scaled automation rather than named-account coverage.",
                ],
            ],
            styles,
            [4.0 * cm, 6.2 * cm, CONTENT_W - 10.2 * cm],
        )
    )
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════
    # 11. RECOMMENDATIONS
    # ════════════════════════════════════════════════════════
    H1("Recommendations and action priorities", "SECTION 11", "s11", styles, story)
    story.append(
        P(
            "The findings converge on a short, ordered list. The ordering is by weighted "
            "exposure and operational separability, and "
            "each recommendation names the finding it rests on, the population it "
            "targets, and the metric that would prove it worked.",
            styles,
        )
    )

    story.append(
        P(
            "The four intervention queues that anchor the priorities are sized below. "
            "Read the scope column as current MRR passing through each queue. Weighted "
            "exposure multiplies that MRR by the target group's historical churn share, "
            "which puts the queues on a common ranking scale without estimating saved "
            "revenue. The gap between the two columns reflects the historical "
            "churn rate of the targeted accounts: the renewal save desk works a large "
            "book at a moderate rate, while payment rescue works a small book at a very "
            "high rate. The data ranks exposure; it does not contain implementation cost "
            "and therefore does not estimate return on effort.",
            styles,
        )
    )
    itv_rows = []
    for _, r in itv.iterrows():
        itv_rows.append(
            [
                r["opportunity"],
                f"{int(r['candidate_customers']):,}",
                usd(r["current_mrr_scope"]),
                f"{r['historical_churn_share'] * 100:.0f}%",
                usd(r["mrr_exposure_proxy"]),
            ]
        )
    story.append(
        data_table(
            ["Intervention queue", "Accounts", "MRR in scope", "Hist. churn", "Weighted exposure"],
            itv_rows,
            styles,
            [5.0 * cm, 2.2 * cm, 2.8 * cm, 2.2 * cm, CONTENT_W - 12.2 * cm],
        )
    )

    recs = [
        (
            "P1",
            "Stand up a renewal save desk for Mid-Market and SMB",
            f"This play carries the largest weighted exposure in the book at "
            f"{usd(itv.iloc[0]['mrr_exposure_proxy'])}, across "
            f"{int(itv.iloc[0]['candidate_customers']):,} accounts holding "
            f"{usd(itv.iloc[0]['current_mrr_scope'])} of MRR in scope, with the focus on "
            "Mid-Market and SMB where the value and the volume meet (Section 09, Figure "
            "1). Work the critical and high risk tiers first, lead with the accounts "
            "flagged on sentiment and support, and run a pre-renewal save playbook with "
            "tailored offers. Measure success as the saved MRR among contacted accounts "
            "against a held-out control, not as gross contact volume.",
        ),
        (
            "P1",
            "Fix the payment-failure path",
            f"Accounts with a failed payment churn at "
            f"{beh.loc['failed_payment_flag', 'churn_rate_in_group'] * 100:.0f} percent, "
            f"more than {floor_multiple(beh.loc['failed_payment_flag', 'churn_rate_lift'])} times the "
            "rate of accounts that pay cleanly (Section 08). The payment-rescue queue is "
            f"only {int(itv.iloc[1]['candidate_customers']):,} accounts but carries "
            f"{usd(itv.iloc[1]['mrr_exposure_proxy'])} of weighted exposure. The operating "
            "fix is straightforward: dunning sequencing, card-update prompts, and retry "
            "logic. It is the smallest of the high-exposure specialist queues and can run "
            "in parallel with the save desk. Measure recovered MRR per failed-payment "
            "account.",
        ),
        (
            "P2",
            "Rebalance acquisition spend away from the weakest channels",
            f"Affiliate and Paid Search accounts churn at "
            f"{chan.loc['Affiliate', 'cumulative_churn_share'] * 100:.1f} and "
            f"{chan.loc['Paid Search', 'cumulative_churn_share'] * 100:.1f} percent against "
            f"{chan.loc['Partner', 'cumulative_churn_share'] * 100:.1f} percent for Partner "
            f"and {chan.loc['Referral', 'cumulative_churn_share'] * 100:.1f} percent for "
            "Referral (Section 07, Figure 10). Run a controlled marginal reallocation and "
            "retain it only if six-month retention improves after accounting for CAC, "
            "segment, plan, and acquisition volume.",
        ),
        (
            "P2",
            "Run an adoption-reactivation programme for the usage-decline tail",
            f"Usage decline touches {int(beh.loc['usage_decline_flag', 'customers_in_group']):,} "
            "accounts, the largest behavioural population, at a lower per-account "
            "certainty than the sentiment flags (Section 08). It suits a scaled, "
            "automated motion: onboarding refreshers, feature nudges, and success "
            f"check-ins. The reactivation queue carries "
            f"{usd(itv.iloc[2]['mrr_exposure_proxy'])} of weighted exposure across "
            f"{int(itv.iloc[2]['candidate_customers']):,} accounts. Measure the share of "
            "flagged accounts that return to a positive usage trend.",
        ),
        (
            "P3",
            "Treat low-tier and Basic-plan signups as a distinct, instrumented class",
            f"Basic-plan accounts churn at "
            f"{plan.loc['Basic', 'cumulative_churn_share'] * 100:.1f} percent (Section 07). "
            "Rather than a save motion, this calls for a product and onboarding fix: a "
            "guided first-value path for entry-tier accounts and clearer upgrade prompts "
            "for the ones that show real engagement. Measure the rate at which Basic "
            "accounts either reach an activation milestone or upgrade within ninety days.",
        ),
        (
            "P3",
            "Replace simulated intervention outcomes with observed results",
            f"The controlled design already assigns {M['experiment_treatment']:,} accounts "
            f"to treatment and {M['experiment_control']:,} to holdout with maximum baseline "
            f"SMD {M['maximum_experiment_smd']:.3f}. Preserve that assignment, record "
            "delivery and cost, and replace simulated outcomes with observed 90-day churn "
            "before scaling or claiming ROI.",
        ),
    ]
    for tag, title, bodytext in recs:
        story.append(
            KeepTogether(
                [
                    Paragraph(
                        f'<font color="#B83530"><b>{tag}</b></font>&nbsp;&nbsp;'
                        f'<font color="#1B3A6B"><b>{title}</b></font>',
                        styles["h3"],
                    ),
                    P(bodytext, styles),
                ]
            )
        )

    story.append(Spacer(1, 4))
    story.append(
        P(
            "Sequenced this way, the first two actions address open-account exposure, the "
            "middle two test acquisition and adoption hypotheses, and the last two build "
            "measurement discipline. Capacity, implementation cost, and delivery timing "
            "are not present in the dataset and must be set by the operating team.",
            styles,
        )
    )

    story.append(
        SectionHeading("Sequencing, ownership, and success measures", styles["h3"], 1, "s11seq")
    )
    story.append(
        P(
            "The plan resolves to the following sequence. Each action carries one owner, "
            "one horizon, and one measure that settles whether it worked, so the "
            "programme can be governed on outcomes rather than activity.",
            styles,
        )
    )
    story.append(
        data_table(
            ["#", "Action", "Owner", "Horizon", "Success measure"],
            [
                [
                    "P1",
                    "Renewal save desk",
                    "Customer Success",
                    "0-90 days",
                    "Saved MRR vs held-out control",
                ],
                [
                    "P1",
                    "Payment-failure path",
                    "Billing / RevOps",
                    "0-60 days",
                    "Recovered MRR per failed-payment account",
                ],
                [
                    "P2",
                    "Acquisition rebalance",
                    "Marketing",
                    "1-2 quarters",
                    "6-month retention of new cohorts by channel",
                ],
                [
                    "P2",
                    "Adoption reactivation",
                    "Customer Success",
                    "1-2 quarters",
                    "Share of flagged accounts returning to positive usage",
                ],
                [
                    "P3",
                    "Basic-plan onboarding fix",
                    "Product",
                    "2 quarters",
                    "90-day activation or upgrade rate",
                ],
                [
                    "P3",
                    "Driver experiments",
                    "Analytics",
                    "Ongoing",
                    "Measured lift vs control per queue",
                ],
            ],
            styles,
            [1.0 * cm, 4.2 * cm, 3.0 * cm, 2.2 * cm, CONTENT_W - 10.4 * cm],
        )
    )

    H2("Management cadence and escalation rules", "s11gov", styles, story)
    story.append(
        P(
            "The programme should be governed as a short operating cycle, not a one-off "
            "analysis handoff. The first review establishes the queue, owners, and "
            "controls. The second tests the mechanics. The third decides which motions "
            "scale and which are stopped. This cadence keeps the team from mistaking "
            "activity for retention impact.",
            styles,
        )
    )
    story.append(
        data_table(
            ["Review point", "Decision to make", "Evidence required"],
            [
                [
                    "Day 30",
                    "Confirm that the critical/high-risk queue is assigned and contactable.",
                    "Named-account coverage, payment-failure retry status, and baseline "
                    "control groups for each intervention.",
                ],
                [
                    "Day 60",
                    "Decide whether payment rescue and adoption reactivation are clearing "
                    "their minimum return bar.",
                    "Recovered MRR per failed-payment account, usage trend recovery rate, "
                    "and untreated-control comparison.",
                ],
                [
                    "Day 90",
                    "Scale, stop, or redesign the save-desk motion.",
                    "Saved MRR versus control, renewal outcomes by risk tier, and cost per "
                    "account worked.",
                ],
                [
                    "Quarterly",
                    "Reallocate acquisition budget by observed cohort quality.",
                    "Six-month retention by channel and plan, plus finance-approved CAC, "
                    "gross margin, and payback.",
                ],
            ],
            styles,
            [2.6 * cm, 5.4 * cm, CONTENT_W - 8.0 * cm],
        )
    )
    story.append(
        P(
            f"The four intervention queues currently cover {usd(M['play_scope_mrr'])} "
            f"of MRR in scope and {usd(M['play_weighted_exposure'])} of weighted "
            "monthly exposure. That is the starting control total. A good first quarter "
            "does not need to eliminate the risk; it needs to show that the critical and "
            "high-risk tail is shrinking, that payment failures are being recovered "
            "faster, and that new cohorts are no longer over-weighted toward the weakest "
            "channels. Those are the proof points that the recommendation is working.",
            styles,
        )
    )
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════
    # 12. PRODUCTION EVIDENCE
    # ════════════════════════════════════════════════════════
    H1("Production evidence still required", "SECTION 12", "s12", styles, story)
    story.append(
        P(
            "The engineering gaps identified in the initial analysis are now implemented: "
            "reconciled MRR movements, unit economics, calibrated temporal probabilities, "
            "randomized holdouts, incremental saved-MRR estimation, and tier-transition "
            "alerts. The remaining questions are evidence gaps that only live operating "
            "history can close.",
            styles,
        )
    )
    story.append(
        data_table(
            ["Implemented control", "Current release evidence", "Production evidence needed"],
            [
                [
                    "Retention-adjusted unit economics",
                    f"Average NRR {M['average_monthly_nrr'] * 100:.1f}%; direct-cost margin "
                    f"{M['gross_margin_rate'] * 100:.1f}%; blended CAC "
                    f"{usd(M['blended_cac'])}.",
                    "Finance-approved spend allocation, cost-to-serve, revenue recognition, "
                    "and mature cohort follow-up.",
                ],
                [
                    "Point-in-time probability model",
                    f"Out-of-time ROC AUC {M['model_roc_auc']:.3f}, average precision "
                    f"{M['model_average_precision']:.3f}, Brier "
                    f"{M['model_brier_score']:.4f}.",
                    "Live outcomes, stable feature definitions, independent recalibration, "
                    "and customer-cluster uncertainty estimates.",
                ],
                [
                    "Product activation definition",
                    "Trailing sessions and adoption separate risk, but do not identify a "
                    "causal first-value event.",
                    "Event-level product usage mapped to activation, expansion, support "
                    "volume, and churn outcomes.",
                ],
                [
                    "Randomized intervention holdout",
                    f"{M['experiment_treatment']:,} treatment and "
                    f"{M['experiment_control']:,} control; maximum baseline SMD "
                    f"{M['maximum_experiment_smd']:.3f}.",
                    "Observed delivery, treatment cost, 90-day outcomes, and sufficient "
                    "sample size before efficacy or ROI claims.",
                ],
                [
                    "Risk migration and alerts",
                    f"Monthly tier transitions, outcome calibration, PSI, and "
                    f"{M['monitoring_alerts']} open release alerts.",
                    "Enough complete operating months to estimate seasonality, alert "
                    "precision, recovery, and retraining cadence.",
                ],
            ],
            styles,
            [4.3 * cm, 6.3 * cm, CONTENT_W - 10.6 * cm],
        )
    )
    story.append(
        P(
            f"The simulated intervention produces an incremental saved-MRR estimate of "
            f"{usd(M['simulated_saved_mrr'])}, with a 95 percent interval from "
            f"{usd(M['simulated_saved_mrr_ci_lower'])} to "
            f"{usd(M['simulated_saved_mrr_ci_upper'])}. That number validates the "
            "holdout estimator and reconciliation logic; budget allocation must wait for "
            "observed outcomes and delivery cost.",
            styles,
        )
    )
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════
    # 13. APPENDIX
    # ════════════════════════════════════════════════════════
    H1("Appendix", "SECTION 13", "s13", styles, story)

    H2("A. Cumulative churn share by commercial cut", "s12a", styles, story)
    rows = []
    for label, frame, idx in [
        ("Segment", seg, ["Enterprise", "Mid-Market", "SMB", "Startup"]),
        ("Plan", plan, ["Enterprise", "Pro", "Growth", "Basic"]),
        ("Channel", chan, list(chan.index)),
        ("Region", reg, list(reg.index)),
    ]:
        for k in idx:
            if k in frame.index:
                r = frame.loc[k]
                rows.append(
                    [
                        label,
                        k,
                        f"{int(r['customers']):,}",
                        f"{int(r['churned_customers']):,}",
                        f"{r['cumulative_churn_share'] * 100:.1f}%",
                        usd(r["avg_monthly_revenue"]),
                    ]
                )
    story.append(
        data_table(
            ["Cut", "Group", "Accounts", "Churned", "Churn share", "Avg MRR"],
            rows,
            styles,
            [2.2 * cm, 3.4 * cm, 2.2 * cm, 2.0 * cm, 2.6 * cm, CONTENT_W - 12.4 * cm],
        )
    )

    H2("B. Full behavioural driver detail", "s12b", styles, story)
    brows = []
    for k, r in beh.iterrows():
        brows.append(
            [
                k.replace("_flag", "").replace("_", " ").title().replace("Nps", "NPS"),
                f"{int(r['customers_in_group']):,}",
                f"{r['churn_rate_in_group'] * 100:.0f}%",
                f"{r['churn_rate_out_group'] * 100:.0f}%",
                f"{r['churn_rate_lift']:.1f}×",
            ]
        )
    story.append(
        data_table(
            ["Signal", "Accounts", "Churn w/ signal", "Churn w/o signal", "Lift"],
            brows,
            styles,
            [5.6 * cm, 2.6 * cm, 3.2 * cm, 3.4 * cm, CONTENT_W - 14.8 * cm],
        )
    )

    H2("C. Recommended-action distribution across the scored book", "s12c", styles, story)
    arows = [[k, f"{v:,}", f"{v / M['scored'] * 100:.1f}%"] for k, v in M["rec_counts"].items()]
    story.append(
        data_table(
            ["Recommended action", "Accounts", "Share of scored book"],
            arows,
            styles,
            [7.0 * cm, 3.0 * cm, CONTENT_W - 10.0 * cm],
        )
    )

    H2("D. Monthly retention trend, last twelve months", "s12d2", styles, story)
    story.append(
        P(
            "The active book at the start of each month, the accounts and revenue lost "
            "during it, and the resulting churn rates. The customer and revenue churn "
            "columns are the data behind Figures 2 and 3; the right-censoring caveat in "
            "Section 10 applies to the final rows.",
            styles,
        )
    )
    story.append(
        data_table(
            ["Month", "Active accts", "Active MRR", "Churned", "Cust. churn", "Rev. churn"],
            M["trend12"],
            styles,
            [2.8 * cm, 2.4 * cm, 2.6 * cm, 2.0 * cm, 2.4 * cm, CONTENT_W - 12.2 * cm],
        )
    )

    H2("E. Figure index", "s12d", styles, story)
    story.append(
        P(
            "Figures 1 through 18 are the static export of the chart pack in "
            f"{code('outputs/graphs/')}, regenerated deterministically from the same "
            "processed data and analytical tables that drive the live dashboard. The "
            f"dashboard ({code('executive-retention-command-center.html')}) carries the "
            "interactive versions of the trend, segment, driver, and queue views, with "
            "period, segment, and channel filters. This report and that dashboard are "
            "two renderings of one "
            "governed pipeline; neither contains a number the other cannot reproduce.",
            styles,
        )
    )

    H2("F. Per-account feature dictionary", "s12f", styles, story)
    story.append(
        P(
            "The behavioural signals examined in Sections 07 and 08 and scored in the "
            "risk index are measured at the account's churn date or, for active accounts, "
            "at the snapshot. The principal fields are defined below.",
            styles,
        )
    )
    story.append(
        data_table(
            ["Field", "Definition"],
            [
                [code("tenure_days"), "Days from subscription start to churn or snapshot."],
                [code("current_mrr"), "Account's monthly recurring revenue at observation."],
                [
                    code("usage_trend"),
                    "Mean weekly sessions in days 0-29 minus the mean in days 30-59.",
                ],
                [code("recent_sessions_30d / 90d"), "Product sessions in the trailing window."],
                [
                    code("feature_adoption_score_recent"),
                    "Mean feature-adoption score in the trailing 30 days, 0 to 100.",
                ],
                [code("support_tickets_30d / 90d"), "Support tickets filed in the window."],
                [code("nps_score_recent"), "Mean NPS in the trailing 90 days."],
                [code("failed_payments_90d"), "Count of failed billing attempts in 90 days."],
                [code("payment_failure_flag"), "Any failed payment in the window."],
                [code("renewal_near_flag"), "Contract renewal falls within 45 days."],
                [code("at_risk_flag"), "Source subscription status is at_risk."],
                [code("churn_flag"), "Account has closed (the analysis target)."],
            ],
            styles,
            [6.2 * cm, CONTENT_W - 6.2 * cm],
        )
    )

    H2("G. Strategic expansion release evidence", "s12g", styles, story)
    story.append(
        data_table(
            ["Control", "Release result", "Interpretation"],
            [
                [
                    "Retention economics",
                    f"NRR {M['average_monthly_nrr'] * 100:.1f}%; gross-margin proxy "
                    f"{M['gross_margin_rate'] * 100:.1f}%; CAC {usd(M['blended_cac'])}",
                    "Complete periods only; recurring-value and direct-cost proxies.",
                ],
                [
                    "90-day probability model",
                    f"ROC AUC {M['model_roc_auc']:.3f}; AP "
                    f"{M['model_average_precision']:.3f}; Brier "
                    f"{M['model_brier_score']:.4f}",
                    "Independent calibration and out-of-time evaluation on synthetic data.",
                ],
                [
                    "Intervention holdout",
                    f"{M['experiment_treatment']:,} treatment / "
                    f"{M['experiment_control']:,} control; max SMD "
                    f"{M['maximum_experiment_smd']:.3f}",
                    "Assignment is valid; forward outcomes are a simulation.",
                ],
                [
                    "Incremental saved MRR",
                    f"{usd(M['simulated_saved_mrr'])}; 95% interval "
                    f"{usd(M['simulated_saved_mrr_ci_lower'])} to "
                    f"{usd(M['simulated_saved_mrr_ci_upper'])}",
                    "Estimator demonstration, not production efficacy or ROI.",
                ],
                [
                    "Monitoring",
                    f"{M['monitoring_alerts']} open alerts; {M['gov_pass']} / "
                    f"{M['gov_total']} validation checks pass",
                    "Monthly transitions exclude partial periods from alert decisions.",
                ],
            ],
            styles,
            [4.1 * cm, 5.0 * cm, CONTENT_W - 9.1 * cm],
        )
    )

    return story


# ════════════════════════════════════════════════════════════
# Build
# ════════════════════════════════════════════════════════════
def build() -> Path:
    styles = build_styles()
    M = load_metrics()
    story = build_story(styles, M)

    doc = ReportDoc(
        str(REPORT_PATH),
        pagesize=A4,
        leftMargin=LMARGIN,
        rightMargin=RMARGIN,
        topMargin=TMARGIN,
        bottomMargin=BMARGIN,
        title="Churn & Retention Intelligence: Retention Review",
        author="Retention Analytics",
        subject="Churn and retention analysis",
    )
    # multiBuild resolves the table of contents page numbers (two passes)
    doc.multiBuild(story, canvasmaker=_invariant_canvas)
    return REPORT_PATH


def _invariant_canvas(*args, **kwargs) -> Canvas:
    """Build byte-stable PDFs when content and the optional report date are unchanged."""
    kwargs["invariant"] = 1
    return Canvas(*args, **kwargs)


def main() -> None:
    path = build()
    size_kb = path.stat().st_size / 1024
    print(f"Report written → {path.relative_to(ROOT)}  ({size_kb:,.0f} KB)")


if __name__ == "__main__":
    main()
