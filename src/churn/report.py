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
from pathlib import Path

import pandas as pd
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import cm
from reportlab.lib.utils import ImageReader
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

# ── Paths ──────────────────────────────────────────────────
ROOT   = Path(__file__).resolve().parents[2]
GRAPHS = ROOT / "outputs" / "graphs"
TABLES = ROOT / "outputs" / "tables"
PROC   = ROOT / "data" / "processed"
OUT    = ROOT / "outputs" / "reports"
OUT.mkdir(parents=True, exist_ok=True)

REPORT_PATH = OUT / "churn-retention-intelligence-report.pdf"

# ── Palette (matches the chart pack and dashboard) ─────────
INK     = colors.HexColor("#1E293B")   # body text
SLATE   = colors.HexColor("#334155")
MUTED   = colors.HexColor("#64748B")
HAIR    = colors.HexColor("#CBD5E1")
PALE    = colors.HexColor("#E2E8F0")
PAPER   = colors.HexColor("#FFFFFF")
NAVY    = colors.HexColor("#1B3A6B")   # accent
LOSS    = colors.HexColor("#B83530")   # emphasis / risk
GAIN    = colors.HexColor("#1B6640")
CREAM   = colors.HexColor("#FAF8F4")

PAGE_W, PAGE_H = A4
LMARGIN = 2.4 * cm
RMARGIN = 2.4 * cm
TMARGIN = 2.3 * cm
BMARGIN = 2.2 * cm
CONTENT_W = PAGE_W - LMARGIN - RMARGIN

REFERENCE_DATE = "1 March 2026"
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
    seg  = _csv("churn_by_segment.csv").set_index("segment")
    chan = _csv("churn_by_acquisition_channel.csv").set_index("acquisition_channel")
    plan = _csv("churn_by_plan_type.csv").set_index("plan_type")
    reg  = _csv("churn_by_region.csv").set_index("region")
    beh  = _csv("behavioral_churn_relationships.csv").set_index("relationship")
    tier = _csv("risk_tier_summary.csv").set_index("risk_tier")
    drv  = _csv("main_analysis_churn_driver_ranking.csv")
    itv  = _csv("main_analysis_intervention_priorities.csv")
    segr = _csv("segment_revenue_risk_contribution.csv").set_index("segment")
    trend = pd.read_csv(TABLES / "overall_retention_trend_monthly.csv",
                        parse_dates=["month"])
    trend = trend[trend["active_customers_start"] >= 50].copy()
    val = pd.read_csv(TABLES / "final_validation_checks.csv")
    gov = [[cat, f"{int((g['status'] == 'PASS').sum())} / {len(g)}"]
           for cat, g in val.groupby("category")]
    gov_total = len(val)
    gov_pass = int((val["status"] == "PASS").sum())

    total = len(feats)
    churned = int(feats["churn_flag"].sum())
    active = total - churned

    # Revenue concentration of lost monthly value
    lost = feats.loc[feats["churn_flag"] == 1, "avg_monthly_revenue"] \
        .sort_values(ascending=False).reset_index(drop=True)
    cum = lost.cumsum() / lost.sum()
    def conc(q):
        k = max(int(len(lost) * q) - 1, 0)
        return float(cum.iloc[k]) * 100

    last = trend.iloc[-1]
    prior9 = trend["customer_churn_rate"].iloc[-12:-3].mean() * 100
    t12 = trend.tail(12)
    trend12 = [
        [r["month"].strftime("%b %Y"),
         f"{int(r['active_customers_start']):,}",
         usd(r["active_mrr_start"], k=True),
         f"{int(r['churned_customers']):,}",
         f"{r['customer_churn_rate']*100:.1f}%",
         f"{r['revenue_churn_rate']*100:.1f}%"]
        for _, r in t12.iterrows()
    ]

    return {
        "total": total, "active": active, "churned": churned,
        "cum_churn_pct": churned / total * 100,
        "active_mrr": float(last["active_mrr_start"]),
        "window_start": trend["month"].min().strftime("%b %Y"),
        "window_end": trend["month"].max().strftime("%b %Y"),
        "avg_cust_churn": trend["customer_churn_rate"].mean() * 100,
        "avg_rev_churn": trend["revenue_churn_rate"].mean() * 100,
        "last_cust_churn": float(last["customer_churn_rate"]) * 100,
        "last_rev_churn": float(last["revenue_churn_rate"]) * 100,
        "prior9_churn": prior9,
        "trend12": trend12,
        "gov": gov, "gov_total": gov_total, "gov_pass": gov_pass,
        # segments
        "seg": seg, "chan": chan, "plan": plan, "reg": reg,
        "beh": beh, "tier": tier, "drv": drv, "itv": itv, "segr": segr,
        "risk_mean": float(risk["churn_risk_score"].mean()),
        "scored": len(risk),
        "rec_counts": risk["recommended_action"].value_counts().to_dict(),
        "driver_counts": risk["main_risk_driver"].value_counts().to_dict(),
        # concentration
        "conc10": conc(0.10), "conc20": conc(0.20),
        "conc30": conc(0.30), "conc50": conc(0.50),
        # tier exposure
        "crit_n": int(tier.loc["critical", "customers"]),
        "high_n": int(tier.loc["high", "customers"]),
        "med_n": int(tier.loc["medium", "customers"]),
        "crit_mrr": float(tier.loc["critical", "total_current_mrr"]),
        "high_mrr": float(tier.loc["high", "total_current_mrr"]),
        "med_mrr": float(tier.loc["medium", "total_current_mrr"]),
    }


def usd(x: float, k: bool = False) -> str:
    if k:
        return f"${x/1000:,.0f}k"
    return f"${x:,.0f}"


# ════════════════════════════════════════════════════════════
# Styles
# ════════════════════════════════════════════════════════════
def build_styles() -> dict:
    body = ParagraphStyle(
        "Body", fontName="Times-Roman", fontSize=10.5, leading=15.5,
        textColor=INK, alignment=TA_JUSTIFY, spaceAfter=8, firstLineIndent=0,
    )
    styles = {
        "body": body,
        "lead": ParagraphStyle(
            "Lead", parent=body, fontSize=11.5, leading=17, spaceAfter=10,
            textColor=SLATE,
        ),
        "h1": ParagraphStyle(
            "H1", fontName="Helvetica-Bold", fontSize=19, leading=22,
            textColor=NAVY, spaceBefore=4, spaceAfter=4,
        ),
        "h1num": ParagraphStyle(
            "H1num", fontName="Helvetica-Bold", fontSize=10, leading=12,
            textColor=LOSS, spaceAfter=2,
        ),
        "h2": ParagraphStyle(
            "H2", fontName="Helvetica-Bold", fontSize=13, leading=16,
            textColor=INK, spaceBefore=14, spaceAfter=5,
        ),
        "h3": ParagraphStyle(
            "H3", fontName="Helvetica-Bold", fontSize=11, leading=14,
            textColor=SLATE, spaceBefore=10, spaceAfter=3,
        ),
        "caption": ParagraphStyle(
            "Caption", fontName="Helvetica", fontSize=8.5, leading=11.5,
            textColor=MUTED, alignment=TA_LEFT, spaceBefore=4, spaceAfter=16,
        ),
        "pull": ParagraphStyle(
            "Pull", fontName="Times-Italic", fontSize=13, leading=18,
            textColor=NAVY, alignment=TA_LEFT, spaceBefore=6, spaceAfter=12,
            leftIndent=14, rightIndent=14, borderColor=HAIR,
        ),
        "kicker": ParagraphStyle(
            "Kicker", fontName="Helvetica-Bold", fontSize=9, leading=12,
            textColor=LOSS, spaceAfter=3,
        ),
        "toc1": ParagraphStyle(
            "TOC1", fontName="Helvetica-Bold", fontSize=10.5, leading=20,
            textColor=INK,
        ),
        "toc2": ParagraphStyle(
            "TOC2", fontName="Times-Roman", fontSize=10, leading=16,
            textColor=SLATE, leftIndent=16,
        ),
        # cover
        "cover_kick": ParagraphStyle(
            "CK", fontName="Helvetica-Bold", fontSize=11, leading=15,
            textColor=LOSS, alignment=TA_LEFT,
        ),
        "cover_title": ParagraphStyle(
            "CT", fontName="Helvetica-Bold", fontSize=33, leading=37,
            textColor=NAVY, alignment=TA_LEFT, spaceBefore=10, spaceAfter=8,
        ),
        "cover_sub": ParagraphStyle(
            "CS", fontName="Times-Italic", fontSize=15, leading=21,
            textColor=SLATE, alignment=TA_LEFT,
        ),
        "cover_meta": ParagraphStyle(
            "CM", fontName="Helvetica", fontSize=9.5, leading=15,
            textColor=MUTED, alignment=TA_LEFT,
        ),
        "tbl": ParagraphStyle(
            "Tbl", fontName="Times-Roman", fontSize=9, leading=12,
            textColor=INK,
        ),
        "tblh": ParagraphStyle(
            "Tblh", fontName="Helvetica-Bold", fontSize=8.5, leading=11,
            textColor=PAPER,
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


def fig(name: str, caption: str, styles: dict, width_frac: float = 1.0,
        max_h: float = 11.5 * cm):
    """Inline figure scaled to the content width, with a numbered caption."""
    path = GRAPHS / name
    iw, ih = ImageReader(str(path)).getSize()
    w = CONTENT_W * width_frac
    h = w * ih / iw
    if h > max_h:
        h = max_h
        w = h * iw / ih
    img = Image(str(path), width=w, height=h)
    img.hAlign = "CENTER"
    cap = Paragraph(caption, styles["caption"])
    return KeepTogether([Spacer(1, 4), img, cap])


def rule(color=HAIR, thickness=0.6, space_before=2, space_after=8, width=None):
    t = Table([[""]], colWidths=[width or CONTENT_W], rowHeights=[0.1])
    t.setStyle(TableStyle([
        ("LINEABOVE", (0, 0), (-1, 0), thickness, color),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))
    return KeepTogether([Spacer(1, space_before), t, Spacer(1, space_after)])


def stat_band(items, styles):
    """A row of headline statistics used in the executive summary."""
    cells = []
    for value, label in items:
        v = Paragraph(value, ParagraphStyle(
            "sv", fontName="Helvetica-Bold", fontSize=17, leading=19,
            textColor=NAVY, alignment=TA_CENTER))
        lab = Paragraph(label, ParagraphStyle(
            "sl", fontName="Helvetica", fontSize=7.8, leading=10,
            textColor=MUTED, alignment=TA_CENTER))
        inner = Table([[v], [lab]], colWidths=[CONTENT_W / len(items) - 6])
        inner.setStyle(TableStyle([
            ("TOPPADDING", (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ]))
        cells.append(inner)
    t = Table([cells], colWidths=[CONTENT_W / len(items)] * len(items))
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), CREAM),
        ("BOX", (0, 0), (-1, -1), 0.6, HAIR),
        ("LINEBEFORE", (1, 0), (-1, -1), 0.5, HAIR),
        ("TOPPADDING", (0, 0), (-1, -1), 12),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 12),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    return KeepTogether([Spacer(1, 6), t, Spacer(1, 14)])


def data_table(header, rows, styles, col_widths, emphasize_last=False):
    head = [Paragraph(h, styles["tblh"]) for h in header]
    body = [[Paragraph(str(c), styles["tbl"]) for c in r] for r in rows]
    t = Table([head, *body], colWidths=col_widths, repeatRows=1)
    style = [
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING", (0, 0), (-1, -1), 7),
        ("RIGHTPADDING", (0, 0), (-1, -1), 7),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LINEBELOW", (0, 0), (-1, -1), 0.4, PALE),
        ("LINEBELOW", (0, 0), (-1, 0), 0.6, NAVY),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [PAPER, CREAM]),
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
            frames=[Frame(LMARGIN, BMARGIN, CONTENT_W, PAGE_H - TMARGIN - BMARGIN,
                          id="cf", leftPadding=0, rightPadding=0,
                          topPadding=0, bottomPadding=0)],
            onPage=self._cover_bg,
        )
        content = PageTemplate(
            id="content",
            frames=[Frame(LMARGIN, BMARGIN, CONTENT_W, PAGE_H - TMARGIN - BMARGIN,
                          id="nf", leftPadding=0, rightPadding=0,
                          topPadding=0, bottomPadding=0)],
            onPage=self._footer,
        )
        self.addPageTemplates([cover, content])

    def afterFlowable(self, flowable):
        if isinstance(flowable, SectionHeading):
            self.notify("TOCEntry",
                        (flowable._toc_level, flowable._toc_text,
                         self.page, flowable._toc_key))
            self.canv.bookmarkPage(flowable._toc_key)
            self.canv.addOutlineEntry(
                _strip_tags(flowable._toc_text), flowable._toc_key,
                level=flowable._toc_level, closed=(flowable._toc_level > 0))

    def _cover_bg(self, canvas, doc):
        canvas.saveState()
        canvas.setFillColor(NAVY)
        canvas.rect(0, PAGE_H - 1.0 * cm, PAGE_W, 1.0 * cm, fill=1, stroke=0)
        canvas.setFillColor(LOSS)
        canvas.rect(0, PAGE_H - 1.0 * cm, 6 * cm, 1.0 * cm, fill=1, stroke=0)
        canvas.setFillColor(NAVY)
        canvas.rect(0, 0, PAGE_W, 0.55 * cm, fill=1, stroke=0)
        canvas.restoreState()

    def _footer(self, canvas, doc):
        canvas.saveState()
        y = BMARGIN - 0.85 * cm
        canvas.setStrokeColor(PALE)
        canvas.setLineWidth(0.5)
        canvas.line(LMARGIN, y + 0.35 * cm, PAGE_W - RMARGIN, y + 0.35 * cm)
        canvas.setFont("Helvetica", 7.5)
        canvas.setFillColor(MUTED)
        canvas.drawString(LMARGIN, y, "Churn & Retention Intelligence")
        canvas.drawRightString(PAGE_W - RMARGIN, y,
                               f"Retention Review  ·  Page {doc.page}")
        canvas.setFont("Helvetica", 7)
        canvas.drawCentredString(PAGE_W / 2, y, "")
        canvas.restoreState()


def _strip_tags(s: str) -> str:
    import re
    return re.sub(r"<[^>]+>", "", s)


# ════════════════════════════════════════════════════════════
# Story assembly
# ════════════════════════════════════════════════════════════
def P(text, styles, key="body"):
    return Paragraph(text, styles[key])


def H1(text, num, key, styles, story):
    story.append(Paragraph(num, styles["h1num"]))
    story.append(SectionHeading(text, styles["h1"], 0, key))
    story.append(rule(NAVY, 1.1, 2, 12))


def H2(text, key, styles, story):
    story.append(SectionHeading(text, styles["h2"], 1, key))


def build_story(styles: dict, M: dict) -> list:
    story: list = []
    seg, chan, plan, reg = M["seg"], M["chan"], M["plan"], M["reg"]
    beh, drv, itv, segr = M["beh"], M["drv"], M["itv"], M["segr"]

    # ── COVER ──────────────────────────────────────────────
    story.append(Spacer(1, 3.2 * cm))
    story.append(P("RETENTION INTELLIGENCE REVIEW", styles, "cover_kick"))
    story.append(P("Churn &amp; Retention<br/>Intelligence", styles, "cover_title"))
    story.append(P("Where the subscription book is losing customers and revenue, "
                   "why those losses cluster where they do, and which accounts to "
                   "defend first.", styles, "cover_sub"))
    story.append(Spacer(1, 1.0 * cm))
    story.append(rule(HAIR, 0.6, 0, 12))
    story.append(P(
        f"Analytical period {M['window_start']} to {M['window_end']}  ·  "
        f"snapshot reference date {REFERENCE_DATE}<br/>"
        f"Book of {M['total']:,} lifetime accounts  ·  "
        f"{M['active']:,} active at snapshot  ·  "
        f"{usd(M['active_mrr'])} active monthly recurring revenue",
        styles, "cover_meta"))
    story.append(Spacer(1, 5.4 * cm))
    story.append(rule(HAIR, 0.6, 0, 8))
    story.append(P(
        "Retention Analytics  ·  "
        "Companion deliverable to the self-contained executive dashboard "
        "(executive-retention-command-center.html).<br/>"
        "Findings are decision-support only, not audited financial results.",
        styles, "cover_meta"))
    story.append(NextPageTemplate("content"))
    story.append(PageBreak())

    # ── TABLE OF CONTENTS ──────────────────────────────────
    story.append(P("Contents", styles, "h1"))
    story.append(rule(NAVY, 1.1, 2, 14))
    toc = TableOfContents()
    toc.levelStyles = [styles["toc1"], styles["toc2"]]
    toc.dotsMinLevel = 0
    story.append(toc)
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════
    # 1. EXECUTIVE SUMMARY
    # ════════════════════════════════════════════════════════
    H1("Executive summary", "SECTION 01", "s1", styles, story)
    story.append(P(
        f"This review reads the full subscription history of a {M['total']:,}-account "
        f"software book. Of those accounts, {M['churned']:,} have churned and "
        f"{M['active']:,} are still active at the {REFERENCE_DATE} snapshot, a "
        f"cumulative customer loss of {M['cum_churn_pct']:.1f} percent of every "
        "account the business has ever signed. The active book carries "
        f"{usd(M['active_mrr'])} of monthly recurring revenue. The question this "
        "report answers is not whether churn exists, which every subscription "
        "business lives with, but where it concentrates, what separates the "
        "accounts that leave from the accounts that stay, and how much current "
        "revenue sits behind each pattern.", styles))

    story.append(stat_band([
        (f"{M['cum_churn_pct']:.1f}%", "CUMULATIVE CUSTOMER CHURN"),
        (usd(M['active_mrr'], k=True), "ACTIVE MRR AT SNAPSHOT"),
        (f"{M['high_n'] + M['crit_n']:,}", "CRITICAL + HIGH-RISK ACCOUNTS"),
        (usd(itv['mrr_exposure_proxy'].sum(), k=True), "WEIGHTED MRR EXPOSURE"),
    ], styles))

    story.append(P(
        "Three findings carry the report. First, churn is not spread evenly. It "
        f"concentrates sharply by who the customer is and how they were acquired. "
        f"Startup-segment accounts have lost {seg.loc['Startup','cumulative_churn_share']*100:.1f} "
        f"percent of their number against {seg.loc['Enterprise','cumulative_churn_share']*100:.1f} "
        "percent for Enterprise, a gap of more than six to one. Affiliate-sourced "
        f"accounts churn at {chan.loc['Affiliate','cumulative_churn_share']*100:.1f} "
        f"percent against {chan.loc['Partner','cumulative_churn_share']*100:.1f} "
        "percent for Partner-sourced accounts. Basic-plan accounts churn at "
        f"{plan.loc['Basic','cumulative_churn_share']*100:.1f} percent against "
        f"{plan.loc['Enterprise','cumulative_churn_share']*100:.1f} percent for "
        "Enterprise plans. The book does not have a churn problem so much as it "
        "has a low-end acquisition problem that turns into a churn problem.",
        styles))

    story.append(P(
        "Second, pre-churn account health separates leavers from stayers with "
        "unusual clarity. Accounts that carry a low net promoter score churn at "
        f"{beh.loc['low_nps_flag','churn_rate_in_group']*100:.0f} percent against "
        f"{beh.loc['low_nps_flag','churn_rate_out_group']*100:.0f} percent for "
        "accounts without that flag. Accounts with a heavy recent support burden "
        f"churn at {beh.loc['high_support_ticket_flag','churn_rate_in_group']*100:.0f} "
        "percent. Failed payments, weak feature adoption, and declining usage each "
        "mark out populations that leave at several times the book rate. These "
        "signals are observed before the account closes, which is what makes them "
        "useful for prevention rather than only for post-mortem accounting.",
        styles))

    story.append(P(
        "Third, the revenue at stake is concentrated enough to act on without a "
        f"broad campaign. The top 20 percent of churned accounts by value account "
        f"for {M['conc20']:.0f} percent of all lost monthly revenue. On the "
        f"forward book, {M['crit_n']:,} accounts sit in the critical risk tier and "
        f"{M['high_n']:,} more in the high tier, together holding "
        f"{usd(M['crit_mrr'] + M['high_mrr'])} of monthly recurring revenue. Four "
        "intervention queues, ranked by weighted exposure, cover the material risk: "
        f"a renewal save desk ({usd(itv.iloc[0]['mrr_exposure_proxy'], k=True)} "
        f"weighted exposure across {int(itv.iloc[0]['candidate_customers']):,} "
        "accounts) leads, followed by payment rescue, adoption reactivation, and "
        "service recovery.", styles))

    story.append(fig("intervention_priorities.png",
                     "Figure 1. Retention plays ranked by weighted MRR exposure. The "
                     "shaded bar is the monthly revenue in scope for each play; the "
                     "solid bar is that revenue weighted by the historical churn rate "
                     "of the accounts it targets.", styles))

    story.append(P(
        "The recommended sequence follows directly from these three findings and "
        "is set out in full in Section 11. In short: stand up the renewal save desk "
        "first because it carries the largest weighted exposure and the accounts are "
        "already identifiable; fix the payment-failure path second because it is "
        "cheap to run and the affected accounts churn at more than seven times the "
        "rate of accounts that pay cleanly; and rebalance acquisition spend away "
        "from the Affiliate and Paid Search channels third, because the cheapest "
        "lasting reduction in churn is to stop acquiring the accounts most likely "
        "to leave.", styles))

    story.append(P(
        "One caveat frames everything that follows. This analysis runs on "
        "deterministic synthetic data and the risk score is a transparent "
        "prioritisation index, not a calibrated churn probability. The patterns are "
        "internally consistent, but the exact magnitudes are illustrative. Section "
        "10 states the limitations in full.", styles))
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════
    # 2. CONTEXT AND OBJECTIVES
    # ════════════════════════════════════════════════════════
    H1("Context and objectives", "SECTION 02", "s2", styles, story)
    story.append(P(
        "A subscription business lives or dies on the difference between the revenue "
        "it adds and the revenue it loses. New logos and expansion get the attention, "
        "but retention sets the ceiling on everything else: a book that loses a "
        "quarter of its accounts over its life has to refill that quarter before it "
        "grows a single point. The purpose of this work is to give the people who "
        "own that problem, the revenue leadership and the customer success "
        "organisation, a clear and defensible picture of where the book is leaking "
        "and a ranked list of where to put their hands first.", styles))

    story.append(P(
        "The economics make the case on their own. Acquiring a replacement account "
        "costs a multiple of what it costs to keep an existing one, and the "
        "replacement starts at zero tenure with none of the expansion that a "
        "retained account accrues over time. A point of churn avoided is therefore "
        "worth more than a point of new logo growth at equal cost, because the "
        "retained account is already past its acquisition spend and its riskiest "
        "early months. This is the lens the report applies throughout: every "
        "avoidable departure is measured not only as lost monthly revenue but as "
        "acquisition spend the business will have to repeat to stand still.", styles))

    H2("The book under review", "s2a", styles, story)
    story.append(P(
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
        "history.", styles))

    H2("The decisions this report supports", "s2b", styles, story)
    story.append(P(
        "The work is built to answer five questions that a retention owner actually "
        "has to make calls on, rather than to produce a general description of the "
        "data:", styles))
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
    story.append(P(
        "Everything in the findings maps back to one of these five questions. Where "
        "the data can only support an associative answer rather than a causal one, "
        "the report says so plainly rather than dressing a correlation as a cause.",
        styles))
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════
    # 3. DATA AND METHODOLOGY
    # ════════════════════════════════════════════════════════
    H1("Data and methodology", "SECTION 03", "s3", styles, story)
    story.append(P(
        "This section describes where the numbers come from and how each metric is "
        "defined, so that any figure in the report can be traced back to a rule "
        "rather than a judgement call. The pipeline is deterministic: it runs from a "
        "fixed seed, every artifact is reconstructible, and the analytical tables "
        "are governed by data contracts that are checked before any output is "
        "released.", styles))

    H2("Data sources", "s3a", styles, story)
    story.append(P(
        "Four raw tables feed the analysis. A customer table holds the acquisition "
        "attributes (segment, region, channel, signup date). A subscription table "
        "holds the plan, the monthly recurring revenue, and the start and end dates "
        "that define whether and when an account churned. A weekly product-usage "
        "table records sessions and feature interactions. A payment table records "
        "billing events and failures. These are joined into one per-account feature "
        "row measured either at the account's churn date or, for active accounts, at "
        "the snapshot reference date, so that every account is observed at a "
        "comparable point in its own life rather than at an arbitrary calendar date.",
        styles))

    story.append(data_table(
        ["Layer", "Artifact", "Grain", "Role in this report"],
        [
            ["Raw", "customers, subscriptions, usage, payments",
             "event / account", "Source of truth for all downstream tables"],
            ["Features", "customer_retention_features.csv",
             "one row per account", "Behavioural signals at churn or snapshot"],
            ["Analysis", "main_analysis_*, churn_by_*",
             "aggregate", "Trends, driver ranking, revenue at risk"],
            ["Risk", "customer_risk_scores.csv",
             "active account", "Tiered priority queue and recommended action"],
            ["Cohort", "cohort_retention_table.csv",
             "cohort × month", "Retention curves and heatmap"],
        ],
        styles,
        [2.6 * cm, 5.2 * cm, 3.0 * cm, CONTENT_W - 10.8 * cm],
    ))

    H2("How the synthetic data is built", "s3b", styles, story)
    story.append(P(
        "The data is generated, not collected, and it matters that the reader knows "
        "exactly what that means for the findings. The generator assigns each account "
        "a latent churn propensity driven by its segment, plan, channel, and a set of "
        "behavioural processes, then simulates usage, support, payment, and survey "
        "events consistent with that propensity. The result is a book in which the "
        "relationships between account health and churn are real and stable but "
        "designed rather than discovered. The report treats the magnitudes as "
        "illustrative; the metric definitions, the analytical framework, and the "
        "prioritisation logic are the transferable outputs.",
        styles))

    H2("Metric definitions", "s3c", styles, story)
    story.append(P(
        "Several numbers in this report look similar and are easily confused, so each "
        "is pinned to an explicit definition here and used consistently throughout.",
        styles))
    story.append(data_table(
        ["Metric", "Definition"],
        [
            ["Cumulative customer churn",
             "Closed accounts as a share of all accounts ever acquired. A lifetime, "
             "stock measure. Reads 27 percent here."],
            ["Monthly customer churn rate",
             "Accounts that close in a month divided by accounts active at the start "
             "of that month. A flow measure, averaging near 1 percent over the window."],
            ["Cumulative churn share (by group)",
             "Within a segment, plan, channel, or region, the share of that group's "
             "accounts that have closed."],
            ["MRR at risk",
             "Current monthly recurring revenue billed by active accounts that carry "
             "the at-risk flag at the snapshot."],
            ["Churn rate lift",
             "Churn rate inside a signal divided by churn rate outside it. A multiple, "
             "not a percentage."],
            ["Weighted MRR exposure",
             "Revenue in an intervention queue multiplied by the historical churn rate "
             "of the accounts it targets. A heuristic for sizing, not a forecast."],
            ["Retention priority score",
             "A transparent index combining behavioural risk and customer value, used "
             "only to rank the queue."],
        ],
        styles,
        [4.6 * cm, CONTENT_W - 4.6 * cm],
    ))
    story.append(P(
        "Two of these deserve emphasis. Cumulative customer churn and the monthly "
        "churn rate are different lenses on the same book and will not match: the "
        "first asks what share of everyone ever signed has left, the second asks how "
        "fast the active book is losing accounts right now. Both appear in this "
        "report and neither is wrong. Revenue loss throughout uses a monthly-value "
        "proxy, the account's recurring monthly revenue at the point it left, rather "
        "than a full contract or lifetime value, which the synthetic data does not "
        "model.", styles))

    H2("Governance and release gates", "s3d", styles, story)
    story.append(P(
        "No figure in this report or the dashboard is released until the pipeline "
        "passes its governance gates. The artifacts are checked against data "
        "contracts that assert the shape, types, ranges, and referential integrity "
        "of every table, and against a final validation suite covering data quality, "
        "metric correctness, analytical integrity, the dashboard render, and release "
        f"policy. At the current build all {M['gov_pass']} of {M['gov_total']} checks "
        "pass, which is what licenses the report to make decision-support claims "
        "rather than screening-grade ones. The gate breakdown is below; the full "
        "check log lives in outputs/tables and is regenerated on every run.", styles))
    story.append(data_table(
        ["Validation gate", "Checks passing"],
        M["gov"], styles,
        [CONTENT_W - 4.5 * cm, 4.5 * cm],
    ))
    story.append(P(
        "The release policy is deliberately conservative about what these passes "
        "entitle the analysis to claim. Clearing every gate makes the output "
        "technically valid and analytically acceptable for decision support with "
        "explicit caveats. It does not make it committee-grade, because the "
        "synthetic-data limitation in Section 10 is an unresolved caveat by "
        "construction. The governance layer encodes that distinction rather than "
        "leaving it to the reader's goodwill.", styles))
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════
    # 4. ANALYTICAL FRAMEWORK
    # ════════════════════════════════════════════════════════
    H1("Analytical framework", "SECTION 04", "s4", styles, story)
    story.append(P(
        "The analysis moves through four lenses in a deliberate order, each one "
        "narrowing the question. It begins with the trend, which tells you whether "
        "the book is getting better or worse in aggregate. It then decomposes the "
        "loss by commercial attribute and by acquisition cohort, which tells you "
        "where the loss lives. It then turns to account-level behaviour, which tells "
        "you what the leaving accounts had in common before they left. Finally it "
        "attaches revenue and a priority score to each pattern, which turns a "
        "diagnosis into a queue of accounts a team can work on Monday morning.",
        styles))

    H2("Associative, not causal", "s4a", styles, story)
    story.append(P(
        "Every relationship in this report is associative. When the report says that "
        "accounts with a low net promoter score churn at a far higher rate, it means "
        "the two co-occur in the data, not that a low score causes the departure. The "
        "low score and the departure may both be downstream of a third thing, a poor "
        "onboarding or a product gap, and the data here cannot separate those. "
        "Establishing cause would require controlled experiments, holding out a "
        "population from an intervention and measuring the difference. The framework "
        "is built so that its outputs are the right inputs to exactly those "
        "experiments: the driver ranking tells you what to test, and the weighted "
        "exposure tells you what to test first.", styles))

    story.append(P(
        "The decomposition step is not cosmetic. A single book-wide churn rate can "
        "move in the opposite direction to every one of its parts if the mix of "
        "those parts is shifting, the trap known as Simpson's paradox. A business "
        "whose every segment is improving can post a worsening aggregate simply by "
        "acquiring more of its weakest segment. Cutting the rate by segment, plan, "
        "channel, and region guards against reading a mix shift as a performance "
        "change, and it is why the cohort and concentration findings are presented "
        "next to the aggregate trend rather than in place of it.", styles))

    H2("The risk index", "s4b", styles, story)
    story.append(P(
        "Active accounts are scored on two axes and ranked on their product. The "
        "first axis is behavioural risk, built from the same pre-churn signals "
        "examined in Section 07: usage trend, feature adoption, support burden, net "
        "promoter score, and payment failures. The second axis is customer value, "
        "built from current monthly revenue and tenure. An account is worth working "
        "when it scores high on both, a high-value account showing real distress, "
        f"rather than on either alone. The mean risk score across the {M['scored']:,} "
        "scored accounts is "
        f"{M['risk_mean']:.1f} on a 0-to-100 scale, which is deliberately low: most "
        "of the book is healthy, and the index exists to surface the minority that "
        "is not. Accounts fall into four tiers, critical, high, medium, and low, and "
        "each carries a single recommended action so the output is a worklist rather "
        "than a chart.", styles))
    story.append(P(
        "The index is transparent by design. Every input is a named, inspectable "
        "signal and the combination rule is a documented formula, not a black-box "
        "model. That transparency is a deliberate trade against predictive power: a "
        "trained classifier might rank marginally better, but a customer success "
        "manager can read this score, see why an account surfaced, and act on it "
        "without a data scientist in the room. For a prioritisation queue that "
        "property is worth more than a few points of accuracy.", styles))
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════
    # 5. FINDINGS — RETENTION HEALTH AND TREND
    # ════════════════════════════════════════════════════════
    H1("Findings: retention health and trend", "SECTION 05", "s5", styles, story)
    story.append(P(
        f"Across the analytical window the book lost an average of "
        f"{M['avg_cust_churn']:.1f} percent of its active accounts per month and "
        f"{M['avg_rev_churn']:.1f} percent of its active revenue per month. That "
        "revenue churn runs below customer churn is the single most reassuring "
        "number in the report: it means the accounts leaving are worth less, on "
        "average, than the accounts staying. The book is shedding its low end and "
        "keeping its high end, which is the favourable shape of churn to have. The "
        "later sections show exactly where that low end sits.", styles))

    story.append(fig("churn_rate_trend.png",
                     "Figure 2. Monthly customer and revenue churn rates across the "
                     "analytical window. The revenue line sits consistently below the "
                     "customer line, the signature of low-value accounts leading the "
                     "losses.", styles))

    story.append(P(
        "The level is healthy but the recent direction is not. Monthly customer "
        f"churn in the final observed month reads {M['last_cust_churn']:.1f} percent "
        f"against a prior nine-month average of {M['prior9_churn']:.1f} percent. The "
        "last three months break clearly above the trend that preceded them. Two "
        "readings are possible and the report holds both. The operational reading is "
        "that something real has shifted in the most recent quarter and the book is "
        "losing accounts faster than it was. The measurement reading is that accounts "
        "closing near the snapshot date are mechanically over-represented because the "
        "window is right-censored, so the final months are not yet fully comparable "
        "to settled history. Section 10 returns to this. Either way the recent "
        "months warrant attention rather than alarm.", styles))

    story.append(fig("recent_churn_acceleration.png",
                     "Figure 3. Monthly customer churn rate over the last twelve "
                     "months. The final three months (highlighted) sit well above the "
                     "prior nine-month average shown by the dotted line.", styles))

    story.append(P(
        "Underneath the rates, the active book has grown steadily in revenue terms "
        f"to {usd(M['active_mrr'])} per month at the snapshot, so the business is "
        "adding more than it is losing. The retention question is therefore not "
        "about survival but about efficiency: every point of avoidable churn is a "
        "point the acquisition engine has to refill before the book grows, and the "
        "next sections show that a large share of the current loss is concentrated "
        "in places where it could be reduced.", styles))

    story.append(fig("mrr_trend.png",
                     "Figure 4. Active monthly recurring revenue by month. The book "
                     "has compounded steadily; the retention task is to protect the "
                     "base, not to rescue a business in decline.", styles))

    H2("The shape of the loss", "s5a", styles, story)
    story.append(P(
        f"The gap between customer churn ({M['avg_cust_churn']:.1f} percent a month "
        f"on average) and revenue churn ({M['avg_rev_churn']:.1f} percent) is worth "
        "reading deliberately, because its sign decides the whole posture of a "
        "retention programme. When revenue churn runs below customer churn, the "
        "average departing account is worth less than the average retained one, and "
        "the book is upgrading its quality even as it loses count. That is the case "
        "here, and it is the opposite of the dangerous pattern, where a business "
        "loses a few large accounts and posts low customer churn against high revenue "
        "churn. A retention owner who saw the dangerous pattern would drop everything "
        "to defend the top accounts; the owner here can instead run an efficient, "
        "mostly automated programme against a large low-value tail and reserve "
        "human effort for the genuine high-value exceptions.", styles))
    story.append(P(
        "This shape also sets expectations for lifetime value. Because the accounts "
        "leaving are concentrated in the low-revenue, low-tenure end, the surviving "
        "book skews toward longer-lived, higher-value accounts, and blended lifetime "
        "value rises mechanically as the weak tail churns out. The risk in such a "
        "book is complacency: a healthy revenue-retention headline can mask a real "
        "and growing customer-count problem that will eventually starve the funnel. "
        "The cohort and concentration findings that follow are the check against "
        "that complacency.", styles))
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════
    # 6. FINDINGS — COHORT RETENTION
    # ════════════════════════════════════════════════════════
    H1("Findings: cohort retention", "SECTION 06", "s6", styles, story)
    story.append(P(
        "Aggregate churn rates hide whether the business is getting better at keeping "
        "the customers it acquires. Cohort analysis answers that by following each "
        "monthly intake of new accounts across its own life and comparing intakes at "
        "the same age. If recent cohorts retain better than older ones at month six, "
        "onboarding and product-market fit are improving. If they retain worse, the "
        "business is acquiring lower-quality accounts or onboarding them less well, "
        "and the aggregate rate will deteriorate later even if it looks stable now.",
        styles))

    story.append(fig("cohort_retention_curves.png",
                     "Figure 5. Retention by cohort age for the eight most recent "
                     "monthly cohorts. Each line follows one acquisition month as its "
                     "accounts age; the most recent cohorts are drawn most prominently.",
                     styles))

    story.append(P(
        "Read at a fixed age, the curves bend the wrong way. Early cohorts hold "
        "almost all of their accounts through the first half-year; more recent "
        "cohorts start to separate downward at the same ages. The deterioration is "
        "modest in absolute terms, six-month retention still reads near 98 to 99 "
        "percent, but the direction is consistent and it is the direction that "
        "matters, because a cohort that retains one point worse at month six "
        "typically retains several points worse by the end of its second year. The "
        "honest qualifier is that the most recent cohorts are right-censored: they "
        "have not yet lived long enough to be compared fairly at the longer horizons, "
        "so the long-run gap should be read as a leading indicator to monitor rather "
        "than a settled fact.", styles))

    story.append(fig("cohort_retention_heatmap.png",
                     "Figure 6. Cohort retention heatmap. Each row is an acquisition "
                     "month, each column a month of age, and each cell the share of "
                     "the original cohort still active. Green is healthy retention, "
                     "red is loss; the gradient down the early columns is where "
                     "onboarding quality shows up.", styles))

    story.append(P(
        "The heatmap makes the pattern legible at a glance. The strongest retention "
        "sits in the upper rows, the older cohorts, and the early-age columns. As the "
        "eye moves down toward the recent cohorts, the early-age cells lose a little "
        "of their green. This is the same story the curves tell, shown as a surface "
        "rather than as lines, and it points the same way: the acquisition mix has "
        "been drifting toward accounts that are slightly harder to keep, which "
        "connects directly to the channel and segment findings in the next section.",
        styles))
    story.append(P(
        "One distinction matters for how this finding is used. Logo retention, the "
        "share of accounts still active, deteriorates a little faster than revenue "
        "retention, the share of original cohort revenue still billing, because the "
        "accounts leaving the recent cohorts are disproportionately the smaller ones. "
        "Six-month revenue retention holds near 99 percent even where logo retention "
        "slips below it. That is the same favourable shape seen in the aggregate "
        "trend, and it tells the retention owner that the cohort drift is a problem "
        "of small-account volume rather than an erosion of the cohorts' revenue "
        "spine. The fix belongs upstream in acquisition and onboarding, not in a "
        "high-touch save motion aimed at the recent cohorts.", styles))
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════
    # 7. FINDINGS — WHERE CHURN CONCENTRATES
    # ════════════════════════════════════════════════════════
    H1("Findings: where churn concentrates", "SECTION 07", "s7", styles, story)
    story.append(P(
        "This is the section that turns a number into a target. The aggregate churn "
        "rate is an average over populations that behave nothing like each other, and "
        "averaging them together hides the only thing a retention team can act on: "
        "which populations are doing the leaving. Decomposed by segment, plan, "
        "channel, and region, the loss is strikingly uneven, and the unevenness lines "
        "up across the cuts in a way that points to a single underlying story.",
        styles))

    H2("By customer segment", "s7a", styles, story)
    story.append(P(
        f"Segment is the sharpest single cut. Startup accounts have lost "
        f"{seg.loc['Startup','cumulative_churn_share']*100:.1f} percent of their "
        f"number and SMB accounts {seg.loc['SMB','cumulative_churn_share']*100:.1f} "
        f"percent, against {seg.loc['Mid-Market','cumulative_churn_share']*100:.1f} "
        f"percent for Mid-Market and just "
        f"{seg.loc['Enterprise','cumulative_churn_share']*100:.1f} percent for "
        "Enterprise. The gradient is monotonic in account size: the smaller the "
        "customer, the more likely it is to leave. The same gradient runs through "
        "average revenue, where Enterprise accounts bill "
        f"{usd(seg.loc['Enterprise','avg_monthly_revenue'])} a month against "
        f"{usd(seg.loc['Startup','avg_monthly_revenue'])} for Startup, and through "
        "the health signals examined below. Small accounts churn more and are worth "
        "less individually, which is why the revenue line sits below the customer "
        "line in the trend.", styles))

    story.append(fig("churn_by_segment.png",
                     "Figure 7. Cumulative churn share by customer segment. The "
                     "gradient is monotonic in account size, from Enterprise at the "
                     "low end to Startup at the high end.", styles, width_frac=0.92))

    story.append(P(
        "The segment story is not only about churn rates; it is about the whole "
        "health profile. The comparison below puts cumulative churn next to average "
        "net promoter score and the average usage trend for each segment. Enterprise "
        "accounts churn least, score highest on net promoter, and show usage that is "
        "still growing. Startup accounts invert all three. The signals are not "
        "independent symptoms; they move together, which is what makes a single "
        "segment-level health read meaningful.", styles))

    story.append(fig("segment_health_comparison.png",
                     "Figure 8. Segment health on three axes: cumulative churn share, "
                     "average net promoter score, and average usage trend. A segment "
                     "that is losing accounts is also the segment with weak sentiment "
                     "and declining usage.", styles))

    H2("By plan type", "s7b", styles, story)
    story.append(P(
        f"Plan tier tells the same story in pricing terms. Basic-plan accounts churn "
        f"at {plan.loc['Basic','cumulative_churn_share']*100:.1f} percent, nearly ten "
        f"times the {plan.loc['Enterprise','cumulative_churn_share']*100:.1f} percent "
        "rate of Enterprise-plan accounts, with Growth and Pro stepping down in "
        "between. Lower-priced plans attract lower-commitment buyers, and the data "
        "shows it. The commercial implication is not to abandon the entry tier, which "
        "feeds the funnel, but to recognise that a Basic-plan signup is a different "
        "and more fragile asset than a Pro-plan signup and should be onboarded and "
        "monitored as one.", styles))

    story.append(fig("churn_by_plan.png",
                     "Figure 9. Cumulative churn share by plan type. Churn falls "
                     "steeply as plan tier rises.", styles, width_frac=0.92))

    H2("By acquisition channel", "s7c", styles, story)
    story.append(P(
        f"Channel is where the analysis becomes directly actionable on spend. "
        f"Affiliate-sourced accounts churn at "
        f"{chan.loc['Affiliate','cumulative_churn_share']*100:.1f} percent and "
        f"Paid Search at {chan.loc['Paid Search','cumulative_churn_share']*100:.1f} "
        f"percent, against {chan.loc['Partner','cumulative_churn_share']*100:.1f} "
        f"percent for Partner and "
        f"{chan.loc['Referral','cumulative_churn_share']*100:.1f} percent for "
        "Referral. The accounts a business pays a third party to send it leave at "
        "roughly two to three times the rate of the accounts that arrive through a "
        "partner or a referral. This is the most controllable finding in the report, "
        "because acquisition mix is a budget decision. Shifting marginal spend from "
        "Affiliate and Paid Search toward Partner and Referral would lower the "
        "blended churn rate of every future cohort without touching a single "
        "retention playbook.", styles))

    story.append(fig("churn_by_channel.png",
                     "Figure 10. Cumulative churn share by acquisition channel. "
                     "Paid and affiliate channels (red) deliver the least durable "
                     "accounts; partner and referral channels (green) the most "
                     "durable.", styles, width_frac=0.92))

    H2("By region", "s7d", styles, story)
    story.append(P(
        f"Region is the weakest of the four cuts but not a flat one. LATAM accounts "
        f"churn at {reg.loc['LATAM','cumulative_churn_share']*100:.1f} percent and "
        f"APAC at {reg.loc['APAC','cumulative_churn_share']*100:.1f} percent, both "
        f"above the {M['cum_churn_pct']:.1f} percent book average, while North "
        f"America sits below it at "
        f"{reg.loc['North America','cumulative_churn_share']*100:.1f} percent. The "
        "spread is narrower than for segment or channel, and a good part of it "
        "probably reflects which segments and channels dominate each region rather "
        "than geography itself. Region is therefore a place to watch and to read "
        "alongside the other cuts, not a primary lever on its own.", styles))

    story.append(fig("churn_by_region.png",
                     "Figure 11. Cumulative churn share by region against the book "
                     "average. LATAM and APAC run hot; North America runs cool.",
                     styles, width_frac=0.92))

    story.append(P(
        "Taken together the four cuts tell one story from four angles. The accounts "
        "that leave are smaller, on cheaper plans, more often acquired through paid "
        "and affiliate channels, and somewhat concentrated outside North America. "
        "These are not four problems; they are four views of the same low-commitment "
        "population, and they explain both why revenue churn sits below customer "
        "churn and why the cohort mix has been drifting. The next section asks what "
        "those accounts looked like in the weeks before they left.", styles))
    story.append(P(
        "One word of caution carries over from the framework in Section 04. Because "
        "the four cuts overlap, they cannot simply be added. Affiliate traffic skews "
        "toward Startup and SMB accounts on Basic plans, so part of the channel "
        "effect is really the segment and plan effect wearing a channel label, and "
        "part of the regional spread is which segments and channels dominate each "
        "region. Separating the independent contribution of each attribute would take "
        "a model that holds the others constant, which is the natural next step "
        "beyond this descriptive cut. For the decisions at hand the overlap does not "
        "change the conclusion: the levers point the same way whether the channel "
        "effect is direct or inherited, because shifting spend toward partner and "
        "referral also shifts the segment and plan mix toward the durable end.",
        styles))
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════
    # 8. FINDINGS — BEHAVIOURAL CHURN DRIVERS
    # ════════════════════════════════════════════════════════
    H1("Findings: behavioural churn drivers", "SECTION 08", "s8", styles, story)
    story.append(P(
        "Commercial attributes say which accounts are structurally fragile. "
        "Behavioural signals say which accounts are in trouble right now, and they do "
        "it with the kind of separation that is rare in real data and worth examining "
        "carefully here. Each signal is a flag a customer success team can see in its "
        "own tools: a low recent net promoter score, a spike in support tickets, a "
        "failed payment, weak feature adoption, or a decline in usage.", styles))

    story.append(fig("signal_separation.png",
                     "Figure 12. Churn rate for accounts with and without each "
                     "pre-churn signal. The gap between the two dots is the whole "
                     "point: these flags split the book into populations that behave "
                     "almost nothing alike.", styles))

    story.append(P(
        f"The separation is stark. Accounts carrying a low net promoter score churn "
        f"at {beh.loc['low_nps_flag','churn_rate_in_group']*100:.0f} percent against "
        f"{beh.loc['low_nps_flag','churn_rate_out_group']*100:.0f} percent for "
        "accounts without it. Accounts with a heavy support burden churn at "
        f"{beh.loc['high_support_ticket_flag','churn_rate_in_group']*100:.0f} percent "
        f"against {beh.loc['high_support_ticket_flag','churn_rate_out_group']*100:.0f} "
        "percent. Failed payments mark a population that churns at "
        f"{beh.loc['failed_payment_flag','churn_rate_in_group']*100:.0f} percent, "
        "weak feature adoption "
        f"{beh.loc['low_feature_adoption_flag','churn_rate_in_group']*100:.0f} "
        "percent, and declining usage "
        f"{beh.loc['usage_decline_flag','churn_rate_in_group']*100:.0f} percent. "
        "Expressed as a multiple, a low net promoter score lifts the churn rate by "
        f"more than {beh.loc['low_nps_flag','churn_rate_lift']:.0f} times and a heavy "
        f"support burden by more than "
        f"{beh.loc['high_support_ticket_flag','churn_rate_lift']:.0f} times relative "
        "to accounts without the flag.", styles))

    story.append(P(
        "These multiples are large partly because the comparison group, accounts "
        "without any distress flag, almost never churns in this book. That is a "
        "property of synthetic data and the report does not lean on the exact "
        "magnitudes. What survives the caveat is the ranking and the practical "
        "consequence: sentiment and support signals identify near-certain churn, "
        "while usage decline identifies a much larger but lower-certainty population. "
        "A retention programme should treat the two differently, reserving expensive "
        "human save motions for the high-certainty sentiment and payment flags and "
        "using cheaper automated nudges for the broad usage-decline population.",
        styles))

    story.append(P(
        "Expressed as a multiple against accounts without each flag, the same five "
        "signals rank in a stable order. Low sentiment and high support burden sit "
        "far out at the top, payment failure in the middle, and weak adoption and "
        "usage decline closer to the no-lift line, though still well above it. The "
        "lift view and the absolute-separation view agree on the ranking and "
        "disagree only on emphasis, which is the reassurance that the ordering is a "
        "property of the data rather than of the chosen comparison.", styles))
    story.append(fig("behavioral_churn_drivers.png",
                     "Figure 13. Churn rate lift by behavioural signal, the ratio of "
                     "the churn rate inside each signal to the rate outside it. Bars "
                     "past the dotted no-lift line mark signals that raise the rate; "
                     "the strongest sit several times above it.", styles,
                     width_frac=0.95))

    H2("Ranking the drivers by money, not just by rate", "s8a", styles, story)
    story.append(P(
        "A high lift on a tiny population is less important than a moderate lift on a "
        "large and valuable one. The ranking below therefore scores each driver by "
        "its excess MRR association: the monthly revenue tied to churned accounts "
        "carrying the signal, above what the book-average churn rate would predict. "
        "This reorders the list. Support burden and low sentiment stay near the top "
        "because they combine a high rate with real revenue, but usage decline rises "
        "sharply, because although its lift is the smallest of the behavioural "
        "signals it touches the most accounts and the most money.", styles))

    story.append(fig("churn_driver_ranking.png",
                     "Figure 14. Churn drivers ranked by excess MRR association. "
                     "Behavioural signals (red) dominate the structural attributes "
                     "(slate); the label shows each driver's churn-rate multiple.",
                     styles))

    drv_top = drv.sort_values("excess_mrr_association_proxy", ascending=False).head(5)
    story.append(data_table(
        ["Driver", "Accounts", "Churn rate", "Lift", "Excess MRR assoc."],
        [[
            r["driver"].replace("_flag", "").replace("_", " ").title()
                       .replace("Nps", "NPS"),
            f"{int(r['impacted_customers']):,}",
            f"{r['churn_rate']*100:.0f}%",
            f"{r['churn_rate_lift']:.1f}×",
            usd(r["excess_mrr_association_proxy"]),
        ] for _, r in drv_top.iterrows()],
        styles,
        [5.4 * cm, 2.2 * cm, 2.4 * cm, 1.8 * cm, CONTENT_W - 11.8 * cm],
    ))
    story.append(P(
        "The five behavioural signals at the top of this table are the foundation of "
        "the risk index in Section 04 and of the intervention queues in Section 11. "
        "They are also the natural candidates for the controlled experiments the "
        "framework calls for: each one defines a population, a plausible "
        "intervention, and a revenue stake large enough to justify the test.",
        styles))
    story.append(P(
        "The signals also compound. An account that is both paying late and filing "
        "support tickets is in a different state from one carrying a single flag, and "
        "the risk index in Section 04 is built to capture that by scoring on the full "
        "signal set rather than on any one flag. In operational terms this means the "
        "save desk should not work five separate lists, one per signal, with the same "
        "account appearing on several. It should work one ranked list in which the "
        "multiply-flagged accounts rise to the top, and reserve its most expensive "
        "human contact for them. The single-flag, usage-decline-only accounts are "
        "the large base of the queue and belong in an automated motion, not a "
        "phone call.", styles))
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════
    # 9. FINDINGS — REVENUE AT RISK
    # ════════════════════════════════════════════════════════
    H1("Findings: revenue at risk and concentration", "SECTION 09", "s9", styles, story)
    story.append(P(
        "Counting accounts tells you how many customers you are losing; it does not "
        "tell you how much money is leaving or whether it is worth a targeted defence. "
        "This section attaches revenue to the risk and asks the question that decides "
        "the operating model: is the exposure concentrated enough to defend account "
        "by account, or so diffuse that only a broad programme could touch it?",
        styles))

    story.append(P(
        "The answer is that it is highly concentrated. Ranking churned accounts by "
        f"value, the top tenth carries {M['conc10']:.0f} percent of all lost monthly "
        f"revenue, the top fifth carries {M['conc20']:.0f} percent, and the top third "
        f"carries {M['conc30']:.0f} percent. By the halfway point of churned accounts "
        f"by value, {M['conc50']:.0f} percent of all lost monthly revenue is already "
        "accounted for. A small set of departures does most of the financial damage, "
        "which is the "
        "single most important fact for designing the response: a focused save desk "
        "working a few hundred named accounts can address the majority of the "
        "revenue at stake.", styles))

    story.append(fig("revenue_concentration.png",
                     "Figure 15. Concentration of lost monthly value across churned "
                     "accounts. The curve bows far above the line of even "
                     "distribution; the marked point shows the top fifth of accounts "
                     "carrying the majority of the loss.", styles))

    H2("Where the at-risk revenue sits", "s9a", styles, story)
    story.append(P(
        "On the forward book, the same concentration appears across segments. The "
        "comparison below sets each segment's current at-risk MRR against the "
        "monthly value it has already lost to churn. SMB carries the largest current "
        f"at-risk MRR at {usd(segr.loc['SMB','current_mrr_at_risk'])}, reflecting its "
        "size, while Enterprise, despite churning the fewest accounts, still carries "
        f"{usd(segr.loc['Enterprise','current_mrr_at_risk'])} of at-risk MRR because "
        "each Enterprise account is worth so much that even a handful in distress "
        "represents real money. This is why value has to sit alongside risk in the "
        "prioritisation: a single wavering Enterprise account can outweigh a dozen "
        "Startup accounts.", styles))

    story.append(fig("revenue_at_risk_by_segment.png",
                     "Figure 16. Current at-risk MRR against churned monthly value by "
                     "segment. SMB leads on current exposure; Enterprise punches above "
                     "its account count on value per account.", styles))

    srows = []
    for s in ["Enterprise", "Mid-Market", "SMB", "Startup"]:
        if s in segr.index:
            r = segr.loc[s]
            srows.append([
                s, f"{int(r['at_risk_customers']):,}",
                usd(r["current_mrr_at_risk"]),
                f"{int(r['churned_customers']):,}",
                usd(r["churned_monthly_value_proxy"]),
            ])
    story.append(data_table(
        ["Segment", "At-risk accts", "At-risk MRR", "Churned accts",
         "Churned monthly value"],
        srows, styles,
        [3.4 * cm, 2.8 * cm, 2.8 * cm, 2.8 * cm, CONTENT_W - 11.8 * cm],
    ))

    H2("The risk tiers", "s9b", styles, story)
    story.append(P(
        f"The risk index sorts the {M['scored']:,} active accounts into four tiers. "
        f"The critical tier holds {M['crit_n']:,} accounts billing "
        f"{usd(M['crit_mrr'])} a month, the high tier {M['high_n']:,} accounts "
        f"billing {usd(M['high_mrr'])}, and the medium tier {M['med_n']:,} accounts "
        f"billing {usd(M['med_mrr'])}. The critical and high tiers together, "
        f"{M['crit_n'] + M['high_n']:,} accounts and "
        f"{usd(M['crit_mrr'] + M['high_mrr'])} of MRR, are small enough for a team to "
        "work by hand and large enough to matter. They are the spine of the priority "
        "queue.", styles))

    story.append(fig("risk_tier_breakdown.png",
                     "Figure 17. Account count and MRR exposure by risk tier. The "
                     "critical and high tiers are a workable number of accounts "
                     "holding a material share of exposed revenue.", styles))

    story.append(P(
        "The score distribution behind those tiers confirms that the book is mostly "
        "healthy and the risk is a tail, not a bulk. The low tier dominates the count "
        "and clusters at the bottom of the score range; the critical and high tiers "
        "are a thin, well-separated tail at the top. That shape is exactly what a "
        "prioritisation index should produce: it is not labelling half the book as "
        "at risk, it is isolating the minority that genuinely is.", styles))

    story.append(fig("risk_score_distribution.png",
                     "Figure 18. Distribution of churn risk scores by tier. The mass "
                     "sits in the healthy low tier; the at-risk tiers form a distinct "
                     "upper tail.", styles))
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════
    # 10. RISKS, LIMITATIONS, CAVEATS
    # ════════════════════════════════════════════════════════
    H1("Risks, limitations, and caveats", "SECTION 10", "s10", styles, story)
    story.append(P(
        "A report that hides its weaknesses is less useful than one that names them, "
        "because the reader cannot judge how far to trust a finding without knowing "
        "what could undermine it. Five limitations bound everything above and each "
        "changes how a specific finding should be read.", styles))

    H2("The data is synthetic", "s10a", styles, story)
    story.append(P(
        "The book is generated from a fixed seed, not collected from a live business. "
        "The relationships are real and stable but designed, so the magnitudes are "
        "illustrative. The behavioural lifts in Section 08 in particular are larger "
        "than a real book would show, because the comparison group of fully healthy "
        "accounts almost never churns here. The method, the pipeline, the metric "
        "definitions, and the prioritisation logic carry over to real data without "
        "change; the specific percentages do not.", styles))

    H2("The recent months are right-censored", "s10b", styles, story)
    story.append(P(
        "The apparent acceleration in the final three months of the trend, where the "
        f"rate climbs to {M['last_cust_churn']:.1f} percent, is partly a measurement "
        "artifact. Accounts that close near the snapshot are mechanically "
        "over-represented relative to accounts that will close later but have not "
        "yet, so the tail of the window overstates the settled rate. The finding to "
        "carry forward is that the recent months merit monitoring, not that the book "
        "has entered a churn crisis.", styles))

    H2("Revenue loss uses a monthly-value proxy", "s10c", styles, story)
    story.append(P(
        "Every revenue figure is built from the account's recurring monthly revenue "
        "at the point it left or at the snapshot, not from contract value or "
        "modelled lifetime value. This understates the true cost of losing a "
        "long-contract Enterprise account and overstates the relative importance of "
        "easily-replaced monthly accounts. The concentration finding in Section 09 is "
        "robust to this, since both ends use the same proxy, but absolute exposure "
        "figures should be read as monthly run-rate, not annual contract value.",
        styles))

    H2("Relationships are associative", "s10d", styles, story)
    story.append(P(
        "No finding in this report establishes cause. A low net promoter score does "
        "not necessarily cause a departure; it co-occurs with it. The intervention "
        "queues in the next section are therefore presented as hypotheses to be "
        "validated with controlled tests, not as guaranteed returns. The weighted "
        "exposure numbers size the prize; they do not promise it.", styles))

    H2("Tiering depends on the chosen thresholds", "s10e", styles, story)
    story.append(P(
        "The value tiers and risk cut-points are quantile-based and would need "
        "recalibration on a real distribution. Moving a threshold moves accounts "
        "between tiers and changes the headline counts. The ranking is stable to "
        "these choices; the exact tier populations are not, and any operational "
        "commitment to a tier size should be set against real data.", styles))

    H2("The book is observed at a single snapshot", "s10f", styles, story)
    story.append(P(
        "Every active account is measured at one date, the reference snapshot, rather "
        "than followed continuously. That keeps the cross-sectional comparison clean, "
        "because every account is read at a comparable point in its own life, but it "
        "means the analysis cannot see seasonality, recovery, or the path an account "
        "took to its current state. An account that spiked on support tickets last "
        "month and has since settled looks the same at the snapshot as one still "
        "deteriorating. A live deployment would refresh the snapshot on a schedule "
        "and watch the trajectory between refreshes, which would sharpen the risk "
        "index and catch recoveries the single snapshot misses.", styles))
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════
    # 11. RECOMMENDATIONS
    # ════════════════════════════════════════════════════════
    H1("Recommendations and action priorities", "SECTION 11", "s11", styles, story)
    story.append(P(
        "The findings converge on a short, ordered list. The ordering is by weighted "
        "exposure and by speed to value, not by how interesting the finding is, and "
        "each recommendation names the finding it rests on, the population it "
        "targets, and the metric that would prove it worked.", styles))

    story.append(P(
        "The four intervention queues that anchor the priorities are sized below. "
        "Read the weighted-exposure column as the order of magnitude of monthly "
        "revenue each play is defending, and the scope column as the gross revenue "
        "passing through the queue. The gap between the two reflects the historical "
        "churn rate of the targeted accounts: the renewal save desk works a large "
        "book at a moderate rate, while payment rescue works a small book at a very "
        "high rate, which is why the second is the stronger return on effort even "
        "though the first defends more revenue in absolute terms.", styles))
    itv_rows = []
    for _, r in itv.iterrows():
        itv_rows.append([
            r["opportunity"],
            f"{int(r['candidate_customers']):,}",
            usd(r["current_mrr_scope"]),
            f"{r['historical_churn_share']*100:.0f}%",
            usd(r["mrr_exposure_proxy"]),
        ])
    story.append(data_table(
        ["Intervention queue", "Accounts", "MRR in scope",
         "Hist. churn", "Weighted exposure"],
        itv_rows, styles,
        [5.0 * cm, 2.2 * cm, 2.8 * cm, 2.2 * cm, CONTENT_W - 12.2 * cm],
    ))

    recs = [
        ("P1", "Stand up a renewal save desk for Mid-Market and SMB",
         f"This play carries the largest weighted exposure in the book at "
         f"{usd(itv.iloc[0]['mrr_exposure_proxy'])}, across "
         f"{int(itv.iloc[0]['candidate_customers']):,} accounts holding "
         f"{usd(itv.iloc[0]['current_mrr_scope'])} of MRR in scope, with the focus on "
         "Mid-Market and SMB where the value and the volume meet (Section 09, Figure "
         "1). Work the critical and high risk tiers first, lead with the accounts "
         "flagged on sentiment and support, and run a pre-renewal save playbook with "
         "tailored offers. Measure success as the saved MRR among contacted accounts "
         "against a held-out control, not as gross contact volume."),
        ("P1", "Fix the payment-failure path",
         f"Accounts with a failed payment churn at "
         f"{beh.loc['failed_payment_flag','churn_rate_in_group']*100:.0f} percent, "
         f"more than {beh.loc['failed_payment_flag','churn_rate_lift']:.0f} times the "
         "rate of accounts that pay cleanly (Section 08). The payment-rescue queue is "
         f"only {int(itv.iloc[1]['candidate_customers']):,} accounts but carries "
         f"{usd(itv.iloc[1]['mrr_exposure_proxy'])} of weighted exposure, and the fix "
         "is cheap: dunning sequencing, card-update prompts, and retry logic. This is "
         "the highest return-on-effort action in the report and should ship in "
         "parallel with the save desk. Measure recovered MRR per failed-payment "
         "account."),
        ("P2", "Rebalance acquisition spend away from the weakest channels",
         f"Affiliate and Paid Search accounts churn at "
         f"{chan.loc['Affiliate','cumulative_churn_share']*100:.1f} and "
         f"{chan.loc['Paid Search','cumulative_churn_share']*100:.1f} percent against "
         f"{chan.loc['Partner','cumulative_churn_share']*100:.1f} percent for Partner "
         "and below 20 percent for Referral (Section 07, Figure 10). Shifting "
         "marginal budget toward partner and referral motions lowers the blended "
         "churn rate of every future cohort with no retention effort at all. Measure "
         "the six-month retention of new cohorts by channel and track the blended "
         "rate down."),
        ("P2", "Run an adoption-reactivation programme for the usage-decline tail",
         f"Usage decline touches {int(beh.loc['usage_decline_flag','customers_in_group']):,} "
         "accounts, the largest behavioural population, at a lower per-account "
         "certainty than the sentiment flags (Section 08). It suits a cheaper, "
         "automated motion: onboarding refreshers, feature nudges, and success "
         f"check-ins. The reactivation queue carries "
         f"{usd(itv.iloc[2]['mrr_exposure_proxy'])} of weighted exposure across "
         f"{int(itv.iloc[2]['candidate_customers']):,} accounts. Measure the share of "
         "flagged accounts that return to a positive usage trend."),
        ("P3", "Treat low-tier and Basic-plan signups as a distinct, instrumented class",
         f"Basic-plan accounts churn at "
         f"{plan.loc['Basic','cumulative_churn_share']*100:.1f} percent (Section 07). "
         "Rather than a save motion, this calls for a product and onboarding fix: a "
         "guided first-value path for entry-tier accounts and clearer upgrade prompts "
         "for the ones that show real engagement. Measure the rate at which Basic "
         "accounts either reach an activation milestone or upgrade within ninety days."),
        ("P3", "Convert the top drivers into controlled experiments",
         "Because every relationship here is associative (Section 10), the driver "
         "ranking should feed a small experiment programme: hold out a control for "
         "each of the top intervention queues and measure the lift directly. This "
         "turns the weighted-exposure estimates into known returns and protects the "
         "budget from chasing a correlation that does not respond to treatment."),
    ]
    for tag, title, bodytext in recs:
        story.append(KeepTogether([
            Paragraph(
                f'<font color="#B83530"><b>{tag}</b></font>&nbsp;&nbsp;'
                f'<font color="#1B3A6B"><b>{title}</b></font>',
                styles["h3"]),
            P(bodytext, styles),
        ]))

    story.append(Spacer(1, 4))
    story.append(P(
        "Sequenced this way, the first two actions defend the revenue already in "
        "distress and ship within a quarter; the middle two reduce the inflow of "
        "fragile accounts and the size of the at-risk tail over the following two "
        "quarters; and the last two harden the programme so its returns are measured "
        "rather than assumed. None of the six depends on a tool the team does not "
        "already have, and all six trace to a quantified finding in this report.",
        styles))

    story.append(SectionHeading("Sequencing, ownership, and success measures",
                                styles["h3"], 1, "s11seq"))
    story.append(P(
        "The plan resolves to the following sequence. Each action carries one owner, "
        "one horizon, and one measure that settles whether it worked, so the "
        "programme can be governed on outcomes rather than activity.", styles))
    story.append(data_table(
        ["#", "Action", "Owner", "Horizon", "Success measure"],
        [
            ["P1", "Renewal save desk", "Customer Success", "0–90 days",
             "Saved MRR vs held-out control"],
            ["P1", "Payment-failure path", "Billing / RevOps", "0–60 days",
             "Recovered MRR per failed-payment account"],
            ["P2", "Acquisition rebalance", "Marketing", "1–2 quarters",
             "6-month retention of new cohorts by channel"],
            ["P2", "Adoption reactivation", "Customer Success", "1–2 quarters",
             "Share of flagged accounts returning to positive usage"],
            ["P3", "Basic-plan onboarding fix", "Product", "2 quarters",
             "90-day activation or upgrade rate"],
            ["P3", "Driver experiments", "Analytics", "Ongoing",
             "Measured lift vs control per queue"],
        ],
        styles,
        [1.0 * cm, 4.2 * cm, 3.0 * cm, 2.2 * cm, CONTENT_W - 10.4 * cm],
    ))
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════
    # 12. APPENDIX
    # ════════════════════════════════════════════════════════
    H1("Appendix", "SECTION 12", "s12", styles, story)

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
                rows.append([label, k, f"{int(r['customers']):,}",
                             f"{int(r['churned_customers']):,}",
                             f"{r['cumulative_churn_share']*100:.1f}%",
                             usd(r['avg_monthly_revenue'])])
    story.append(data_table(
        ["Cut", "Group", "Accounts", "Churned", "Churn share", "Avg MRR"],
        rows, styles,
        [2.2 * cm, 3.4 * cm, 2.2 * cm, 2.0 * cm, 2.6 * cm, CONTENT_W - 12.4 * cm],
    ))

    H2("B. Full behavioural driver detail", "s12b", styles, story)
    brows = []
    for k, r in beh.iterrows():
        brows.append([
            k.replace("_flag", "").replace("_", " ").title().replace("Nps", "NPS"),
            f"{int(r['customers_in_group']):,}",
            f"{r['churn_rate_in_group']*100:.0f}%",
            f"{r['churn_rate_out_group']*100:.0f}%",
            f"{r['churn_rate_lift']:.1f}×",
        ])
    story.append(data_table(
        ["Signal", "Accounts", "Churn w/ signal", "Churn w/o signal", "Lift"],
        brows, styles,
        [5.6 * cm, 2.6 * cm, 3.2 * cm, 3.4 * cm, CONTENT_W - 14.8 * cm],
    ))

    H2("C. Recommended-action distribution across the scored book", "s12c", styles, story)
    arows = [[k, f"{v:,}", f"{v / M['scored'] * 100:.1f}%"]
             for k, v in M["rec_counts"].items()]
    story.append(data_table(
        ["Recommended action", "Accounts", "Share of scored book"],
        arows, styles,
        [7.0 * cm, 3.0 * cm, CONTENT_W - 10.0 * cm],
    ))

    H2("D. Monthly retention trend, last twelve months", "s12d2", styles, story)
    story.append(P(
        "The active book at the start of each month, the accounts and revenue lost "
        "during it, and the resulting churn rates. The customer and revenue churn "
        "columns are the data behind Figures 2 and 3; the right-censoring caveat in "
        "Section 10 applies to the final rows.", styles))
    story.append(data_table(
        ["Month", "Active accts", "Active MRR", "Churned", "Cust. churn", "Rev. churn"],
        M["trend12"], styles,
        [2.8 * cm, 2.4 * cm, 2.6 * cm, 2.0 * cm, 2.4 * cm, CONTENT_W - 12.2 * cm],
    ))

    H2("E. Figure index", "s12d", styles, story)
    story.append(P(
        "Figures 1 through 18 are the static export of the chart pack in "
        "outputs/graphs/, regenerated deterministically from the same processed data "
        "and analytical tables that drive the live dashboard. The dashboard "
        "(executive-retention-command-center.html) carries the interactive versions "
        "of the trend, segment, driver, and queue views, with period, segment, and "
        "channel filters. This report and that dashboard are two renderings of one "
        "governed pipeline; neither contains a number the other cannot reproduce.",
        styles))

    H2("F. Per-account feature dictionary", "s12f", styles, story)
    story.append(P(
        "The behavioural signals examined in Sections 07 and 08 and scored in the "
        "risk index are measured at the account's churn date or, for active accounts, "
        "at the snapshot. The principal fields are defined below.", styles))
    story.append(data_table(
        ["Field", "Definition"],
        [
            ["tenure_days", "Days from acquisition to churn or snapshot."],
            ["current_mrr", "Account's monthly recurring revenue at observation."],
            ["usage_trend", "Change in 30-day session count; negative is decline."],
            ["recent_sessions_30d / 90d", "Product sessions in the trailing window."],
            ["feature_adoption_score_recent",
             "Breadth of feature use, 0 to 100."],
            ["support_tickets_30d / 90d", "Support tickets filed in the window."],
            ["nps_score_recent", "Most recent net promoter response."],
            ["failed_payments_90d", "Count of failed billing attempts in 90 days."],
            ["payment_failure_flag", "Any failed payment in the window."],
            ["renewal_near_flag", "Renewal falls within the near-term horizon."],
            ["at_risk_flag", "Account carries at least one distress signal."],
            ["churn_flag", "Account has closed (the analysis target)."],
        ],
        styles,
        [5.4 * cm, CONTENT_W - 5.4 * cm],
    ))

    return story


# ════════════════════════════════════════════════════════════
# Build
# ════════════════════════════════════════════════════════════
def build() -> Path:
    styles = build_styles()
    M = load_metrics()
    story = build_story(styles, M)

    doc = ReportDoc(
        str(REPORT_PATH), pagesize=A4,
        leftMargin=LMARGIN, rightMargin=RMARGIN,
        topMargin=TMARGIN, bottomMargin=BMARGIN,
        title="Churn & Retention Intelligence: Retention Review",
        author="Retention Analytics", subject="Churn and retention analysis",
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
