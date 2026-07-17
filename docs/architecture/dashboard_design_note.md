# Dashboard Design Note

Why the executive dashboard is shaped the way it is. This is a design-rationale
record, not a usage guide — for what the dashboard shows, open it; for how it is
built, read `src/churn/dashboard.py`.

## Goal

A static artifact for reviewing account priority, supporting signals, and the
recommended intervention. It must be reproducible from the same governed tables
used by the memo and report.

## Shape

The page is an argument, not a wall of cards: an executive brief, then five
numbered sections that answer one question each — trajectory (is it getting
worse?), structure (where does it concentrate?), the decision surface (what does
the scoring policy actually say?), capacity (how far do our contacts go?), and
the queue (who do we call, and with what play?).

## Decisions

### Self-contained, single HTML file
The dashboard embeds its data (as inline JSON) and its chart runtime (vendored
`assets/vendor/chart.umd.min.js`) into one HTML file. No server, no CDN, no
network at view time.

- **Why:** the artifact works offline and does not depend on a live application
  or CDN. `config/vendor_assets.json` pins the chart runtime version and hash.
- **Cost:** the HTML payload is larger and the vendored runtime must be updated
  explicitly.

### Precomputed facts and risk cube
Python emits three bounded structures: a compact monthly dimensional fact table,
snapshot aggregates, and a risk KPI cube containing `__all__` rollups across
segment, region, channel, plan, and risk tier. The browser filters and aggregates
the monthly facts, looks up risk KPIs, and filters the snapshot rows.

- **Why:** customer-level feature construction stays in Python, while the small
  in-browser operations needed for filters remain inspectable and deterministic.
- **Cost:** combinatorial payload growth. The complete open-account score table
  is embedded so rankings remain exact after filtering; the browser renders at
  most 300 matching rows at once to keep interaction responsive.

### The scoring policy is drawn, not just applied
`retention_priority = churn_risk * (0.65 + 0.35 * customer_value / 100)` is a
surface over two axes, so the dashboard plots it as one: every open account as a
point on churn risk × customer value, with each tier threshold drawn as the curve
`churn_risk = threshold / (0.65 + 0.0035 * value)`.

- **Why:** the priority score is the product's central judgement, and a number in
  a table cannot be argued with. Drawn, the policy can be: the curves bend, which
  shows that value multiplies risk rather than adding to it, and the empty
  top-right corner shows that the `critical` override never fires on this book.
- **Single source:** the cut-offs, blend weights, and override thresholds are
  exported from `risk.py` into the payload as `policy`, so the drawn boundaries
  cannot drift from the constants the scores were computed with.
- **Cost:** the payload carries the policy, and the template owns the inverse of
  the priority formula (`risk = threshold / (base + weight * value/100)`) in
  order to solve the curve for a given threshold.

### Work order is a decision, not a default
Ranking by `retention_priority_score` is risk-led by construction: customer value
only modulates the index by a third, while MRR spans two orders of magnitude, so
priority correlates with revenue at ~0.18. Worked top-down, the governed order
reaches materially less of the exposed MRR per contact than a value-weighted one.

- **Why:** hiding that would make the dashboard flatter than the data. The
  capacity section plots the cumulative MRR reached per account contacted under
  three orderings, and the selected ordering re-ranks the queue below it.
- **Boundary:** the ordering changes the *view*, never the score. The governed
  priority index remains the default and every published table is unaffected.
  `risk × revenue` is presented as a ranking index only — the risk score is not
  a calibrated probability, so no expected-value claim is made.

### Every figure has a table twin
Each figure carries a chart/table toggle that renders the same slice as a plain
table.

- **Why:** the charts encode magnitude with colour and position; the table is the
  WCAG-clean equivalent and doubles as the read-out an analyst copies into a memo.

### Colour means exactly one thing
Red encodes retention risk and nothing else; every other mark is neutral ink. The
tier ramp (`medium → critical`) and the cohort ramp are single-hue ordinal ramps
validated for monotone lightness, adjacent-step separation, and contrast against
both the light and dark surfaces. The `low` tier — 72% of accounts — wears the
neutral mark, because it is context rather than the point.

### Content-hash versioning
`_build_version` hashes the canonical rendered-data payload, template, and
vendored runtime into a short digest.

- **Why:** the digest changes when the delivered dashboard changes, while
  ignoring byte-level differences in intermediate CSV serialization. CI
  rebuilds the release and uses `git diff --exit-code` to detect artifact drift.

### Single official artifact
`_enforce_single_official_html` deletes any HTML in the dashboard directory that
isn't the canonical filename before writing. `index.html` and `docs/index.html`
are thin redirects to it.

- **Why:** stale or renamed copies create ambiguity. The build leaves one
  canonical dashboard payload.

### Validation embedded, built last
The dashboard reads validation tables as *optional* inputs and is rebuilt after
the validation gate runs (`make all` renders once, then re-renders), so the
published dashboard carries the current readiness summary rather than a stale
one. Missing validation tables degrade gracefully to empty frames rather than
crashing the first render.

### Defensive inline-script escaping
`build_html` escapes `</script`, `&`, `<`, `>`, and the U+2028/U+2029 line
separators before injecting JSON and JS into `<script>` tags.

- **Why:** the data is embedded inside HTML script context. Without escaping, a
  stray `</script>` sequence or a Unicode line separator in the data could break
  out of the script element and corrupt the page. This is the same class of
  concern as XSS even though the data is internally generated — defence in depth.

## What this design deliberately is not

- Not a live BI tool. It is a point-in-time, reproducible snapshot keyed to a
  fixed reference date. Re-run the pipeline to move the snapshot.
- Not interactive analytics. Filters slice precomputed facts; there is no
  ad-hoc query path by design.
- Not a probability model surface. The risk score is a transparent
  prioritisation index (see `risk_scoring_methodology.md`), and the dashboard
  presents it as such.
