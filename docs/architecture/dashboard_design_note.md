# Dashboard Design Note

Why the executive dashboard is shaped the way it is. This is a design-rationale
record, not a usage guide — for what the dashboard shows, open it; for how it is
built, read `src/churn/dashboard.py`.

## Goal

A static artifact for reviewing account priority, supporting signals, and the
recommended intervention. It must be reproducible from the same governed tables
used by the memo and report.

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

### Content-hash versioning
`_build_version` hashes the governed dashboard inputs, Python module sources,
template, configuration, and vendored runtime into a short digest.

- **Why:** the digest changes when an input changes. CI separately rebuilds the
  release and uses `git diff --exit-code` to detect artifact drift.

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
