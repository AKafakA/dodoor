"""
Side-by-side comparison report for the Euro-Par 2026 artifact.

Builds a single HTML page that pairs every reference plot in `--reference`
with the corresponding plot from the AE reproduction run in `--candidate`,
and prepends a numeric summary table (median / p95 / p99 end-to-end
scheduling latency, per scheduler per QPS) parsed from the raw scheduler
metrics logs.

The numeric summary is more robust than pixel comparison: cluster runs
cannot be reproduced bit-exactly, but the *ordering* of schedulers under
each QPS should match the reference (Dodoor at or near the bottom of the
tail-latency curve). The reviewer can read this directly off the table.
"""

import argparse
import html
import json
import os
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

# Make the deploy.python.* package importable regardless of where the
# script is launched from.
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from deploy.python.analysis.scheduler_metrics import SchedulerMetrics  # noqa: E402


EXPERIMENT_DIR_RE = re.compile(
    r"^(?P<scheduler>\w+)_batch_(?P<batch>[\d.]+)_beta_(?P<beta>[\d.]+)_"
    r"cpu_(?P<cpu>[\d.]+)_duration_(?P<dur>[\d.]+)_qps_(?P<qps>\d+)$"
)


@dataclass
class ExpStats:
    scheduler: str
    qps: int
    p50: float | None
    p95: float | None
    p99: float | None
    finished: int


def collect_pngs(root: Path) -> list[Path]:
    pngs = []
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            if fn.lower().endswith(".png"):
                pngs.append(Path(dirpath, fn))
    return sorted(pngs)


def last_or_none(xs: list) -> float | None:
    return xs[-1] if xs else None


def parse_one_experiment(exp_dir: Path) -> ExpStats | None:
    """Parse the single metrics log under <exp_dir>/metrics/ and return the
    last-sample latency percentiles, or None if no log is present."""
    m = EXPERIMENT_DIR_RE.match(exp_dir.name)
    if not m:
        return None
    metrics_dir = exp_dir / "metrics"
    if not metrics_dir.is_dir():
        return None
    candidates = [p for p in metrics_dir.iterdir() if p.suffix == ".log"]
    if not candidates:
        return None
    parser = SchedulerMetrics(str(candidates[0]))
    return ExpStats(
        scheduler=m.group("scheduler"),
        qps=int(m.group("qps")),
        p50=last_or_none(parser.metrics["e2e_latency_p50"]),
        p95=last_or_none(parser.metrics["e2e_latency_p95"]),
        p99=last_or_none(parser.metrics["e2e_latency_p99"]),
        finished=last_or_none(parser.metrics["finished_tasks"]) or 0,
    )


def parse_campaign(campaign_dir: Path) -> dict[tuple[str, int], ExpStats]:
    """Map (scheduler, qps) → stats for every experiment under one campaign
    directory (e.g. `.../scheduler/azure_600/`)."""
    out: dict[tuple[str, int], ExpStats] = {}
    if not campaign_dir.is_dir():
        return out
    for sub in sorted(campaign_dir.iterdir()):
        if not sub.is_dir():
            continue
        stats = parse_one_experiment(sub)
        if stats:
            out[(stats.scheduler, stats.qps)] = stats
    return out


def parse_log_root(log_root: Path) -> dict[str, dict[tuple[str, int], ExpStats]]:
    """Map campaign-name → (scheduler, qps) → stats."""
    out: dict[str, dict[tuple[str, int], ExpStats]] = {}
    if not log_root.is_dir():
        return out
    for campaign in sorted(p.name for p in log_root.iterdir() if p.is_dir()):
        out[campaign] = parse_campaign(log_root / campaign)
    return out


def stats_to_json(stats_by_campaign: dict[str, dict[tuple[str, int], ExpStats]]) -> dict:
    return {
        campaign: [
            {"scheduler": s.scheduler, "qps": s.qps,
             "p50": s.p50, "p95": s.p95, "p99": s.p99,
             "finished": s.finished}
            for s in by_key.values()
        ]
        for campaign, by_key in stats_by_campaign.items()
    }


def stats_from_json(blob: dict) -> dict[str, dict[tuple[str, int], ExpStats]]:
    out: dict[str, dict[tuple[str, int], ExpStats]] = {}
    for campaign, rows in blob.items():
        out[campaign] = {
            (r["scheduler"], int(r["qps"])): ExpStats(
                scheduler=r["scheduler"], qps=int(r["qps"]),
                p50=r.get("p50"), p95=r.get("p95"), p99=r.get("p99"),
                finished=int(r.get("finished") or 0),
            )
            for r in rows
        }
    return out


def render_summary_table(
    campaign: str,
    reference: dict[tuple[str, int], ExpStats],
    candidate: dict[tuple[str, int], ExpStats],
) -> str:
    """Build one HTML table for one campaign comparing reference vs.
    candidate latencies."""
    keys = sorted(set(reference) | set(candidate), key=lambda k: (k[1], k[0]))
    if not keys:
        return ""

    rows = []
    for sched, qps in keys:
        ref = reference.get((sched, qps))
        cand = candidate.get((sched, qps))

        def cell(s: ExpStats | None, attr: str) -> str:
            if s is None:
                return "&mdash;"
            v = getattr(s, attr)
            return f"{v:.2f}" if v is not None else "&mdash;"

        rows.append(
            f"<tr>"
            f"<td>{html.escape(sched)}</td><td>{qps}</td>"
            f"<td>{cell(ref, 'p50')}</td><td>{cell(cand, 'p50')}</td>"
            f"<td>{cell(ref, 'p95')}</td><td>{cell(cand, 'p95')}</td>"
            f"<td>{cell(ref, 'p99')}</td><td>{cell(cand, 'p99')}</td>"
            f"<td>{ref.finished if ref else '&mdash;'}</td>"
            f"<td>{cand.finished if cand else '&mdash;'}</td>"
            f"</tr>"
        )

    return f"""
<h2>Latency summary &mdash; <code>{html.escape(campaign)}</code></h2>
<table>
  <thead>
    <tr>
      <th rowspan="2">scheduler</th><th rowspan="2">QPS</th>
      <th colspan="2">p50 (ms)</th>
      <th colspan="2">p95 (ms)</th>
      <th colspan="2">p99 (ms)</th>
      <th colspan="2">finished tasks</th>
    </tr>
    <tr>
      <th>ref</th><th>cand</th>
      <th>ref</th><th>cand</th>
      <th>ref</th><th>cand</th>
      <th>ref</th><th>cand</th>
    </tr>
  </thead>
  <tbody>
{chr(10).join(rows)}
  </tbody>
</table>
"""


def render(reference: Path, candidate: Path, output: Path,
           ref_stats: dict[str, dict[tuple[str, int], ExpStats]] | None,
           cand_stats: dict[str, dict[tuple[str, int], ExpStats]] | None) -> None:
    # ---- Numeric summary -------------------------------------------------
    summary_html = ""
    if ref_stats or cand_stats:
        campaigns = sorted(set(ref_stats or {}) | set(cand_stats or {}))
        for campaign in campaigns:
            ref = (ref_stats or {}).get(campaign, {})
            cand = (cand_stats or {}).get(campaign, {})
            summary_html += render_summary_table(campaign, ref, cand)

    # ---- Side-by-side images --------------------------------------------
    rows = []
    for ref_png in collect_pngs(reference):
        rel = ref_png.relative_to(reference)
        cand_png = candidate / rel
        rel_ref = os.path.relpath(ref_png, output.parent)
        rel_cand = (
            os.path.relpath(cand_png, output.parent)
            if cand_png.exists()
            else None
        )
        rows.append((rel, rel_ref, rel_cand, cand_png.exists()))

    missing = sum(1 for _, _, _, exists in rows if not exists)
    total = len(rows)
    reproduced = total - missing

    head = f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<title>Dodoor artifact: reference vs reproduction</title>
<style>
  body {{ font-family: system-ui, sans-serif; max-width: 1500px; margin: 2em auto; padding: 0 1em; color: #222; }}
  h1 {{ font-size: 1.4em; }}
  h2 {{ font-size: 1.1em; margin-top: 2em; }}
  .summary {{ background: #f5f5f5; padding: 1em; border-radius: 6px; margin-bottom: 2em; }}
  table {{ border-collapse: collapse; font-size: 0.9em; margin-bottom: 1em; }}
  th, td {{ border: 1px solid #ccc; padding: 4px 8px; text-align: right; font-variant-numeric: tabular-nums; }}
  th:first-child, td:first-child {{ text-align: left; }}
  thead tr:first-child th {{ background: #eee; }}
  .row {{ display: flex; gap: 1em; margin-bottom: 2em; align-items: flex-start; flex-wrap: wrap; }}
  .row > div {{ flex: 1 1 600px; min-width: 0; }}
  .row img {{ max-width: 100%; border: 1px solid #ddd; }}
  .caption {{ font-family: ui-monospace, monospace; font-size: 0.85em; color: #555; margin-bottom: 0.4em; }}
  .missing {{ background: #fff3cd; border: 1px dashed #d4a017; padding: 1em; border-radius: 4px; color: #7a5800; }}
  hr {{ border: none; border-top: 1px solid #eee; margin: 2em 0; }}
</style>
</head><body>
<h1>Dodoor artifact: reference vs reproduction</h1>
<div class="summary">
<p><strong>Reference plots:</strong> <code>{html.escape(str(reference))}</code></p>
<p><strong>Reproduction plots:</strong> <code>{html.escape(str(candidate))}</code></p>
<p>{reproduced} of {total} reference figures have a candidate counterpart.
Cluster results cannot be reproduced bit-exactly; the reviewer's task is to
confirm that, for each pair below, scheduler <em>ordering</em>, CDF shapes,
and the trends across the swept parameter match the reference. The numeric
summary that follows reports the last-sample percentiles per scheduler and
QPS so ordering can be verified directly.</p>
</div>
{summary_html}
<hr>
<h2>Side-by-side figures</h2>
"""

    body_parts = [head]
    for rel, rel_ref, rel_cand, exists in rows:
        body_parts.append('<hr>')
        body_parts.append(f'<h3>{html.escape(str(rel))}</h3>')
        body_parts.append('<div class="row">')
        body_parts.append(
            f'<div><div class="caption">reference</div>'
            f'<img src="{html.escape(rel_ref)}" alt="reference {html.escape(str(rel))}"></div>'
        )
        if exists:
            body_parts.append(
                f'<div><div class="caption">reproduction</div>'
                f'<img src="{html.escape(rel_cand)}" alt="candidate {html.escape(str(rel))}"></div>'
            )
        else:
            body_parts.append(
                '<div><div class="caption">reproduction</div>'
                '<div class="missing">No candidate plot at the expected path. '
                'The corresponding experiment phase may not have been run, or '
                'plotting failed.</div></div>'
            )
        body_parts.append('</div>')

    body_parts.append('</body></html>\n')
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("".join(body_parts))
    print(f"Wrote {output} ({reproduced}/{total} pairs).")


def emit_reference_summary(log_root: Path, output: Path) -> None:
    """Pre-compute the reference numeric summary so the artifact ZIP can
    ship a small JSON instead of the multi-GB raw log tree."""
    stats = parse_log_root(log_root)
    blob = stats_to_json(stats)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(blob, indent=2))
    n = sum(len(v) for v in blob.values())
    print(f"Wrote {output} ({n} experiment rows across {len(blob)} campaigns).")


def load_stats(log_root: Path | None, summary_json: Path | None) -> (
    dict[str, dict[tuple[str, int], ExpStats]] | None
):
    """Prefer the live log tree; fall back to the pre-baked summary JSON."""
    if log_root and log_root.is_dir():
        return parse_log_root(log_root)
    if summary_json and summary_json.is_file():
        return stats_from_json(json.loads(summary_json.read_text()))
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reference", type=Path,
                    help="Directory of reference plots (e.g. deploy/plots).")
    ap.add_argument("--candidate", type=Path,
                    help="Directory of plots produced by this run (e.g. deploy/plots_ae).")
    ap.add_argument("--output", type=Path,
                    help="HTML report path.")
    ap.add_argument("--ref-log-root", type=Path,
                    default=Path("deploy/resources/log/scheduler"),
                    help="Reference scheduler log root for numeric summary.")
    ap.add_argument("--cand-log-root", type=Path,
                    default=None,
                    help="Candidate scheduler log root. Defaults to "
                         "$DODOOR_LOG_BASE_DIR/scheduler if set, else "
                         "deploy/resources/log_ae/scheduler.")
    ap.add_argument("--ref-summary-json", type=Path,
                    default=Path("deploy/resources/reference_summary.json"),
                    help="Pre-baked reference summary, used when the raw "
                         "reference log tree is not present (e.g. inside "
                         "the AE submission ZIP, where the multi-GB log "
                         "tree is replaced by this small JSON).")
    ap.add_argument("--emit-reference-summary", type=Path,
                    help="Instead of rendering an HTML report, parse "
                         "--ref-log-root and write the summary JSON here. "
                         "Used by build_artifact.sh.")
    args = ap.parse_args()

    if args.emit_reference_summary:
        emit_reference_summary(args.ref_log_root, args.emit_reference_summary)
        return

    if not (args.reference and args.candidate and args.output):
        ap.error("--reference, --candidate, and --output are required for "
                 "rendering (omit them only with --emit-reference-summary).")

    cand_log_root = args.cand_log_root
    if cand_log_root is None:
        base = os.environ.get("DODOOR_LOG_BASE_DIR", "deploy/resources/log_ae")
        cand_log_root = Path(base) / "scheduler"

    ref_stats = load_stats(args.ref_log_root, args.ref_summary_json)
    cand_stats = load_stats(cand_log_root, None)

    render(args.reference, args.candidate, args.output, ref_stats, cand_stats)


if __name__ == "__main__":
    main()
