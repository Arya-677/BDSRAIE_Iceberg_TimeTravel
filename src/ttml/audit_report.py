"""Audit-readiness report generator.

Reads ONLY evidence files produced by real runs:
  reports/demo_results.json         (ttml demo)
  reports/storage/storage_results.csv (ttml experiments)
  reports/junit.xml                 (pytest --junitxml=reports/junit.xml)
  reports/data_card.md              (written by the team)
and writes reports/audit_readiness_report.md and .pdf. Anything it cannot find is shown as 'NOT RUN',
never invented. Sections that need human judgement are marked TEAM TO COMPLETE.
"""
from __future__ import annotations

import html
import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd

CONTROLS = [
    ("T1", "Every model version is bound to an Iceberg snapshot ID", "registry_binds"),
    ("T2", "Exact retrain from the registry after the table has changed", "exact_retrain"),
    ("T3", "Time travel returns the data as it was", "time_travel"),
    ("T4", "Rollback restores the known-good state; tainted models quarantined", "rollback"),
    ("T5", "Retention preserves audit evidence and refuses unsafe cleanup", "retention"),
    ("T6", "Negative control: the test can fail (unpinned retrain differs)", "negative_control"),
]


def load_junit(path: str | Path) -> dict[str, str]:
    p = Path(path)
    if not p.exists():
        return {}
    out = {}
    root = ET.parse(p).getroot()
    for prop in root.iter("property"):
        if prop.get("name") == "ttml_iceberg_mode":
            out["__mode__"] = prop.get("value")          # real | sim | plain
    for tc in root.iter("testcase"):
        status = "passed"
        if tc.find("failure") is not None or tc.find("error") is not None:
            status = "FAILED"
        elif tc.find("skipped") is not None:
            status = "skipped"
        out[f"{tc.get('classname', '')}.{tc.get('name', '')}"] = status
    return out


def _verdict(b) -> str:
    return "PASS" if b is True else ("FAIL" if b is False else "NOT RUN")


def _control_status(junit: dict, key: str) -> str:
    hits = [s for n, s in junit.items() if key in n and not n.startswith("__")]
    if not hits:
        return "NOT RUN"
    if "FAILED" in hits:
        return "FAIL"
    if all(s == "skipped" for s in hits):
        return "SKIPPED (needs Iceberg)"
    mode = junit.get("__mode__")
    if mode == "sim":
        return "PASSED ON SIMULATOR ONLY (not evidence)"
    if mode in (None, "plain"):
        return "PASS (engine not recorded: rerun with TTML_ICEBERG=1)"
    return "PASS"


def _md_table(rows: list[list], header: list[str]) -> str:
    out = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    for r in rows:
        out.append("| " + " | ".join(str(x).replace("|", "/") for x in r) + " |")
    return "\n".join(out)


def _fmt_bytes(b) -> str:
    if b is None or (isinstance(b, float) and pd.isna(b)):
        return "n/a"
    b = float(b)
    for u in ("B", "KiB", "MiB", "GiB"):
        if b < 1024 or u == "GiB":
            return f"{b:.1f} {u}" if u != "B" else f"{int(b)} B"
        b /= 1024


def _cell(col: str, v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    if col.endswith("_bytes"):
        return _fmt_bytes(v)
    if col == "overhead_ratio":
        return f"{v:.3f}"
    if isinstance(v, float) and v.is_integer() and col != "update_rate":
        return int(v)
    return v


def _snapshot_storage_section(A, snap: pd.DataFrame | None, results: dict | None, reports_dir: str) -> None:
    from .snapshot_storage import fmt_bytes, summarize
    A("### 6.1 Storage difference between snapshots\n")
    A("For every snapshot of the loans table: its size as of that snapshot, what that commit added and removed, how much of it is shared "
      "with the previous snapshot, the storage actually kept, and what full copies would cost. Figures come from Iceberg's own per-commit "
      "snapshot summaries (no data scan). `kept` counts every snapshot still present; expired snapshots are no longer counted.\n")
    if snap is None or snap.empty:
        A("NOT RUN: `reports/storage/snapshot_storage.csv` not found. Run `ttml demo` or `ttml snapshot-storage`.\n")
        return
    s = summarize(snap)
    A(_md_table([
        ["Snapshots", s["snapshots"]],
        ["Table size at first / last snapshot", f"{fmt_bytes(s['first_table_bytes'])} / {fmt_bytes(s['last_table_bytes'])}"],
        ["Storage kept for all snapshots", fmt_bytes(s["cumulative_retained_bytes"])],
        ["History overhead (kept / current table)", f"{s['history_overhead_ratio']:.2f}x" if s["history_overhead_ratio"] else "n/a"],
        ["If every snapshot were a full copy", f"{fmt_bytes(s['full_copy_cumulative_bytes'])} (sharing saves {s['savings_factor']:.1f}x)" if s["savings_factor"] else "n/a"],
        ["Average share reused from previous snapshot", f"{s['avg_shared_pct_after_first']:.1f}%"],
        ["Largest single commit", f"{fmt_bytes(s['largest_commit_bytes'])} (snapshot #{s['largest_commit_snapshot_no']})"],
        ["Bytes removed from the table over time", fmt_bytes(s["total_removed_bytes"])],
        ["Commit types", ", ".join(f"{k}: {v}" for k, v in s["operations"].items())],
    ], ["Metric", "Value"]))
    rec = ((results or {}).get("snapshot_storage") or {}).get("reconciliation") or {}
    if "difference_bytes" in rec:
        A(f"\nCross-check: the per-commit summaries add up to {fmt_bytes(rec['cumulative_from_summaries'])}; Iceberg's file listing shows "
          f"{fmt_bytes(rec['retained_from_all_files'])} retained (difference {fmt_bytes(abs(rec['difference_bytes']))}).\n")
    show = snap if len(snap) <= 24 else snap.nlargest(24, "added_bytes").sort_values("snapshot_no")
    A("\n" + _md_table([[int(r["snapshot_no"]), r["operation"], fmt_bytes(r["table_bytes"]), fmt_bytes(r["added_bytes"]), fmt_bytes(r["removed_bytes"]),
                          f"{r['shared_pct']:.0f}%", fmt_bytes(r["cumulative_retained_bytes"])] for _, r in show.iterrows()],
                        ["#", "commit", "table size", "added", "removed", "shared with previous", "kept so far"]))
    if len(snap) > 24:
        A(f"\n(Showing the 24 largest commits of {len(snap)}; the full table is `reports/storage/snapshot_storage.csv`.)")
    A("")
    for img in ("snapshot_storage_growth.png", "snapshot_storage_changes.png", "snapshot_storage_compare.png"):
        if Path(reports_dir, "storage", img).exists():
            A(f"![{img}](storage/{img})\n")


def build_markdown(results: dict | None, storage: pd.DataFrame | None, junit: dict, data_card: str | None,
                   reports_dir: str = "reports", snap_storage: pd.DataFrame | None = None) -> str:
    L: list[str] = []
    A = L.append
    A("# Audit-Readiness Report: Time Travel for Reproducible ML\n")
    A("*Generated from run evidence. Items marked TEAM TO COMPLETE need human judgement. Nothing here is invented: "
      "if an evidence file was missing, the section says NOT RUN.*\n")

    if junit.get("__mode__") == "sim" or (results or {}).get("engine") == "SIMULATOR":
        A("> **WARNING: some or all results below were produced by the Iceberg SIMULATOR used for logic testing. "
          "They are NOT audit evidence. Rerun `scripts/run_demo.sh` against the real Iceberg runtime and regenerate this report.**\n")
    ds = (results or {}).get("dataset") or {}
    if results and ds.get("kind") == "synthetic":
        A("> **NOTE: the demo evidence in sections 4, 5 and 7 was generated on SYNTHETIC data. It shows the machinery works; "
          "rerun on your real dataset (`bash run.sh`, option 5) before submitting.**\n")
    A("## 1. Scope and control summary\n")
    A("A credit-default model is trained on loan data held in an Apache Iceberg table. The question an auditor asks is: "
      "*which exact data was this model trained on, and can you prove you can rebuild it?* The controls below answer it.\n")
    A(_md_table([[c, d, _control_status(junit, k)] for c, d, k in CONTROLS], ["Control", "What it proves", "Test result"]))
    A("\nTest results come from `pytest --junitxml=reports/junit.xml`. SKIPPED means the Iceberg tests were not run in that environment; "
      "the report is not audit-ready until they pass.\n")

    A("## 2. Data and data card\n")
    A(data_card.strip() if data_card else "NOT PROVIDED: write `reports/data_card.md` (source, licence, version, rows used, excluded columns, known limitations).")
    A("")

    A("## 3. Lineage design\n")
    A("- Source CSV is split into calendar months. Each month is one append commit, hence one Iceberg snapshot.\n"
      "- Training pins the current snapshot ID once, reads strictly `VERSION AS OF` that ID, sorts by `loan_id`, and fingerprints the exact frame.\n"
      "- A model cannot be registered without `snapshot_id`, data fingerprint, feature-list hash, parameters and git commit. The registry is append-only.\n"
      "- Each model's snapshot is tagged (`model-<version>`) so snapshot expiry cannot delete it. Approval extends the tag to the release retention period.\n"
      "- The fingerprint is an order-independent content hash. It detects change; it is not a cryptographic proof (see limitations).\n")

    A("## 4. Reproducibility results\n")
    if not results:
        A("NOT RUN: `reports/demo_results.json` not found. Run `python -m ttml.cli demo`.\n")
    else:
        e = results.get("exact_retrain", {})
        if ds:
            A(f"Dataset used by this run: **{ds.get('kind')}**, file `{Path(str(ds.get('path'))).name}`, SHA-256 `{str(ds.get('sha256'))[:16]}...`, "
              f"{ds.get('rows_in_demo_months', 'n/a')} rows across the 14 demo months {results.get('months', ['?'])[0]} to {results.get('months', ['?'])[-1]}.\n")
        A("Model **v1** was trained on snapshot "
          f"`{results.get('v1', {}).get('snapshot_id')}`. The table then moved on (later head `{results.get('head_after_more_months')}`). "
          "v1 was rebuilt using only its registry row.\n")
        A(_md_table([
            ["L1", "same snapshot and identical data fingerprint", _verdict(e.get("L1_same_data"))],
            ["L2", f"bit-identical predictions on {e.get('n_probe')} probe rows (max abs diff {e.get('max_abs_pred_diff')})", _verdict(e.get("L2_identical_predictions"))],
            ["L3", "identical model file hash", _verdict(e.get("L3_identical_model_file"))],
        ], ["Level", "Check", "Result"]))
        A(f"\nFingerprints: original `{e.get('fingerprint_a')}`, rebuilt `{e.get('fingerprint_b')}`.\n")
        e2 = results.get("exact_retrain_after_retention", {})
        A(f"After the retention job ran, v1 was rebuilt again: L1 {_verdict(e2.get('L1_same_data'))}, "
          f"L2 {_verdict(e2.get('L2_identical_predictions'))}, L3 {_verdict(e2.get('L3_identical_model_file'))}.\n")
        nc = results.get("negative_control", {})
        A(f"**Negative control.** Reading today's head instead of v1's snapshot gives a different fingerprint: {_verdict(nc.get('differs'))} "
          "(this shows the comparison is capable of failing).\n")
        A("**Limitation.** Bit-identical results are only claimed for the same environment (library versions, CPU architecture, single thread). "
          "Floating-point results can differ across architectures. The environment is recorded in the registry (`lib_versions`, `env_lock_sha256`).\n")

    A("## 5. Rollback results\n")
    if not results or "rollback" not in results:
        A("NOT RUN.\n")
    else:
        rb = results["rollback"]
        fl = results.get("audit_flagged", [])
        pb = results.get("poisoned_batch")
        A(f"A deliberately bad month ({pb}: `int_rate` scaled x100, half the labels flipped) was loaded with the ingest gate bypassed to simulate a failure that slipped through. "
          "Model **v3** was then trained on the tainted table.\n")
        A(_md_table([
            ["Post-hoc audit flagged the bad batch", _verdict(any(f.get("batch") == pb for f in fl)),
             next((f.get("why", "") for f in fl if f.get("batch") == pb), "")],
            ["Other batches flagged by the heuristic (possible false positives)", "info",
             ", ".join(results.get("audit_other_flags") if results.get("audit_other_flags") is not None
                       else [f.get("batch") for f in fl if f.get("batch") != pb]) or "none"],
            ["Models quarantined", _verdict(bool(rb.get("models_quarantined"))), ", ".join(rb.get("models_quarantined", []))],
            ["Rolled back to last good snapshot", "PASS", f"{rb.get('from')} -> {rb.get('rolled_back_to')}"],
            ["Head fingerprint equals fingerprint logged when that snapshot was good", _verdict(rb.get("fingerprint_matches_logged_good_state")), ""],
            ["Head equals pre-incident fingerprint", _verdict(rb.get("head_equals_pre_incident_fingerprint")), ""],
            ["Bad snapshot still inspectable after rollback", _verdict(rb.get("bad_snapshot_still_listed")), "history is not erased"],
            ["v3 status", results.get("status_v3"), "append-only status events"],
        ], ["Check", "Result", "Detail"]))
        A("\nCorrected months were then re-ingested and a clean model **v4** trained. "
          "Rollback changes what is *current*; it does not erase the bad snapshot, and models trained on it must be quarantined explicitly.\n")

    A("## 6. Storage overhead\n")
    A("Definition: `overhead_ratio = retained_bytes / live_bytes` (data + delete files, counted once each). Metadata is reported separately. "
      "Corrections are synthetic (a seeded fraction of the previous month's labels flipped each month).\n")
    if storage is None or storage.empty:
        A("NOT RUN: `reports/storage/storage_results.csv` not found. Run `python -m ttml.cli experiments`.\n")
    else:
        cols = [c for c in ["exp", "mode", "update_rate", "stage", "retain_last", "snapshots", "live_bytes", "retained_bytes",
                            "metadata_bytes", "overhead_ratio"] if c in storage.columns]
        rows = []
        for _, r in storage.iterrows():
            rows.append([_cell(c, r[c]) for c in cols])
        A(_md_table(rows, cols))
        A("")
        for img, cap in (("e1_append_only.png", "E1"), ("e2_e3_update_rate.png", "E2/E3"), ("e4_compaction.png", "E4"), ("e5_expiry_sweep.png", "E5")):
            if Path(reports_dir, "storage", img).exists():
                A(f"![{cap}](storage/{img})\n")          # relative to the report file, so Markdown viewers resolve it
        try:
            e1 = storage[storage.exp == "E1"]["overhead_ratio"]
            A(f"Measured: E1 ratio ranged {e1.min():.3f} to {e1.max():.3f} across {int(storage[storage.exp == 'E1'].snapshots.min())} to "
              f"{int(storage[storage.exp == 'E1'].snapshots.max())} snapshots.")
            hi = storage[(storage.exp.isin(["E2", "E3"])) & (storage.update_rate == storage.update_rate.max())]
            A("At the highest update rate: " + "; ".join(f"{r['mode']} {r['overhead_ratio']:.3f}" for _, r in hi.iterrows()) + ".\n")
        except Exception:
            pass
        A("**Interpretation: TEAM TO COMPLETE.** State what the numbers show for YOUR data size, compare with the hypotheses in the plan "
          "(append-only near 1.0; copy-on-write grows faster than merge-on-read; compaction can raise retained bytes until old snapshots are expired), "
          "and say where they were not confirmed. Absolute values depend on file sizes and partitioning.\n")

    _snapshot_storage_section(A, snap_storage, results, reports_dir)
    A("## 7. Retention policy\n")
    A(_md_table([
        ["1 Release evidence", "approved/deployed models (and quarantined, for forensics)", "release_retention_days (placeholder 2555 = ~7 years; CONFIRM with compliance)", "tag with RETAIN"],
        ["2 Experiment", "candidate models", "experiment_retention_days (90)", "tag with RETAIN"],
        ["3 Routine ingestion", "untagged monthly snapshots", "30 days, always keep latest 10", "expire_snapshots"],
    ], ["Tier", "Which snapshots", "Retention", "Mechanism"]))
    A("\nSafety guard: before any expiry, every non-retired registered model must have an existing, tagged snapshot; otherwise the job aborts and names the model.\n")
    if results and "retention" in results:
        r = results["retention"]
        A(f"Demo run: snapshots {r.get('snapshots_before')} -> {r.get('snapshots_after')}; "
          f"v1 snapshot survived: {_verdict(r.get('v1_snapshot_survived'))}; "
          f"an untagged old snapshot was expired: {_verdict(r.get('untagged_old_snapshot_expired'))}; "
          f"guard problems after run: {r.get('guard_problems_now') or 'none'}.\n")
    A("Open issues: right to erasure vs immutable history (TEAM TO COMPLETE with your legal position); the 7-year figure is a placeholder.\n")

    A("## 8. Limitations and residual risk\n")
    for t in [
        "Iceberg snapshot IDs and the content fingerprint detect change but are not tamper-proof. Someone with storage write access could alter files. "
        "Mitigations: object-store versioning / object lock, signed exports of the registry.",
        "Exact reproducibility is claimed for an identical software and hardware environment only.",
        "Local Hadoop catalog is single-writer; production should use a shared catalog (JDBC/REST).",
        "Update workloads in the storage experiments are synthetic.",
        "This is a teaching artefact on public data, not a validated lending model. Excluding sensitive/proxy attributes does not guarantee fairness.",
    ]:
        A(f"- {t}")
    A("")
    A("## 9. Auditor question to evidence\n")
    A(_md_table([
        ["Which data trained model X?", "registry.snapshot_id + data_filter + feature list", "model_registry; `VERSION AS OF` read"],
        ["Is that data intact?", "re-read snapshot, compare fingerprint", "T2/T3; registry.data_fingerprint"],
        ["Can you rebuild the same model?", "retrain from registry row only", "Section 4"],
        ["What code/settings?", "git_commit, params_json, seed, env_lock_sha256", "registry columns"],
        ["What if a bad load happened?", "audit, quarantine, rollback, verify", "Section 5; rollback_log"],
        ["Could cleanup destroy evidence?", "tags + guard", "Section 7; T5"],
        ["Who did what, when?", "trained_by, status events, ingest_log, rollback_log", "audit tables"],
    ], ["Auditor asks", "Answer", "Evidence"]))
    A("")
    if results and results.get("registry_rows"):
        A("## Appendix: registry rows from the demo run\n")
        A(_md_table([[r.get("model_version"), r.get("snapshot_id"), str(r.get("git_commit"))[:10], str(r.get("artifact_sha256"))[:16], r.get("data_fingerprint")]
                     for r in results["registry_rows"]], ["model", "snapshot_id", "git", "artifact sha256", "data fingerprint"]))
    return "\n".join(L) + "\n"


# ---------------------------------------------------------------- tiny Markdown -> PDF
def md_to_pdf(md: str, out_pdf: str, base_dir: str = ".") -> None:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.pdfmetrics import registerFontFamily
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.platypus import Image, Paragraph, Preformatted, SimpleDocTemplate, Spacer, Table, TableStyle

    D = "/usr/share/fonts/truetype/dejavu/"
    base, bold, mono = "Helvetica", "Helvetica-Bold", "Courier"
    try:
        for n, f in (("DV", "DejaVuSans.ttf"), ("DV-B", "DejaVuSans-Bold.ttf"), ("DV-I", "DejaVuSans-Oblique.ttf"),
                     ("DV-BI", "DejaVuSans-BoldOblique.ttf"), ("DVM", "DejaVuSansMono.ttf")):
            pdfmetrics.registerFont(TTFont(n, D + f))
        registerFontFamily("DV", normal="DV", bold="DV-B", italic="DV-I", boldItalic="DV-BI")
        base, bold, mono = "DV", "DV-B", "DVM"
    except Exception:
        pass
    navy = colors.HexColor("#14213D")
    st = {
        "p": ParagraphStyle("p", fontName=base, fontSize=9, leading=13, spaceAfter=5),
        "h1": ParagraphStyle("h1", fontName=bold, fontSize=17, leading=21, textColor=navy, spaceAfter=8),
        "h3": ParagraphStyle("h3", fontName=bold, fontSize=10, leading=13, textColor=navy, spaceBefore=8, spaceAfter=3),
        "h2": ParagraphStyle("h2", fontName=bold, fontSize=12, leading=15, textColor=colors.HexColor("#0F766E"), spaceBefore=10, spaceAfter=4),
        "li": ParagraphStyle("li", fontName=base, fontSize=9, leading=13, leftIndent=12, bulletIndent=2, spaceAfter=2),
        "cell": ParagraphStyle("cell", fontName=base, fontSize=7.4, leading=9.6),
        "code": ParagraphStyle("code", fontName=mono, fontSize=7, leading=9),
    }

    def inline(t: str) -> str:
        t = html.escape(t)
        t = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", t)
        t = re.sub(r"(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)", r"<i>\1</i>", t)
        return re.sub(r"`(.+?)`", rf'<font name="{mono}">\1</font>', t)

    story, lines, i = [], md.splitlines(), 0
    width = A4[0] - 36 * mm
    while i < len(lines):
        ln = lines[i]
        if ln.startswith("```"):
            j = i + 1
            while j < len(lines) and not lines[j].startswith("```"):
                j += 1
            story.append(Preformatted("\n".join(lines[i + 1:j]), st["code"])); i = j + 1; continue
        if ln.startswith("|"):
            j = i
            while j < len(lines) and lines[j].startswith("|"):
                j += 1
            rows = [[c.strip() for c in r.strip().strip("|").split("|")] for r in lines[i:j] if not re.match(r"^\|[-| :]+\|$", r)]
            n = max(len(r) for r in rows)
            data = [[Paragraph(inline(c), st["cell"]) for c in r + [""] * (n - len(r))] for r in rows]
            t = Table(data, colWidths=[width / n] * n, repeatRows=1)
            t.setStyle(TableStyle([("GRID", (0, 0), (-1, -1), 0.25, colors.grey), ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#E5E7EB")),
                                   ("VALIGN", (0, 0), (-1, -1), "TOP")]))
            story += [t, Spacer(1, 6)]; i = j; continue
        m = re.match(r"!\[(.*?)\]\((.*?)\)", ln)
        if m:
            if (Path(base_dir) / m.group(2)).exists():
                img = Image(str(Path(base_dir) / m.group(2))); r = min(width * 0.8 / img.imageWidth, 1.0)
                img.drawWidth, img.drawHeight = img.imageWidth * r, img.imageHeight * r
                story += [img, Spacer(1, 6)]
        elif ln.startswith("> "):
            story.append(Paragraph("<b>" + inline(ln[2:]) + "</b>", ParagraphStyle("warn", parent=st["p"], textColor=colors.HexColor("#B91C1C"))))
        elif ln.startswith("### "):
            story.append(Paragraph(inline(ln[4:]), st["h3"]))
        elif ln.startswith("# "):
            story.append(Paragraph(inline(ln[2:]), st["h1"]))
        elif ln.startswith("## "):
            story.append(Paragraph(inline(ln[3:]), st["h2"]))
        elif ln.startswith("- "):
            story.append(Paragraph(inline(ln[2:]), st["li"], bulletText="\u2022"))
        elif ln.strip():
            story.append(Paragraph(inline(ln), st["p"]))
        i += 1
    SimpleDocTemplate(out_pdf, pagesize=A4, leftMargin=18 * mm, rightMargin=18 * mm, topMargin=16 * mm, bottomMargin=16 * mm,
                      title="Audit-Readiness Report").build(story)


def generate(reports_dir: str = "reports") -> tuple[str, str]:
    d = Path(reports_dir)
    res = json.loads((d / "demo_results.json").read_text(encoding="utf-8")) if (d / "demo_results.json").exists() else None
    sp = d / "storage" / "storage_results.csv"
    storage = pd.read_csv(sp) if sp.exists() else None
    card = (d / "data_card.md").read_text(encoding="utf-8") if (d / "data_card.md").exists() else None
    snp = d / "storage" / "snapshot_storage.csv"
    snap = pd.read_csv(snp) if snp.exists() else None
    md = build_markdown(res, storage, load_junit(d / "junit.xml"), card, reports_dir=str(d), snap_storage=snap)
    mp, pp = d / "audit_readiness_report.md", d / "audit_readiness_report.pdf"
    mp.write_text(md, encoding="utf-8")
    md_to_pdf(md, str(pp), base_dir=str(d))
    return str(mp), str(pp)
