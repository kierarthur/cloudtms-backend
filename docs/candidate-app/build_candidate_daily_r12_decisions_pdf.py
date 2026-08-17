#!/usr/bin/env python3
"""Append the Candidate Daily Phase 3 R12 correction and installation decisions to R11."""

from __future__ import annotations

import argparse
import hashlib
import re
from pathlib import Path

from pypdf import PdfReader, PdfWriter
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.platypus import BaseDocTemplate, Frame, PageBreak, PageTemplate, Paragraph, Spacer

from build_candidate_daily_r6_decisions_pdf import (
    GOLD, MUTED, NAVY, PALE_GOLD, bullets, callout, markup, styles, table,
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def page_canvas(canvas, doc):
    canvas.saveState()
    width, height = A4
    canvas.setFillColor(NAVY)
    canvas.rect(0, height - 18 * mm, width, 18 * mm, fill=1, stroke=0)
    canvas.setFillColor(colors.white)
    canvas.setFont("Helvetica-Bold", 8)
    canvas.drawString(18 * mm, height - 11.2 * mm, "CLOUDTMS CANDIDATE APP - CURRENT DECISIONS")
    canvas.setFont("Helvetica", 7.5)
    canvas.drawRightString(width - 18 * mm, height - 11.2 * mm, "CANDIDATE DAILY PHASE 3 R12 - 17 AUGUST 2026")
    canvas.setStrokeColor(GOLD)
    canvas.setLineWidth(1.1)
    canvas.line(18 * mm, 16 * mm, width - 18 * mm, 16 * mm)
    canvas.setFillColor(MUTED)
    canvas.setFont("Helvetica", 7.3)
    canvas.drawString(18 * mm, 10.5 * mm, "Sections 95-97 - durable accepted subset and disabled Google deployment")
    canvas.drawRightString(width - 18 * mm, 10.5 * mm, f"R12 addendum page {doc.page}")
    canvas.restoreState()


def section(story, sty, title):
    story.append(PageBreak())
    story.append(Paragraph(markup(title), sty["section"]))


def build_addendum(output: Path, facts: dict[str, str]) -> None:
    sty = styles()
    doc = BaseDocTemplate(
        str(output), pagesize=A4, rightMargin=18 * mm, leftMargin=18 * mm,
        topMargin=25 * mm, bottomMargin=22 * mm,
        title="CloudTMS Candidate App Current Decisions - Candidate Daily Phase 3 R12",
        author="Arthur Rai / CloudTMS",
        subject="Sections 95-97: accepted-subset correction and disabled Google deployment",
    )
    frame = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height, id="main")
    doc.addPageTemplates([PageTemplate(id="r12", frames=[frame], onPage=page_canvas)])

    story = [Spacer(1, 8 * mm)]
    story.append(Paragraph("Current Decisions", sty["title"]))
    story.append(Paragraph("Candidate Daily Phase 3", sty["sub"]))
    story.append(Paragraph("R12 correction and disabled Google deployment - Sections 95-97", sty["sub"]))
    story.append(table(sty, ["Authority", "Current fact"], [
        ["Decision date", "17 August 2026"],
        ["Base authority", "Accepted Sections 1-94 and AV-001 through AV-270 remain controlling except where R12 is explicitly later-controlling."],
        ["R11 audit result", "Three bounded defects existed in the Availability write/mirror/recovery family; the R10 database/API GO remains intact."],
        ["R12 correction", "Mirror only durable accepted legacy rows; never mirror deferred/rejected rows; retain every STATUS_CHECK or uncertain operation."],
        ["Google installation", "Availability API version 216 and NEW MASTER ROTA version 102 are deployed in TEST."],
        ["Disabled invariant", "CLOUDTMS_CANDIDATE_BRIDGE_ENABLED=false remains binding; legacy behaviour only and zero bridge traffic."],
        ["Feature state", "No Candidate flag, entitlement, source link, Candidate business data, provider effect or production authority is enabled."],
        ["Next gate", "Independent R12 review, then separately authorised one-cohort enabled TEST proving."],
    ], [43 * mm, 120 * mm]))
    story.append(Spacer(1, 5 * mm))
    story.append(callout(sty, "Installing and versioning the source while the bridge is false is not feature activation."))

    section(story, sty, "95. Durable accepted-subset and ordering authority")
    story.append(Paragraph("The completed legacy write result is the sole factual authority for an Availability mirror. The original browser request is never copied wholesale into CloudTMS.", sty["body"]))
    story.append(table(sty, ["Decision", "Required result"], [
        ["Accepted row", "Only applied=true and deferred!=true, with exact YYYY-MM-DD date and a closed legacy code."],
        ["All rejected", "Identity, state, log and network no-op; return the exact existing legacy response."],
        ["Mixed result", "Freeze and sign only the durable accepted subset; rejected and deferred dates are absent."],
        ["Busy/deferred", "Do not mirror at enqueue time. Preserve the existing deferred response."],
        ["Queue flush", "Perform existing value/background writes first; collect only successful rows; release the legacy write lock; then mirror that exact subset."],
        ["Malformed result", "Fail open toward legacy truth without inventing a CloudTMS command."],
    ], [42 * mm, 121 * mm]))
    story.append(Spacer(1, 5 * mm))
    bullets(story, sty, [
        "Accepted dates are unique and exact; duplicate or contradictory accepted facts fail open.",
        "The persisted fingerprint and signed body are derived from the same normalized accepted subset.",
        "A failed legacy Sheet row cannot reach CloudTMS merely because it appeared in the browser request.",
        "No bridge network operation occurs while the legacy write lock is held.",
    ])
    story.append(callout(sty, "AV-271 to AV-275 are later-controlling for the Availability write family.", PALE_GOLD, GOLD))

    section(story, sty, "96. Closed recovery contract and self-contained evidence")
    story.append(Paragraph("HTTP status alone is not terminal authority. Every response is classified by the exact route-specific triple HTTP status, error_code and retry_class.", sty["body"]))
    story.append(table(sty, ["Disposition", "Operation ownership"], [
        ["2xx plus ok=true", "Authoritative success; render/return durable result and clear the operation."],
        ["Approved terminal DO_NOT_RETRY or REFRESH triple", "Authoritative rejection; bounded log and clear only when the closed catalogue explicitly permits it."],
        ["STATUS_CHECK", "Non-terminal; retain the exact operation and probe status."],
        ["RETRY_SAME_KEY / RETRY_AFTER / transport uncertainty", "Retain exact request, key, correlation, source HMAC and body; probe status first."],
        ["Malformed or unknown 4xx", "Fail closed as uncertain; do not delete the recovery identity."],
        ["Status 404 / NOT_FOUND / DO_NOT_RETRY", "Only authoritative not-found; permits the one exact retry when not already consumed."],
        ["Not-found after retry consumed", "Status-only forever until an authoritative result; never execute a second retry."],
    ], [50 * mm, 113 * mm]))
    story.append(Spacer(1, 5 * mm))
    story.append(table(sty, ["Evidence gate", "R12 result"], [
        ["Focused Phase 1A/1B/2/3", facts["focused_result"]],
        ["Complete backend JavaScript", facts["complete_result"]],
        ["Phase 3 standalone inside archive", "18 passed, 0 failed from extracted pack root with the packaged canonical vector."],
        ["R11 independent report", "Included unmodified with the archive and standalone report identity recorded."],
        ["Local-path/secrets gate", "No machine-local path or secret value is allowed in the archive."],
    ], [54 * mm, 109 * mm]))
    story.append(Spacer(1, 5 * mm))
    story.append(callout(sty, "AV-276 to AV-278 freeze the closed recovery catalogue, one-retry limit and self-contained audit rule."))

    section(story, sty, "97. Installed TEST authority, rollback and next phases")
    story.append(table(sty, ["Surface", "Installed R12 authority"], [
        ["Availability API", "Complete corrected Code.gs plus CloudTMSCandidateBridge.gs; active web-app version 216; version 215 retained."],
        ["NEW MASTER ROTA", "One minimal existing-post seam plus CloudTMSCandidateBridge.gs; active web-app version 102; version 101 retained; operator configuration helper preserved."],
        ["Google properties", "Operator-installed TEST property names; secret values neither read nor packaged; bridge false."],
        ["Candidate Workers", "Public broker and private verifier retain the accepted Phase 1B route family; secret-change versions are current; no Worker source change or deployment in R12."],
        ["Database", "No schema, RPC or Candidate row change. R10 database/API GO remains controlling."],
        ["Legacy estate", "No UI, login, msisdn, Sheet shape, cache, manifest, scope, trigger, Emergency or specialist change."],
    ], [48 * mm, 115 * mm]))
    story.append(Spacer(1, 5 * mm))
    story.append(table(sty, ["Phase", "Remaining full-product outcome"], [
        ["Phase 3 proving", "Independent R12 acceptance; one approved TEST cohort; signed route, generation, recovery, projection, quota/latency/outage soak."],
        ["Phase 4", "Complete responsive Candidate web/iOS/Android journeys and retained specialist interfaces."],
        ["Phase 5", "Controlled TEST cutover with identity, parity, soak, error-budget and rollback proof."],
        ["Phase 6", "Emergency, cannot-attend, leave-early, running-late, DNA, messages/content, Past Shifts, DAILY signing and EMAIL/PHONE acceptance."],
        ["Phase 7", "Gradual entitled rollout, monitoring and separately authorised retirement of the temporary browser adapter."],
    ], [35 * mm, 128 * mm]))
    story.append(Spacer(1, 5 * mm))
    story.append(callout(sty, "AV-279 and AV-280 record disabled installation/deployment and preserve the full later-phase scope. No production or feature enablement is authorised."))

    doc.build(story)


def merge(base_path: Path, addendum_path: Path, output_path: Path) -> tuple[int, int, int]:
    base = PdfReader(str(base_path))
    addendum = PdfReader(str(addendum_path))
    if len(base.pages) != 105:
        raise RuntimeError(f"Expected R11 base to contain 105 pages; found {len(base.pages)}")
    writer = PdfWriter()
    for page in base.pages:
        writer.add_page(page)
    for page in addendum.pages:
        writer.add_page(page)
    writer.add_metadata({
        "/Title": "CloudTMS Candidate App Current Decisions - Candidate Daily Phase 3 R12",
        "/Author": "Arthur Rai / CloudTMS",
        "/Subject": "Current decisions plus Candidate Daily R12 correction and disabled Google deployment",
        "/Keywords": "CloudTMS, Candidate App, Daily, Phase 3, R12, Apps Script, coexistence",
    })
    with output_path.open("wb") as handle:
        writer.write(handle)
    return len(base.pages), len(addendum.pages), len(base.pages) + len(addendum.pages)


def verify(base_path: Path, output_path: Path) -> None:
    base = PdfReader(str(base_path))
    output = PdfReader(str(output_path))
    if len(output.pages) <= len(base.pages):
        raise RuntimeError("Combined PDF contains no R12 addendum")
    for index, page in enumerate(base.pages):
        if (page.extract_text() or "") != (output.pages[index].extract_text() or ""):
            raise RuntimeError(f"R11 base page text changed at page {index + 1}")
        if page.mediabox != output.pages[index].mediabox:
            raise RuntimeError(f"R11 base page dimensions changed at page {index + 1}")
    appended = re.sub(r"\s+", " ", "\n".join((p.extract_text() or "") for p in output.pages[len(base.pages):]))
    required = [
        "95. Durable accepted-subset", "96. Closed recovery contract", "97. Installed TEST authority",
        "AV-271", "AV-280", "version 216", "version 102", "CLOUDTMS_CANDIDATE_BRIDGE_ENABLED=false",
        "18 passed, 0 failed", "Phase 7",
    ]
    missing = [item for item in required if item not in appended]
    if missing:
        raise RuntimeError(f"Required R12 decisions missing: {missing}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--focused-result", required=True)
    parser.add_argument("--complete-result", required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    addendum = output.with_name(output.stem + "_addendum_only.pdf")
    build_addendum(addendum, vars(args))
    base_pages, addendum_pages, total_pages = merge(args.base.resolve(), addendum, output)
    verify(args.base.resolve(), output)
    addendum.unlink()
    print(f"DECISIONS_R12_PASS|base_pages={base_pages}|addendum_pages={addendum_pages}|total_pages={total_pages}|sha256={sha256(output)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
