#!/usr/bin/env python3
"""Append Candidate Daily Phase 3 R13 Master recovery decisions to R12."""

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
    canvas.drawRightString(width - 18 * mm, height - 11.2 * mm, "CANDIDATE DAILY PHASE 3 R13 - 17 AUGUST 2026")
    canvas.setStrokeColor(GOLD)
    canvas.setLineWidth(1.1)
    canvas.line(18 * mm, 16 * mm, width - 18 * mm, 16 * mm)
    canvas.setFillColor(MUTED)
    canvas.setFont("Helvetica", 7.3)
    canvas.drawString(18 * mm, 10.5 * mm, "Sections 98-100 - quota-safe generation recovery and TEST proving gate")
    canvas.drawRightString(width - 18 * mm, 10.5 * mm, f"R13 addendum page {doc.page}")
    canvas.restoreState()


def section(story, sty, title):
    story.append(PageBreak())
    story.append(Paragraph(markup(title), sty["section"]))


def build_addendum(output: Path, facts: dict[str, str]) -> None:
    sty = styles()
    doc = BaseDocTemplate(
        str(output), pagesize=A4, rightMargin=18 * mm, leftMargin=18 * mm,
        topMargin=25 * mm, bottomMargin=22 * mm,
        title="CloudTMS Candidate App Current Decisions - Candidate Daily Phase 3 R13",
        author="Arthur Rai / CloudTMS",
        subject="Sections 98-100: quota-safe Master recovery and population-wide TEST gate",
    )
    frame = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height, id="main")
    doc.addPageTemplates([PageTemplate(id="r13", frames=[frame], onPage=page_canvas)])

    story = [Spacer(1, 8 * mm)]
    story.append(Paragraph("Current Decisions", sty["title"]))
    story.append(Paragraph("Candidate Daily Phase 3", sty["sub"]))
    story.append(Paragraph("R13 Master generation durability correction - Sections 98-100", sty["sub"]))
    story.append(table(sty, ["Authority", "Current fact"], [
        ["Decision date", "17 August 2026"],
        ["Base authority", "Accepted Sections 1-97 and AV-001 through AV-280 remain controlling except where R13 is explicitly later-controlling."],
        ["R12 review", "Availability findings remain closed. Three executable Master durability/quota defects require correction."],
        ["R13 correction", "Freeze every generation command before POST in quota-safe verified chunks; recover pending before new work; classify exact route results."],
        ["Population decision", "The first enabled TEST exercise is population-wide. Kier Arthur is the first observational phone-app journey only."],
        ["Google authority", "Availability version 216 and Master version 102 remain active. The R13 Master helper is saved to Head but deliberately undeployed pending independent GO."],
        ["Feature state", "Both bridge flags, Candidate entitlements and public Candidate features remain disabled."],
    ], [43 * mm, 120 * mm]))
    story.append(Spacer(1, 5 * mm))
    story.append(callout(sty, "Source publication and an undeployed Apps Script Head do not activate the bridge."))

    section(story, sty, "98. Quota-safe immutable Master generation authority")
    story.append(Paragraph(
        "Every accepted legacy update-end event is owned by one ordered pending index. The complete immutable command must be recoverable before any signed POST occurs.",
        sty["body"],
    ))
    story.append(table(sty, ["Boundary", "R13 requirement"], [
        ["Pending owner", "CTMS_P3_ROTA_PENDING_INDEX lists ordered manifest identities for the complete event."],
        ["Manifest", "Freezes body SHA-256, exact UTF-8 byte length, chunk keys, batch UUID, idempotency key, correlation ID, item count and disposition."],
        ["Body chunks", "UTF-8-safe numbered values, each no more than 7,000 bytes."],
        ["Property store", "Preflight bridge-owned total below 480,000 bytes. Insufficient capacity means no POST."],
        ["Worker request", "No more than 50 items and no more than 245,760 serialized UTF-8 bytes."],
        ["Reassembly", "Verify ordered byte length and SHA-256 before every replay."],
        ["Corruption", "Fail closed without deleting the owner or inventing a replacement command."],
    ], [42 * mm, 121 * mm]))
    story.append(Spacer(1, 5 * mm))
    bullets(story, sty, [
        "All batches are persisted before the first request for the logical event.",
        "The seven-day automatic replacement rule is removed.",
        "A partial/orphan body is never treated as an executable command.",
        "The existing Availability publication remains first and authoritative for the temporary legacy path.",
    ])
    story.append(callout(sty, "AV-281 to AV-286 are later-controlling for Master generation persistence.", PALE_GOLD, GOLD))

    section(story, sty, "99. Exact recovery, disposition and population authority")
    story.append(table(sty, ["Exact outcome", "Ownership"], [
        ["2xx plus ok=true", "That frozen batch succeeds and may be cleared."],
        ["409 BATCH_IN_PROGRESS / STATUS_CHECK", "Retain and exact-replay the same body/key/correlation; no completion log."],
        ["409 SOURCE_EVENT_CONFLICT / DO_NOT_RETRY", "Explicit terminal rejection; never completion."],
        ["422 GENERATION_INCOMPLETE / DO_NOT_RETRY", "Explicit terminal rejection; never completion."],
        ["429, 5xx, transport error", "Retain exact command."],
        ["Malformed/unknown result", "Retain exact command."],
    ], [55 * mm, 108 * mm]))
    story.append(Spacer(1, 5 * mm))
    bullets(story, sty, [
        "Pending recovery always runs before a later accepted event can build a new generation.",
        "A new timestamp, UUID, key, correlation or body is forbidden while any prior batch remains unresolved.",
        "Overall completion is logged once only after every ordered batch succeeds.",
        "The product owner selected population-wide TEST enablement; no candidate-specific allowlist or hard-coded Kier identity exists.",
        "Every eligible Google source row must have one exact TEST source link before enablement. Bridge enablement never creates links.",
        "An existing global Candidate key proves the established Candidate-product mapping but does not replace the Daily Google-source HMAC catalogue.",
        "Controlled source-link bootstrap binds the HMAC to the existing Candidate UUID and never creates or replaces a Candidate record.",
        "A new-app-only candidate joins through the admin-entered global Candidate key and does not need to use the temporary legacy browser.",
        "Google generates the CID1 global key from normalized Credentially Public ID; the administrator enters that generated CID1 value in CloudTMS.",
        "Kier Arthur is the first observational phone-app journey, not a runtime scope boundary.",
    ])
    story.append(callout(sty, "AV-287 to AV-298 freeze recovery ordering, population scope, Google-generated CID1 mapping, distinct source-HMAC authority and app-only onboarding to the same Candidate UUID."))

    section(story, sty, "100. Verification, deployment gate and remaining full product")
    story.append(table(sty, ["Evidence", "R13 result"], [
        ["Focused Phase 1A/1B/2/3/R13", facts["focused_result"]],
        ["R13 Master recovery/quota", "19 passed, 0 failed."],
        ["Complete backend JavaScript", facts["complete_result"]],
        ["Candidate Worker builds", "Public broker and private Candidate dry builds pass; no Worker runtime changed."],
        ["Real TEST broker negative proof", "Valid route/body with deliberately invalid signing authority fails closed at SYSTEM_AUTH_FAILED; no source or business mutation."],
        ["Source-link readiness", "Kier's existing global Candidate key is present and matches; that key is distinct from the Daily Google-source HMAC catalogue, whose active Kier and overall TEST counts are zero."],
        ["App-only onboarding", "Google generates CID1 from normalized Credentially Public ID; the administrator enters that exact value; bootstrap binds the separate source HMAC to the same existing UUID."],
        ["Google deployment", "R13 Master Head is saved but not deployed. Independent GO is required first."],
    ], [52 * mm, 111 * mm]))
    story.append(Spacer(1, 5 * mm))
    story.append(table(sty, ["Next phase/gate", "Outcome"], [
        ["R13 GO/deploy", "Deploy Master while false; prove source/trigger/legacy parity; bootstrap exact source links."],
        ["Phase 3 enabled proving", "Population-wide TEST bridge with Kier observed first; signed generation, tiles, writes, recovery, projection, quota/latency/outage and rollback."],
        ["Phase 4", "Complete responsive Candidate web, iOS and Android plus retained specialist interfaces."],
        ["Phase 5", "Controlled TEST cutover, parity, soak, error budget and rollback."],
        ["Phase 6", "Emergency and attendance effects, messages/content, Past Shifts, DAILY signing and EMAIL/PHONE acceptance."],
        ["Phase 7", "Gradual rollout, monitoring and separately authorised retirement of the temporary browser adapter."],
    ], [38 * mm, 125 * mm]))
    story.append(Spacer(1, 5 * mm))
    story.append(callout(sty, "AV-293 to AV-298 bind observational scope, source-link readiness, Google-generated CID1 mapping, existing Candidate-row preservation, app-only onboarding and the independent-GO-before-Google-deployment rule."))

    doc.build(story)


def merge(base_path: Path, addendum_path: Path, output_path: Path) -> tuple[int, int, int]:
    base = PdfReader(str(base_path))
    addendum = PdfReader(str(addendum_path))
    if len(base.pages) != 109:
        raise RuntimeError(f"Expected R12 base to contain 109 pages; found {len(base.pages)}")
    writer = PdfWriter()
    for page in base.pages:
        writer.add_page(page)
    for page in addendum.pages:
        writer.add_page(page)
    writer.add_metadata({
        "/Title": "CloudTMS Candidate App Current Decisions - Candidate Daily Phase 3 R13",
        "/Author": "Arthur Rai / CloudTMS",
        "/Subject": "Current decisions plus quota-safe Master generation recovery",
        "/Keywords": "CloudTMS, Candidate App, Daily, Phase 3, R13, Apps Script, recovery",
    })
    with output_path.open("wb") as handle:
        writer.write(handle)
    return len(base.pages), len(addendum.pages), len(base.pages) + len(addendum.pages)


def verify(base_path: Path, output_path: Path) -> None:
    base = PdfReader(str(base_path))
    output = PdfReader(str(output_path))
    if len(output.pages) <= len(base.pages):
        raise RuntimeError("Combined PDF contains no R13 addendum")
    for index, page in enumerate(base.pages):
        if (page.extract_text() or "") != (output.pages[index].extract_text() or ""):
            raise RuntimeError(f"R12 base page text changed at page {index + 1}")
        if page.mediabox != output.pages[index].mediabox:
            raise RuntimeError(f"R12 base page dimensions changed at page {index + 1}")
    appended = re.sub(r"\s+", " ", "\n".join((p.extract_text() or "") for p in output.pages[len(base.pages):]))
    required = [
        "98. Quota-safe immutable Master", "99. Exact recovery", "100. Verification",
        "AV-281", "AV-298", "7,000", "480,000", "245,760",
        "population-wide TEST", "Kier Arthur", "version 102", "19 passed, 0 failed",
    ]
    missing = [item for item in required if item not in appended]
    if missing:
        raise RuntimeError(f"Required R13 decisions missing: {missing}")


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
    print(f"DECISIONS_R13_PASS|base_pages={base_pages}|addendum_pages={addendum_pages}|total_pages={total_pages}|sha256={sha256(output)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
