#!/usr/bin/env python3
"""Append the Candidate Daily Phase 2/1B R10 rollback correction to the R9 PDF."""

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
    canvas.drawRightString(width - 18 * mm, height - 11.2 * mm, "PHASE 2 + PHASE 1B R10 - 17 AUGUST 2026")
    canvas.setStrokeColor(GOLD)
    canvas.setLineWidth(1.1)
    canvas.line(18 * mm, 16 * mm, width - 18 * mm, 16 * mm)
    canvas.setFillColor(MUTED)
    canvas.setFont("Helvetica", 7.3)
    canvas.drawString(18 * mm, 10.5 * mm, "Sections 85-88 - first-rollback unresolved-work correction")
    canvas.drawRightString(width - 18 * mm, 10.5 * mm, f"R10 addendum page {doc.page}")
    canvas.restoreState()


def section(story, sty, title):
    story.append(PageBreak())
    story.append(Paragraph(markup(title), sty["section"]))


def build_addendum(output: Path, facts: dict[str, str]) -> None:
    sty = styles()
    doc = BaseDocTemplate(
        str(output), pagesize=A4, rightMargin=18 * mm, leftMargin=18 * mm,
        topMargin=25 * mm, bottomMargin=22 * mm,
        title="CloudTMS Candidate App Current Decisions - Phase 2 and Phase 1B R10",
        author="Arthur Rai / CloudTMS",
        subject="Sections 85-88: Candidate Daily first-rollback unresolved-work correction",
    )
    frame = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height, id="main")
    doc.addPageTemplates([PageTemplate(id="r10", frames=[frame], onPage=page_canvas)])

    story = [Spacer(1, 10 * mm)]
    story.append(Paragraph("Current Decisions", sty["title"]))
    story.append(Paragraph("Candidate Daily Phase 2 and Phase 1B", sty["sub"]))
    story.append(Paragraph("Later-controlling R10 correction - Sections 85-88", sty["sub"]))
    story.append(table(sty, ["Authority", "Current fact"], [
        ["Decision date", "17 August 2026"],
        ["Base authority", "Accepted R5/R7, R8 architecture and R9 locked transition proof. Sections 1-84 remain controlling except for the bounded first-rollback correction here."],
        ["Independent finding", "R9 derived unresolved work as NONE, but the first SUPABASE_PRIMARY to ROLLBACK_PENDING edge could still commit when the caller truthfully supplied NONE."],
        ["R10 correction", "Every changed authority mode now rejects database-derived NONE as CANDIDATE_DAILY_NOT_READY after caller/database disposition equality."],
        ["Transport status", "Phase 1B GO remains in force. R10 changes no private/public HTTP mapping."],
        ["Product state", "Installed in TEST for review only; all Candidate flags remain false and Candidate/Daily tables remain empty."],
        ["Next gate", "Fresh independent R10 review. Phase 3 remains blocked until Phase 2 receives GO."],
    ], [42 * mm, 121 * mm]))
    story.append(Spacer(1, 6 * mm))
    story.append(callout(sty, "NONE accurately describes unresolved work. Accuracy does not make unresolved work safe for an authority switch."))

    section(story, sty, "85. R9 finding and exact R10 disposition")
    story.append(Paragraph("The R9 package materially passed its locked database proof, Phase 1B mappings, access control, disabled-state safety and dual-engine matrix. Its remaining blocker was one missing semantic barrier on the first rollback edge. The equality check rejected a false DRAINED claim, but a truthful NONE claim could pass because that edge did not use the later strict parity block.", sty["body"]))
    story.append(table(sty, ["Boundary", "R10 decision"], [
        ["Runtime", "Add one database-owned guard after derived/caller disposition equality."],
        ["First rollback", "SUPABASE_PRIMARY to ROLLBACK_PENDING rejects NONE before writing entitlement, mode or ledger."],
        ["Final rollback", "Existing complete R9 Google parity barrier remains unchanged."],
        ["No-op", "Same-mode exact no-op with NONE remains NO_CHANGE."],
        ["Schema/RPC/API", "No table, signature, RPC, route, request or response change."],
        ["Protected owners", "No Google, Office, finance, Invoice, Banking Pay, Policy X, provider or production change."],
    ], [42 * mm, 121 * mm]))

    section(story, sty, "86. Stable semantics and preserved settled paths")
    story.append(table(sty, ["Locked outcome", "Required result"], [
        ["Caller DRAINED; DB NONE", "SEMANTIC_REJECTION because the caller assertion is false."],
        ["Caller NONE; DB NONE; mode changes", "CANDIDATE_DAILY_NOT_READY because unresolved work blocks authority movement."],
        ["Caller NONE; DB NONE; exact mode/entitlement no-op", "Existing NO_CHANGE result; no transition ledger row."],
        ["Caller DRAINED; DB DRAINED", "Eligible to continue through every other applicable R9 barrier."],
        ["Caller RECONCILED; DB RECONCILED", "Eligible only with the existing exact deferred-overlay generation/date/hash proof."],
    ], [50 * mm, 113 * mm]))
    story.append(Spacer(1, 5 * mm))
    story.append(callout(sty, "The first rollback stage is not final Google cutover. It may accept settled DRAINED or exact RECONCILED truth, but never unresolved NONE.", PALE_GOLD, GOLD))
    bullets(story, sty, [
        "The global Candidate Daily flag must already be false.",
        "Candidate entitlement must become or remain false.",
        "Expected mode, version and entitlement must equal locked database truth.",
        "The item subtransaction rolls back its transition fence and all local writes on rejection.",
        "The final ROLLBACK_PENDING to GOOGLE_PRIMARY edge retains full source, generation, cursor, reconciliation and overlay proof.",
    ])

    section(story, sty, "87. Complete first-rollback adversarial matrix")
    story.append(table(sty, ["Unresolved database owner", "R10 proof"], [
        ["Projection PENDING", "False DRAINED conflicts; truthful NONE is not ready."],
        ["Projection CLAIMED", "False DRAINED conflicts; truthful NONE is not ready."],
        ["Projection RETRY", "False DRAINED conflicts; truthful NONE is not ready."],
        ["Projection TERMINAL", "False DRAINED conflicts; truthful NONE is not ready."],
        ["Candidate command IN_PROGRESS", "Truthful NONE is not ready."],
        ["Other Candidate Daily batch IN_PROGRESS", "Truthful NONE is not ready."],
        ["External effect IN_PROGRESS", "Truthful NONE is not ready."],
        ["External effect UNKNOWN", "Truthful NONE is not ready."],
        ["Two concurrent different-key attempts", "Both reject; mode/fence/ledger remain unchanged."],
    ], [53 * mm, 110 * mm]))
    story.append(Spacer(1, 5 * mm))
    story.append(table(sty, ["Decision IDs", "Later-controlling family"], [
        ["AV-245", "NONE never authorises a changed authority mode"],
        ["AV-246", "Every unresolved owner blocks the first rollback edge"],
        ["AV-247", "False DRAINED and truthful unresolved NONE remain distinct"],
        ["AV-248", "No rejected/concurrent rollback authority, entitlement, ledger or fence drift"],
        ["AV-249", "Valid settled/no-op/R9 barriers remain intact"],
    ], [38 * mm, 125 * mm]))

    section(story, sty, "88. Verification, deployment and next gate")
    story.append(table(sty, ["Gate", "R10 fact"], [
        ["Runtime commit", facts["runtime_commit"]],
        ["PostgreSQL", facts["postgres_result"]],
        ["JavaScript", facts["javascript_result"]],
        ["Candidate DB workflow", facts["candidate_db_run"]],
        ["Safe migration/install", facts["safe_migration_run"]],
        ["Repeatable file SHA-256", facts["repeatable_sha256"]],
        ["Installed function SHA-256", facts["function_sha256"]],
        ["TEST Worker versions", "normal " + facts["normal_worker_version"] + "; private " + facts["private_worker_version"] + "; public " + facts["public_worker_version"]],
        ["Safety", "All Candidate flags false; Candidate core/Daily/mail rows empty; no real transition/effect; production untouched."],
    ], [48 * mm, 115 * mm]))
    story.append(Spacer(1, 5 * mm))
    story.append(callout(sty, "Requested verdict: GO or one bounded evidence-backed NO-GO for R10. A GO closes the outstanding Phase 2 rollback blocker while retaining Phase 1B GO and permits the planned Phase 3 gate only. It does not activate Candidate Daily or complete the full Candidate App."))

    doc.build(story)


def merge(base_path: Path, addendum_path: Path, output_path: Path) -> tuple[int, int, int]:
    base = PdfReader(str(base_path))
    addendum = PdfReader(str(addendum_path))
    if len(base.pages) != 93:
        raise RuntimeError(f"Expected R9 base to contain 93 pages; found {len(base.pages)}")
    writer = PdfWriter()
    for page in base.pages:
        writer.add_page(page)
    for page in addendum.pages:
        writer.add_page(page)
    writer.add_metadata({
        "/Title": "CloudTMS Candidate App Current Decisions - Phase 2 and Phase 1B R10",
        "/Author": "Arthur Rai / CloudTMS",
        "/Subject": "Current decisions plus Candidate Daily first-rollback unresolved-work correction",
        "/Keywords": "CloudTMS, Candidate App, Daily, Phase 2, Phase 1B, R10, rollback",
    })
    with output_path.open("wb") as handle:
        writer.write(handle)
    return len(base.pages), len(addendum.pages), len(base.pages) + len(addendum.pages)


def verify(base_path: Path, output_path: Path) -> None:
    base = PdfReader(str(base_path))
    output = PdfReader(str(output_path))
    if len(output.pages) <= len(base.pages):
        raise RuntimeError("Combined PDF contains no R10 addendum")
    for index, page in enumerate(base.pages):
        if (page.extract_text() or "") != (output.pages[index].extract_text() or ""):
            raise RuntimeError(f"R9 base page text changed at page {index + 1}")
        if page.mediabox != output.pages[index].mediabox:
            raise RuntimeError(f"R9 base page dimensions changed at page {index + 1}")
    appended = re.sub(r"\s+", " ", "\n".join(
        (page.extract_text() or "") for page in output.pages[len(base.pages):]
    ))
    required = [
        "85. R9 finding and exact R10 disposition",
        "86. Stable semantics and preserved settled paths",
        "87. Complete first-rollback adversarial matrix",
        "88. Verification, deployment and next gate",
        "AV-245",
        "does not activate Candidate Daily",
    ]
    missing = [item for item in required if item not in appended]
    if missing:
        raise RuntimeError(f"Required R10 decisions missing: {missing}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--runtime-commit", required=True)
    parser.add_argument("--candidate-db-run", required=True)
    parser.add_argument("--safe-migration-run", required=True)
    parser.add_argument("--repeatable-sha256", required=True)
    parser.add_argument("--function-sha256", required=True)
    parser.add_argument("--postgres-result", required=True)
    parser.add_argument("--javascript-result", required=True)
    parser.add_argument("--normal-worker-version", required=True)
    parser.add_argument("--private-worker-version", required=True)
    parser.add_argument("--public-worker-version", required=True)
    args = parser.parse_args()
    facts = vars(args)
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    addendum = output.with_name(output.stem + "_addendum_only.pdf")
    build_addendum(addendum, facts)
    base_pages, addendum_pages, total_pages = merge(args.base.resolve(), addendum, output)
    verify(args.base.resolve(), output)
    addendum.unlink()
    print(
        f"DECISIONS_R10_PASS|base_pages={base_pages}|addendum_pages={addendum_pages}|"
        f"total_pages={total_pages}|sha256={sha256(output)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
