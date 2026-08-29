#!/usr/bin/env python3
"""Append the Candidate Daily Phase 2/1B R9 barrier correction to the R8 PDF."""

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
    canvas.drawRightString(width - 18 * mm, height - 11.2 * mm, "PHASE 2 + PHASE 1B R9 - 17 AUGUST 2026")
    canvas.setStrokeColor(GOLD)
    canvas.setLineWidth(1.1)
    canvas.line(18 * mm, 16 * mm, width - 18 * mm, 16 * mm)
    canvas.setFillColor(MUTED)
    canvas.setFont("Helvetica", 7.3)
    canvas.drawString(18 * mm, 10.5 * mm, "Sections 80-84 - database-owned authority transition correction")
    canvas.drawRightString(width - 18 * mm, 10.5 * mm, f"R9 addendum page {doc.page}")
    canvas.restoreState()


def section(story, sty, title):
    story.append(PageBreak())
    story.append(Paragraph(markup(title), sty["section"]))


def build_addendum(output: Path, facts: dict[str, str]) -> None:
    sty = styles()
    doc = BaseDocTemplate(
        str(output), pagesize=A4, rightMargin=18 * mm, leftMargin=18 * mm,
        topMargin=25 * mm, bottomMargin=22 * mm,
        title="CloudTMS Candidate App Current Decisions - Phase 2 and Phase 1B R9",
        author="Arthur Rai / CloudTMS",
        subject="Sections 80-84: database-owned Candidate Daily transition barrier",
    )
    frame = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height, id="main")
    doc.addPageTemplates([PageTemplate(id="r9", frames=[frame], onPage=page_canvas)])

    story = [Spacer(1, 10 * mm)]
    story.append(Paragraph("Current Decisions", sty["title"]))
    story.append(Paragraph("Candidate Daily Phase 2 and Phase 1B", sty["sub"]))
    story.append(Paragraph("Later-controlling R9 correction - Sections 80-84", sty["sub"]))
    story.append(table(sty, ["Authority", "Current fact"], [
        ["Decision date", "17 August 2026"],
        ["Base authority", "Accepted R5/R7 plus the R8 implementation. Sections 1-79 remain controlling except where this addendum corrects R8 transition proof."],
        ["Independent finding", "R8 documented cutover/rollback barriers but the transition RPC could commit without independently proving the complete locked database state."],
        ["R9 correction", "One existing RPC now owns the complete source, generation, cursor, reconciliation, overlay and in-flight proof in the same transaction as the immutable transition."],
        ["Product state", "Installed/deployed in TEST but all Candidate flags remain false and Candidate/Daily tables remain empty."],
        ["Next gate", "Fresh independent R9 operation-level review. No feature activation, Google edit, Candidate data, production or legacy retirement is authorised."],
    ], [42 * mm, 121 * mm]))
    story.append(Spacer(1, 6 * mm))
    story.append(callout(sty, "The request supplies expectations, not authority. PostgreSQL must lock, derive and freeze the database-winner facts before any mode change can commit."))

    section(story, sty, "80. R8 finding and bounded R9 disposition")
    story.append(Paragraph("The R8 package materially passed schema, RPC, broker/private mapping, access control and disabled-state review. Its single blocker was limited to candidate_daily_authority_transition_atomic_v1. The function accepted sparse caller transition facts and did not prove the complete cutover/rollback barrier described by AV-214 and AV-215.", sty["body"]))
    story.append(table(sty, ["Boundary", "R9 decision"], [
        ["Schema", "No change: exactly twelve Daily tables."],
        ["Public RPC catalogue", "No change: exactly thirteen service-role-only Daily RPCs."],
        ["HTTP/OpenAPI", "No change: existing Phase 1B mapping and response contract remain exact."],
        ["Runtime source", "Only the existing transition definition and executable regression workflow change."],
        ["Features/data", "Remain disabled and empty; R9 creates no real scope, source, entitlement or effect."],
        ["Protected owners", "No Google, Office, finance, Invoice, Banking Pay, Policy X, provider or production change."],
    ], [45 * mm, 118 * mm]))

    section(story, sty, "81. Locked database proof and derived disposition")
    story.append(table(sty, ["Locked owner", "Required proof"], [
        ["Feature/scope/entitlement", "Exact existing scope, expected mode/version/entitlement and closed global-switch relationship."],
        ["Source links", "Exactly one time-current PRIMARY in exactly one active link group."],
        ["Generation/days", "Exact active expected generation/version, published and complete with fourteen day rows."],
        ["Sync state", "READY plus accepted, required-visible and effective-visible cursors all equal the locked canonical version."],
        ["Current facts", "Reconciliation timestamp is no older than generation, availability and projection facts."],
        ["In-flight owners", "Commands, other batches, effects and all projection rows are locked and classified."],
    ], [45 * mm, 118 * mm]))
    story.append(Spacer(1, 5 * mm))
    story.append(callout(sty, "PostgreSQL derives DRAINED, RECONCILED or NONE. Caller CANCELLED is never derived and cannot authorise a transition.", PALE_GOLD, GOLD))

    section(story, sty, "82. Generation, cursor, overlay and rollback rules")
    story.append(table(sty, ["Rule", "Fail-closed behaviour"], [
        ["Generation", "Missing, BUILDING, partial, stale, wrong ID/version or incomplete day set rejects."],
        ["Cursor", "Missing sync, cursor lag, non-READY state, pending/retry/terminal count or missing revision/reconciliation rejects."],
        ["Overlay", "DEFERRED_OVERLAY counts only with exact generation ID/version, date and source-row hash."],
        ["Unresolved work", "PENDING, CLAIMED, RETRY, TERMINAL, command, other batch, IN_PROGRESS effect and UNKNOWN effect all block."],
        ["Forward", "Google to Supabase uses the full strict barrier; entitlement may remain false for dark proving."],
        ["Rollback", "Disable globally first, enter ROLLBACK_PENDING with entitlement false, then use the full strict barrier before Google becomes primary."],
    ], [42 * mm, 121 * mm]))

    section(story, sty, "83. Replay, concurrency, cohorts and immutable evidence")
    bullets(story, sty, [
        "A missing authority scope is rejected and never created by transition.",
        "An exact no-op returns NO_CHANGE and appends no transition.",
        "Same key plus the same request returns the exact durable batch result; changed facts under that key conflict.",
        "Concurrent exact callers serialize on one batch receipt and receive one result.",
        "Concurrent different-key cutovers serialize on deterministic scope locks; one commits and the other receives an explicit stale-precondition rejection.",
        "Every cohort item resets all local state and runs in an isolated subtransaction; one rejection cannot leak a source row, entitlement or transition fence.",
        "Every authority-changing ledger entry freezes the locked generation version, complete sync snapshot and database-derived in-flight disposition.",
    ])
    story.append(Spacer(1, 5 * mm))
    story.append(table(sty, ["Decision IDs", "Later-controlling family"], [
        ["AV-229-AV-232", "Existing scope, locked proof, derived disposition and one source authority"],
        ["AV-233-AV-236", "Complete generation, exact cursors/latest facts and exact overlay"],
        ["AV-237-AV-240", "All in-flight owners, entitlement/mode, two-stage rollback and winner snapshots"],
        ["AV-241-AV-244", "Partial cohorts, parallel replay/single winner, no-op and dual-engine executable proof"],
    ], [38 * mm, 125 * mm]))

    section(story, sty, "84. Verification, deployment and next gate")
    story.append(table(sty, ["Gate", "R9 fact"], [
        ["Runtime commit", facts["runtime_commit"]],
        ["PostgreSQL", "43 Candidate SQL suites plus real-chain and parallel transition proof PASS on 17.6 and 18.1."],
        ["JavaScript", "613 complete tests PASS; focused transition source contract PASS."],
        ["Worker builds", "Normal TEST, private Candidate and public Candidate dry builds PASS."],
        ["Candidate DB workflow", facts["candidate_db_run"]],
        ["Safe migration/install", facts["safe_migration_run"]],
        ["Installed repeatable SHA-256", facts["repeatable_sha256"]],
        ["TEST Worker versions", "normal " + facts["normal_worker_version"] + "; private " + facts["private_worker_version"] + "; public " + facts["public_worker_version"]],
        ["Safety", "All Candidate flags false; Candidate core/Daily/mail rows empty; production untouched."],
    ], [48 * mm, 115 * mm]))
    story.append(Spacer(1, 5 * mm))
    story.append(callout(sty, "Requested verdict: GO or one bounded evidence-backed NO-GO for the R9 authority-transition correction. A GO grants the outstanding Phase 2 authority verdict while retaining the existing Phase 1B transport GO, and permits the already-planned Phase 3 gate only. It does not activate Candidate Daily or complete the full Candidate App."))

    doc.build(story)


def merge(base_path: Path, addendum_path: Path, output_path: Path) -> tuple[int, int, int]:
    base = PdfReader(str(base_path))
    addendum = PdfReader(str(addendum_path))
    if len(base.pages) != 87:
        raise RuntimeError(f"Expected R8 base to contain 87 pages; found {len(base.pages)}")
    writer = PdfWriter()
    for page in base.pages:
        writer.add_page(page)
    for page in addendum.pages:
        writer.add_page(page)
    writer.add_metadata({
        "/Title": "CloudTMS Candidate App Current Decisions - Phase 2 and Phase 1B R9",
        "/Author": "Arthur Rai / CloudTMS",
        "/Subject": "Current decisions plus database-owned Daily authority-transition correction",
        "/Keywords": "CloudTMS, Candidate App, Daily, Phase 2, Phase 1B, R9, cutover, rollback",
    })
    with output_path.open("wb") as handle:
        writer.write(handle)
    return len(base.pages), len(addendum.pages), len(base.pages) + len(addendum.pages)


def verify(base_path: Path, output_path: Path) -> None:
    base = PdfReader(str(base_path))
    output = PdfReader(str(output_path))
    if len(output.pages) <= len(base.pages):
        raise RuntimeError("Combined PDF contains no R9 addendum")
    for index, page in enumerate(base.pages):
        if (page.extract_text() or "") != (output.pages[index].extract_text() or ""):
            raise RuntimeError(f"R8 base page text changed at page {index + 1}")
        if page.mediabox != output.pages[index].mediabox:
            raise RuntimeError(f"R8 base page dimensions changed at page {index + 1}")
    appended = re.sub(r"\s+", " ", "\n".join(
        (page.extract_text() or "") for page in output.pages[len(base.pages):]
    ))
    required = [
        "80. R8 finding and bounded R9 disposition",
        "81. Locked database proof and derived disposition",
        "82. Generation, cursor, overlay and rollback rules",
        "83. Replay, concurrency, cohorts and immutable evidence",
        "84. Verification, deployment and next gate",
        "AV-229-AV-232",
        "does not activate Candidate Daily",
    ]
    missing = [item for item in required if item not in appended]
    if missing:
        raise RuntimeError(f"Required R9 decisions missing: {missing}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--runtime-commit", required=True)
    parser.add_argument("--candidate-db-run", required=True)
    parser.add_argument("--safe-migration-run", required=True)
    parser.add_argument("--repeatable-sha256", required=True)
    parser.add_argument("--normal-worker-version", required=True)
    parser.add_argument("--private-worker-version", required=True)
    parser.add_argument("--public-worker-version", required=True)
    args = parser.parse_args()
    facts = {
        "runtime_commit": args.runtime_commit,
        "candidate_db_run": args.candidate_db_run,
        "safe_migration_run": args.safe_migration_run,
        "repeatable_sha256": args.repeatable_sha256,
        "normal_worker_version": args.normal_worker_version,
        "private_worker_version": args.private_worker_version,
        "public_worker_version": args.public_worker_version,
    }
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    addendum = output.with_name(output.stem + "_addendum_only.pdf")
    build_addendum(addendum, facts)
    base_pages, addendum_pages, total_pages = merge(args.base.resolve(), addendum, output)
    verify(args.base.resolve(), output)
    addendum.unlink()
    print(
        f"DECISIONS_R9_PASS|base_pages={base_pages}|addendum_pages={addendum_pages}|"
        f"total_pages={total_pages}|sha256={sha256(output)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
