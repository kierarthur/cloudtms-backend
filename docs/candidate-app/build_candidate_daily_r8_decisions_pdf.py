#!/usr/bin/env python3
"""Append the Candidate Daily Phase 2/1B R8 authority to the accepted R7 PDF."""

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
    canvas.drawRightString(width - 18 * mm, height - 11.2 * mm, "PHASE 2 + PHASE 1B R8 - 17 AUGUST 2026")
    canvas.setStrokeColor(GOLD)
    canvas.setLineWidth(1.1)
    canvas.line(18 * mm, 16 * mm, width - 18 * mm, 16 * mm)
    canvas.setFillColor(MUTED)
    canvas.setFont("Helvetica", 7.3)
    canvas.drawString(18 * mm, 10.5 * mm, "Sections 70-79 - installed Daily authority and broker-to-RPC integration")
    canvas.drawRightString(width - 18 * mm, 10.5 * mm, f"R8 addendum page {doc.page}")
    canvas.restoreState()


def section(story, sty, title):
    story.append(PageBreak())
    story.append(Paragraph(markup(title), sty["section"]))


def build_addendum(output: Path) -> None:
    sty = styles()
    doc = BaseDocTemplate(
        str(output), pagesize=A4, rightMargin=18 * mm, leftMargin=18 * mm,
        topMargin=25 * mm, bottomMargin=22 * mm,
        title="CloudTMS Candidate App Current Decisions - Phase 2 and Phase 1B R8",
        author="Arthur Rai / CloudTMS",
        subject="Sections 70-79: additive Daily database authority and broker integration",
    )
    frame = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height, id="main")
    doc.addPageTemplates([PageTemplate(id="r8", frames=[frame], onPage=page_canvas)])

    story = [Spacer(1, 10 * mm)]
    story.append(Paragraph("Current Decisions", sty["title"]))
    story.append(Paragraph("Candidate Daily Phase 2 and Phase 1B", sty["sub"]))
    story.append(Paragraph("Later-controlling R8 addendum - Sections 70-79", sty["sub"]))
    story.append(table(sty, ["Authority", "Current fact"], [
        ["Decision date", "17 August 2026"],
        ["Base authority", "Accepted R5 plus R6/R7; Sections 1-69 remain controlling unless this R8 addendum expressly records the implemented later phase."],
        ["R8 purpose", "Install the exact twelve-table/thirteen-RPC Phase 2 authority and deploy complete Phase 1B broker-to-RPC integration."],
        ["Product state", "Installed/deployed in TEST; all thirteen Candidate flags false; no entitlement, source, generation, availability, receipt or effect row."],
        ["Next gate", "Independent R8 review. A GO releases Phase 3 work only; it is not activation, full-app completion or production authority."],
    ], [42 * mm, 121 * mm]))
    story.append(Spacer(1, 6 * mm))
    story.append(callout(sty, "R8 establishes business authority without enabling it. The database and Workers are present, but Candidate Daily remains unavailable until the complete feature, entitlement, mode, generation, freshness, parity and rollout gates pass."))

    section(story, sty, "70. Phase disposition and full-product boundary")
    story.append(Paragraph("Phase 0 R5 and corrected Phase 1A R7 are accepted. R8 implements Phase 2 and Phase 1B as the next dependency-controlled package. The full Candidate application remains broader than Daily availability and retains all accepted authentication, session, submission, approval, QR/electronic, evidence, notification, Office and workflow authorities.", sty["body"]))
    story.append(table(sty, ["Phase", "R8 disposition"], [
        ["Phase 2", "Twelve-table/thirteen-RPC additive authority implemented and installed in TEST."],
        ["Phase 1B", "Candidate/system operations wired public broker -> private Worker -> installed RPCs; deployed disabled."],
        ["Phase 3", "Not started. Minimal Google server adapter and Master dual publication remain mandatory."],
        ["Phase 4", "Not started. Complete responsive web/iOS/Android Daily UI and shadow parity remain mandatory."],
        ["Phase 5", "Controlled TEST cutover not authorised; rollback, parity, soak and error budgets required."],
        ["Phase 6", "Full Emergency/specialist and DAILY signing/EMAIL/PHONE acceptance remains mandatory."],
        ["Phase 7", "Gradual rollout and separately authorised legacy-browser retirement remain mandatory."],
    ], [31 * mm, 132 * mm]))
    story.append(Spacer(1, 5 * mm))
    story.append(callout(sty, "A Phase 2/1B GO is not a claim that the app is complete and must never enable a Candidate flag, create an entitlement, edit Google, run a real effect or begin production rollout."))

    section(story, sty, "71. Exact additive database authority")
    story.append(table(sty, ["Table owner", "Purpose"], [
        ["authority_scopes / entitlements", "One mode/version fence and exact Candidate/cohort enablement."],
        ["source_links", "Trusted legacy-source digest to one canonical Candidate; no raw browser secret or arbitrary Candidate nomination."],
        ["command_receipts / batch_receipts", "Factual idempotency key, request hash and durable exact result."],
        ["rota_generations / rota_days", "Immutable versioned generation and complete fourteen-day day facts."],
        ["availability_days", "Canonical Candidate/day value bound to a generation."],
        ["sheet_projection_outbox / sync_state", "Lease/retry/park plus sole durable/effective cursor and freshness authority."],
        ["authority_transitions", "Immutable append-only cutover/rollback transition record."],
        ["external_effect_receipts", "Exact Emergency/specialist effect claim, lease, completion and status."],
    ], [53 * mm, 110 * mm]))
    story.append(Spacer(1, 5 * mm))
    bullets(story, sty, [
        "Exactly twelve additive tables; no table is a replacement for the seven accepted Candidate core business tables.",
        "Every Daily table has RLS and no direct anon/authenticated/service-role DML.",
        "Exactly thirteen service-role-only security-definer RPCs own all business access.",
        "The migration adds candidate_daily_enabled=false only; it creates no business row or entitlement.",
    ])

    section(story, sty, "72. Identity, receipts and exact replay")
    story.append(Paragraph("Candidate calls derive identity from the authenticated private Candidate session. Legacy calls resolve one approved environment/source identity through the private source-link catalogue. Neither request may nominate an arbitrary Candidate UUID.", sty["body"]))
    story.append(table(sty, ["Invariant", "Binding decision"], [
        ["Command key", "Caller-owned Idempotency-Key identifies one factual Candidate/legacy operation."],
        ["Batch key", "Generation publication has its own durable batch receipt and canonical request hash."],
        ["Exact replay", "Same key plus same factual request returns the stored database result; an internal replay marker never crosses publicly."],
        ["Conflict", "Same key plus changed Candidate/source/generation/date/value/action facts conflicts."],
        ["Transport nonce", "Always separate from database idempotency; legitimate retry uses a fresh HMAC nonce and the same business key."],
        ["Sensitive data", "No password, raw Candidate token, Apps Script secret or legacy browser session is stored in receipts/source links."],
    ], [43 * mm, 120 * mm]))

    section(story, sty, "73. Generation, availability and Candidate capability")
    story.append(callout(sty, "A Candidate Daily capability is true only when every server-owned prerequisite is true. Electronic/DAILY shape alone is never evidence of availability capability."))
    story.append(table(sty, ["Required proof", "Owner"], [
        ["Global switch", "settings_defaults candidate_daily_enabled"],
        ["Exact entitlement", "private.candidate_daily_entitlements"],
        ["Candidate identity", "authenticated Candidate session/account"],
        ["Authority mode", "private.candidate_daily_authority_scopes"],
        ["Complete current generation", "generation plus exact fourteen-day rota rows"],
        ["Freshness/cursor", "private.candidate_daily_sync_state"],
        ["Operation preconditions", "the exact read/write RPC"],
    ], [48 * mm, 115 * mm]))
    story.append(Spacer(1, 5 * mm))
    story.append(Paragraph("The existing bootstrap reads this same database helper and deep-preserves all accepted baseline fields. TEST has the flag false and the entitlement/source/generation/availability tables empty, so the capability is false.", sty["body"]))

    section(story, sty, "74. Projection, freshness and deferred overlay")
    story.append(table(sty, ["Rule", "Controlling behaviour"], [
        ["Single outbox", "All Sheet projection work is durable and claimable by one bounded lease owner."],
        ["Completion fence", "Only the exact claim lease may complete/retry/park the row."],
        ["Dual cursor", "One sync-state row owns durable cursor and effective-visible cursor; no browser/App Script memory owns either."],
        ["Deferred overlay", "Permitted only with exact current generation/hash proof."],
        ["Retreat", "Overlay removal/change retreats effective visibility and requeues eligible parked work."],
        ["Freshness", "Candidate reads fail closed on stale/missing generation/cursor state; signed-system status/projection remains independently authenticated."],
    ], [42 * mm, 121 * mm]))

    section(story, sty, "75. Mode transition, rollback and effect authority")
    story.append(Paragraph("The only authority modes are GOOGLE_PRIMARY, ROLLBACK_PENDING and SUPABASE_PRIMARY. One atomic RPC checks expected mode/version, generation, reconciliation and cursor/freshness barriers before appending the immutable transition record.", sty["body"]))
    story.append(table(sty, ["Boundary", "Decision"], [
        ["Direct flip", "Prohibited when the required transition/reconciliation proof is absent."],
        ["Rollback", "Explicit and auditable through ROLLBACK_PENDING; receipts/history are not deleted."],
        ["Effect claim", "One factual effect key/request hash and bounded lease owner."],
        ["Effect complete", "Exact lease/factual identity owns terminal result or retry state."],
        ["Lost response", "Status reads the same durable result; another key is not invented."],
        ["Concrete provider", "Fails typed unavailable until Phase 3/6 adapter exists; R8 runs no real effect."],
    ], [42 * mm, 121 * mm]))

    section(story, sty, "76. Phase 1B private and public integration")
    story.append(table(sty, ["Layer", "Sole responsibility"], [
        ["Public broker", "Origin/client/rate/access-token/body/schema boundary; rebuilds strict allowlisted public responses."],
        ["Private Worker", "Service authentication, Candidate session, Google HMAC/nonce and closed RPC composition."],
        ["Database", "Canonical identity, policy, generation, availability, receipt, cursor, transition and effect truth."],
        ["Specialist seam", "Typed dependency interface only; never a test-only fake or invented success in production."],
    ], [42 * mm, 121 * mm]))
    story.append(Spacer(1, 5 * mm))
    bullets(story, sty, [
        "All accepted Daily operations map to an installed RPC or one explicitly later-gated specialist seam.",
        "Public responses never expose receipt hashes, source mappings, lease tokens, database errors or internal replay markers.",
        "Signed-system continuity does not consult the Candidate product switch.",
        "Candidate Daily reads/writes consult the complete database-owned capability conjunction.",
    ])

    section(story, sty, "77. Minimal legacy change and continuing Google services")
    story.append(callout(sty, "Do not modernise the temporary legacy app. Contain it. Preserve its browser UI, login and msisdn lookup; strengthen only the later server-to-server Apps Script -> CloudTMS boundary.", PALE_GOLD, GOLD))
    story.append(Spacer(1, 5 * mm))
    story.append(table(sty, ["Component", "Lifecycle decision"], [
        ["Legacy browser", "Unchanged through coexistence; receives no CloudTMS token/HMAC secret/Supabase access/Candidate UUID selector."],
        ["Availability Apps Script", "Phase 3 adds the smallest signed compatibility/projection/effect adapter while retaining existing response behaviour."],
        ["NEW MASTER ROTA", "Active current Head is web v101. Continue Availability publication and later add signed CloudTMS generation publication."],
        ["Availability/Emergency", "Continue during and after legacy-browser retirement until separately migrated and accepted."],
        ["Specialists", "Must work through both clients during coexistence with one CloudTMS receipt/effect authority."],
    ], [44 * mm, 119 * mm]))

    section(story, sty, "78. Verification, deployment and safety")
    story.append(table(sty, ["Gate", "R8 fact"], [
        ["PostgreSQL", "42 Candidate suites PASS on 17.6 and 18.1."],
        ["JavaScript", "605 complete and 35 focused tests PASS."],
        ["OpenAPI/builds", "62-path OpenAPI PASS; both Candidate Worker dry builds PASS."],
        ["Installed TEST", "12 tables, 13 RPCs, exact ledgers/ACLs; 0/13 flags enabled; all Candidate/Daily rows zero."],
        ["Private Worker", "Version 689bbe95-bf31-4f91-8e5a-40289558cefa at 100%."],
        ["Public broker", "Version 18f67f8e-3ca2-46ad-9599-8512894de6c3 at 100%; health/readiness 200/200."],
        ["Safe migration", "Candidate install succeeded; broad run then stopped only on three declared pre-existing James definition hashes."],
        ["No change", "No Google/frontend/normal Worker/finance/Banking Pay/production change; no Candidate data/effect/communication."],
    ], [43 * mm, 120 * mm]))

    section(story, sty, "79. Decision register AV-193-AV-228 and next gate")
    story.append(table(sty, ["IDs", "Decision family", "Disposition"], [
        ["AV-193-AV-200", "Exact tables/RPCs/ACL/flag/system continuity/Candidate capability/bootstrap", "Implemented and installed; product disabled"],
        ["AV-201-AV-208", "Legacy source mapping, no browser authority, generation and exact receipts", "Implemented; no real source/data"],
        ["AV-209-AV-216", "Outbox/lease/cursors/overlay/modes/rollback/effect receipt", "Implemented and PG17/18 verified"],
        ["AV-217-AV-222", "Typed specialist seam, Phase 1B mapping, session identity, response reconstruction, replay separation, rates", "Implemented; specialist execution later-gated"],
        ["AV-223-AV-228", "Disabled/empty TEST, no Google change, Master lifecycle, full-app remaining phases and no unrelated drift", "Proved/preserved"],
    ], [30 * mm, 91 * mm, 42 * mm]))
    story.append(Spacer(1, 5 * mm))
    story.append(callout(sty, "Current verdict requested: GO or bounded NO-GO for Phase 2 and Phase 1B R8. A GO releases Phase 3 implementation only. It does not enable Candidate Daily, complete the Candidate UI, authorise real effects, deploy production or retire the legacy browser."))

    doc.build(story)


def merge(base_path: Path, addendum_path: Path, output_path: Path) -> tuple[int, int, int]:
    base = PdfReader(str(base_path))
    addendum = PdfReader(str(addendum_path))
    if len(base.pages) != 76:
        raise RuntimeError(f"Expected accepted R7 base to contain 76 pages; found {len(base.pages)}")
    writer = PdfWriter()
    for page in base.pages:
        writer.add_page(page)
    for page in addendum.pages:
        writer.add_page(page)
    writer.add_metadata({
        "/Title": "CloudTMS Candidate App Current Decisions - Phase 2 and Phase 1B R8",
        "/Author": "Arthur Rai / CloudTMS",
        "/Subject": "Accepted decisions plus installed Daily database/RPC and broker integration",
        "/Keywords": "CloudTMS, Candidate App, Daily, Availability, Phase 2, Phase 1B, R8",
    })
    with output_path.open("wb") as handle:
        writer.write(handle)
    return len(base.pages), len(addendum.pages), len(base.pages) + len(addendum.pages)


def verify(base_path: Path, output_path: Path) -> None:
    base = PdfReader(str(base_path))
    output = PdfReader(str(output_path))
    if len(output.pages) <= len(base.pages):
        raise RuntimeError("Combined PDF contains no R8 addendum")
    for index, page in enumerate(base.pages):
        if (page.extract_text() or "") != (output.pages[index].extract_text() or ""):
            raise RuntimeError(f"Accepted R7 page text changed at page {index + 1}")
        if page.mediabox != output.pages[index].mediabox:
            raise RuntimeError(f"Accepted R7 page dimensions changed at page {index + 1}")
    appended = re.sub(r"\s+", " ", "\n".join((page.extract_text() or "") for page in output.pages[len(base.pages):]))
    required = [
        "70. Phase disposition and full-product boundary",
        "71. Exact additive database authority",
        "72. Identity, receipts and exact replay",
        "73. Generation, availability and Candidate capability",
        "74. Projection, freshness and deferred overlay",
        "75. Mode transition, rollback and effect authority",
        "76. Phase 1B private and public integration",
        "77. Minimal legacy change and continuing Google services",
        "78. Verification, deployment and safety",
        "79. Decision register AV-193-AV-228 and next gate",
        "candidate_daily_enabled",
        "does not enable Candidate Daily",
    ]
    missing = [item for item in required if item not in appended]
    if missing:
        raise RuntimeError(f"Required R8 decisions missing: {missing}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    base = args.base.resolve()
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    addendum = output.with_name(output.stem + "_addendum_only.pdf")
    build_addendum(addendum)
    base_pages, addendum_pages, total_pages = merge(base, addendum, output)
    verify(base, output)
    addendum.unlink()
    print(f"DECISIONS_R8_PASS|base_pages={base_pages}|addendum_pages={addendum_pages}|total_pages={total_pages}|sha256={sha256(output)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
