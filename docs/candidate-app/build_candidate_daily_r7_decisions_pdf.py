#!/usr/bin/env python3
"""Append the later-controlling Candidate Daily R7 correction to the accepted R6 PDF."""

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
    BLUE, GOLD, INK, MUTED, NAVY, PALE, PALE_GOLD,
    bullets, callout, markup, styles, table,
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
    canvas.drawRightString(width - 18 * mm, height - 11.2 * mm, "PHASE 1A R7 CORRECTION - 16 AUGUST 2026")
    canvas.setStrokeColor(GOLD)
    canvas.setLineWidth(1.1)
    canvas.line(18 * mm, 16 * mm, width - 18 * mm, 16 * mm)
    canvas.setFillColor(MUTED)
    canvas.setFont("Helvetica", 7.3)
    canvas.drawString(18 * mm, 10.5 * mm, "Sections 65-69 - R6 transport correction and coexistence clarification")
    canvas.drawRightString(width - 18 * mm, 10.5 * mm, f"R7 addendum page {doc.page}")
    canvas.restoreState()


def section(story, sty, title):
    story.append(PageBreak())
    story.append(Paragraph(markup(title), sty["section"]))


def build_addendum(output: Path) -> None:
    sty = styles()
    doc = BaseDocTemplate(
        str(output), pagesize=A4, rightMargin=18 * mm, leftMargin=18 * mm,
        topMargin=25 * mm, bottomMargin=22 * mm,
        title="CloudTMS Candidate App Current Decisions - Phase 1A R7 Correction",
        author="Arthur Rai / CloudTMS",
        subject="Sections 65-69: R6 independent-audit corrections and coexistence clarification",
    )
    frame = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height, id="main")
    doc.addPageTemplates([PageTemplate(id="r7", frames=[frame], onPage=page_canvas)])

    story = [Spacer(1, 11 * mm)]
    story.append(Paragraph("Current Decisions", sty["title"]))
    story.append(Paragraph("Candidate Daily Phase 1A correction", sty["sub"]))
    story.append(Paragraph("Later-controlling R7 addendum - Sections 65-69", sty["sub"]))
    story.append(table(sty, ["Authority", "Current fact"], [
        ["Decision date", "16 August 2026"],
        ["Base authority", "Accepted 61-page R5 plus the 9-page R6 addendum; Sections 1-64 remain controlling except where R7 expressly supersedes an operational fact."],
        ["R7 purpose", "Close all nine bounded findings from the independent R6 audit without beginning Phase 2."],
        ["Product state", "Candidate Daily remains globally disabled, database-dark, Google-unchanged and effect-free."],
        ["Next gate", "Independent R7 review; only a GO may release Phase 2 additive SQL/RPC authoring."],
    ], [42 * mm, 121 * mm]))
    story.append(Spacer(1, 6 * mm))
    story.append(callout(sty,
        "R7 is a correction, not an expansion. It changes the public failure boundary, pre-auth system throttling, nonce retention, framing and canonicalisation proofs. It adds no Daily table/RPC, Google adapter, Candidate UI, entitlement or business effect."))

    section(story, sty, "65. Independent R6 findings and closure")
    story.append(Paragraph(
        "The independent R6 audit issued a bounded NO-GO for Phase 1A source. It accepted the 24-route catalogue, additive bootstrap success, access topology, HMAC verification order, body/rate/deadline metadata and dark/no-mutation boundary, but identified nine exact transport defects. R7 retains the accepted design and closes each defect directly.", sty["body"]))
    story.append(table(sty, ["Finding", "R7 controlling correction"], [
        ["R6-ERR-001", "Every Daily/bootstrap error has exactly ok, error_code, correlation_id, retry_class and required safe message; details is absent or one typed closed variant."],
        ["R6-RESP-001", "The public broker validates route/status/code/retry/correlation/schema and rebuilds the body; any private drift becomes one conforming generic dependency error."],
        ["R6-ORIGIN-001", "Disallowed Origin, client and preflight/header policy failures remain public 403 FORBIDDEN / DO_NOT_RETRY."],
        ["R6-RATE-001", "Signed-system pre-auth traffic consumes both a source-IP bucket and a trusted-key/shared-invalid-key bucket before private HMAC work."],
        ["R6-NONCE-001", "Ten-minute replay retention starts at successful server consumption, never at the caller-signed timestamp."],
        ["R6-CORR-001", "A valid signed correlation is preserved; missing/invalid input is rejected at the public pre-auth edge with a newly generated valid response ULID."],
        ["R6-FRAME-001", "When Content-Length is supplied, Candidate and system paths require exact equality with actual bytes."],
        ["R6-HMAC-QUERY-001", "Percent-encoded query names/values use explicit ASCII/code-unit tuple ordering in JavaScript and Python."],
        ["R6-RAW-001", "The deployed contract is stated at the Fetch-observable header boundary; raw pre-normalisation properties are never claimed without a platform-level HTTP probe."],
    ], [33 * mm, 130 * mm]))

    section(story, sty, "66. Closed response and transport authority")
    story.append(Paragraph(
        "The merged R5 OpenAPI remains the sole API authority. R7 does not edit its route or error matrices. Runtime tests load the frozen 25-operation matrix and compare every permitted status/error/retry triple exactly, including bootstrap.", sty["body"]))
    story.append(table(sty, ["Boundary", "Required behaviour"], [
        ["Daily error", "additionalProperties=false; fixed public message; correlation is a valid ULID; no untyped unavailable_reason object."],
        ["Public response", "Never forwards private JSON bytes. It reconstructs allowlisted public fields and safe headers only."],
        ["Nonconforming private body", "Returns CANDIDATE_DAILY_NOT_READY/STATUS_CHECK on Candidate routes or DEPENDENCY_UNAVAILABLE/RETRY_AFTER on bootstrap/system routes."],
        ["Phase 1A success", "No new Daily business success is possible. A wrong or unexpected success schema fails closed to the generic dependency result."],
        ["403 policy", "Origin, native-client and preflight/header rejection stay 403 FORBIDDEN / DO_NOT_RETRY, never 500 retryable."],
        ["Framing", "Candidate maximum remains 32 KiB; system maximum remains 256 KiB; supplied declared length must equal bytes read."],
    ], [44 * mm, 119 * mm]))
    story.append(Spacer(1, 5 * mm))
    story.append(callout(sty,
        "Private implementation data cannot cross the public broker merely because it is valid JSON. Internal stack, database, token, storage, diagnostic and unknown future fields all cause safe generic reconstruction."))

    section(story, sty, "67. Rate, nonce, HMAC and platform boundary")
    story.append(table(sty, ["Invariant", "R7 decision"], [
        ["Pre-auth rate", "One IP bucket always applies. An accepted PRIMARY/OVERLAP key ID may have its own second bucket; every unverified/unknown ID shares one invalid-key bucket. Rotating attacker labels cannot multiply private work."],
        ["Nonce lifetime", "R2 uploaded time or the stored server consumption epoch owns age. At 599 seconds it remains; at 600 seconds it is eligible for cleanup, including requests accepted at both +/-300-second clock edges."],
        ["Correlation", "The HMAC signs a valid supplied ULID. The public edge does not alter it. Missing/invalid signed input never reaches private and the error envelope still carries a valid generated ULID."],
        ["Query ordering", "Canonical percent-encoded strings are sorted by explicit ASCII/code units, then value; locale collation is prohibited."],
        ["Raw headers", "Workers can enforce only the Fetch Request/Headers representation they receive. Duplicate lines may already be combined and outer whitespace may be normalised. The verifier rejects the resulting ambiguous/invalid observable value; direct raw-pair vectors remain reference-parser evidence, not a claim that wire bytes survive Cloudflare unchanged."],
    ], [42 * mm, 121 * mm]))
    story.append(Spacer(1, 4 * mm))
    bullets(story, sty, [
        "No public HMAC secret or nonce store is introduced.",
        "No unknown key ID receives an independent throttle bucket.",
        "The R5 signed prefix/body algorithm remains unchanged; only deterministic query ordering and replay-age ownership are corrected.",
        "Future routes with a non-empty query must use the same corrected Node/Python vectors before activation.",
    ])

    section(story, sty, "68. Google coexistence and decommission boundary")
    story.append(Paragraph(
        "The user deployed NEW MASTER ROTA current Head as active web-app version 101 on 16 August 2026. That later operational fact supersedes the R6 evidence statement that active version 100 differed from Head. The current Head hash and certified-source relationship remain the recorded authority; Phase 3 must recheck them immediately before any Google edit.", sty["body"]))
    story.append(callout(sty,
        "Decommissioning the temporary legacy Availability browser and its LEGACY_COMPAT facade does not decommission the Availability Sheet/Apps Script service, Emergency functions, Master Rota publication, signed system synchronisation, projection/freshness routes or effect authority.", PALE_GOLD, GOLD))
    story.append(Spacer(1, 5 * mm))
    story.append(table(sty, ["Component", "Lifecycle decision"], [
        ["Legacy browser/UI/login", "Preserve unchanged during coexistence; remove only after the new Candidate app is proven and separately approved."],
        ["LEGACY_COMPAT facade", "Temporary server-side translation for the old browser. It may be removed with the old browser after accepted cutover."],
        ["Availability Sheet/Apps Script", "Continues after browser retirement until its emergency/specialist responsibilities are separately migrated and accepted."],
        ["NEW MASTER ROTA", "Continues publishing generation/working truth required by Availability and specialist flows; it sends to the signed CloudTMS system boundary as Phase 3 settles."],
        ["Emergency/specialist flows", "Must work with both old and new clients during coexistence and retain one CloudTMS receipt/effect authority. No flow is silently retired with the old browser."],
        ["Final specialist retirement", "A separate later decision, migration and acceptance gate; never inferred from Candidate app launch."],
    ], [44 * mm, 119 * mm]))

    section(story, sty, "69. Decision register AV-181-AV-192 and remaining gate")
    story.append(table(sty, ["IDs", "Decision family", "Current disposition"], [
        ["AV-181-AV-183", "Closed Daily/bootstrap errors, public response rebuilding and exact 403 policy", "Implemented; adversarially tested"],
        ["AV-184-AV-188", "Pre-auth throttling, consumption-age nonce, correlation, exact framing, ASCII query and honest Fetch boundary", "Implemented; Node/Python/platform tests"],
        ["AV-189", "Master Rota active web deployment v101 now represents current Head", "User-deployed operational fact; future edit recheck required"],
        ["AV-190", "Legacy browser retirement is separate from continuing Availability/Emergency/Master/system services", "Binding coexistence/decommission rule"],
        ["AV-191", "Any R7 TEST deployment remains dark; flags false; no SQL, Google, UI or external effect", "Required safety boundary"],
        ["AV-192", "Phase 2 remains the next phase only after independent R7 GO", "Not started / separately gated"],
    ], [30 * mm, 92 * mm, 41 * mm]))
    story.append(Spacer(1, 5 * mm))
    story.append(callout(sty,
        "Current verdict requested: GO or bounded NO-GO for corrected Phase 1A only. Do not infer Phase 2 SQL approval, Google edit/deploy approval, Candidate UI completion, feature enablement, production authority or legacy/specialist decommissioning."))

    doc.build(story)


def merge(base_path: Path, addendum_path: Path, output_path: Path) -> tuple[int, int, int]:
    base = PdfReader(str(base_path))
    addendum = PdfReader(str(addendum_path))
    if len(base.pages) != 70:
        raise RuntimeError(f"Expected accepted R6 base to contain 70 pages; found {len(base.pages)}")
    writer = PdfWriter()
    for page in base.pages:
        writer.add_page(page)
    for page in addendum.pages:
        writer.add_page(page)
    writer.add_metadata({
        "/Title": "CloudTMS Candidate App Current Decisions - Phase 1A R7",
        "/Author": "Arthur Rai / CloudTMS",
        "/Subject": "Accepted R5/R6 decisions plus bounded Phase 1A transport correction",
        "/Keywords": "CloudTMS, Candidate App, Daily, Availability, HMAC, Phase 1A, R7",
    })
    with output_path.open("wb") as handle:
        writer.write(handle)
    return len(base.pages), len(addendum.pages), len(base.pages) + len(addendum.pages)


def verify(base_path: Path, output_path: Path) -> None:
    base = PdfReader(str(base_path))
    output = PdfReader(str(output_path))
    if len(output.pages) <= len(base.pages):
        raise RuntimeError("Combined PDF contains no R7 addendum")
    for index, page in enumerate(base.pages):
        if (page.extract_text() or "") != (output.pages[index].extract_text() or ""):
            raise RuntimeError(f"Accepted R6 page text changed at page {index + 1}")
        if page.mediabox != output.pages[index].mediabox:
            raise RuntimeError(f"Accepted R6 page dimensions changed at page {index + 1}")
    appended = re.sub(r"\s+", " ", "\n".join(
        (page.extract_text() or "") for page in output.pages[len(base.pages):]
    ))
    required = [
        "65. Independent R6 findings and closure",
        "66. Closed response and transport authority",
        "67. Rate, nonce, HMAC and platform boundary",
        "68. Google coexistence and decommission boundary",
        "69. Decision register AV-181-AV-192 and remaining gate",
        "version 101",
        "599 seconds",
        "invalid-key bucket",
        "does not decommission",
    ]
    missing = [item for item in required if item not in appended]
    if missing:
        raise RuntimeError(f"Required R7 decisions missing from combined PDF: {missing}")


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
    print(
        f"DECISIONS_R7_PASS|base_pages={base_pages}|addendum_pages={addendum_pages}"
        f"|total_pages={total_pages}|sha256={sha256(output)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
