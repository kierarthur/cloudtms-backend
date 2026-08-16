#!/usr/bin/env python3
"""Append the later-controlling Candidate Daily R6 addendum to the accepted R5 PDF."""

from __future__ import annotations

import argparse
import hashlib
import re
from html import escape
from pathlib import Path

from pypdf import PdfReader, PdfWriter
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    BaseDocTemplate,
    Frame,
    KeepTogether,
    PageBreak,
    PageTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)


NAVY = colors.HexColor("#17324D")
BLUE = colors.HexColor("#286D9B")
PALE = colors.HexColor("#EAF2F8")
PALE_GOLD = colors.HexColor("#F7F0DF")
GOLD = colors.HexColor("#C69C3D")
INK = colors.HexColor("#1E2933")
MUTED = colors.HexColor("#5B6770")
GREEN = colors.HexColor("#166B52")
RED = colors.HexColor("#9C2F2F")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def markup(text: str) -> str:
    value = escape(text.strip())
    value = re.sub(r"`([^`]+)`", r'<font name="Courier">\1</font>', value)
    value = re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", value)
    return value


def styles():
    base = getSampleStyleSheet()
    return {
        "title": ParagraphStyle(
            "R6Title", parent=base["Title"], fontName="Helvetica-Bold", fontSize=23,
            leading=28, textColor=NAVY, alignment=TA_LEFT, spaceAfter=6 * mm,
        ),
        "sub": ParagraphStyle(
            "R6Sub", parent=base["Normal"], fontName="Helvetica", fontSize=11.5,
            leading=16, textColor=MUTED, spaceAfter=5 * mm,
        ),
        "section": ParagraphStyle(
            "R6Section", parent=base["Heading1"], fontName="Helvetica-Bold", fontSize=17,
            leading=21, textColor=NAVY, spaceAfter=4.5 * mm, keepWithNext=True,
        ),
        "heading": ParagraphStyle(
            "R6Heading", parent=base["Heading2"], fontName="Helvetica-Bold", fontSize=11.5,
            leading=15, textColor=BLUE, spaceBefore=2 * mm, spaceAfter=2.5 * mm, keepWithNext=True,
        ),
        "body": ParagraphStyle(
            "R6Body", parent=base["BodyText"], fontName="Helvetica", fontSize=9.1,
            leading=13.2, textColor=INK, spaceAfter=3 * mm, splitLongWords=True,
        ),
        "small": ParagraphStyle(
            "R6Small", parent=base["BodyText"], fontName="Helvetica", fontSize=7.8,
            leading=10.8, textColor=MUTED, spaceAfter=1.7 * mm, splitLongWords=True,
        ),
        "bullet": ParagraphStyle(
            "R6Bullet", parent=base["BodyText"], fontName="Helvetica", fontSize=8.8,
            leading=12.8, textColor=INK, leftIndent=5.5 * mm, firstLineIndent=-3.2 * mm,
            bulletIndent=0, spaceAfter=2.1 * mm, splitLongWords=True,
        ),
        "callout": ParagraphStyle(
            "R6Callout", parent=base["BodyText"], fontName="Helvetica-Bold", fontSize=9.8,
            leading=14, textColor=NAVY,
        ),
        "table": ParagraphStyle(
            "R6Table", parent=base["BodyText"], fontName="Helvetica", fontSize=7.4,
            leading=9.8, textColor=INK, splitLongWords=True,
        ),
        "table_head": ParagraphStyle(
            "R6TableHead", parent=base["BodyText"], fontName="Helvetica-Bold", fontSize=7.5,
            leading=9.8, textColor=colors.white,
        ),
    }


def page_canvas(canvas, doc):
    canvas.saveState()
    width, height = A4
    canvas.setFillColor(NAVY)
    canvas.rect(0, height - 18 * mm, width, 18 * mm, fill=1, stroke=0)
    canvas.setFillColor(colors.white)
    canvas.setFont("Helvetica-Bold", 8)
    canvas.drawString(18 * mm, height - 11.2 * mm, "CLOUDTMS CANDIDATE APP — CURRENT DECISIONS")
    canvas.setFont("Helvetica", 7.5)
    canvas.drawRightString(width - 18 * mm, height - 11.2 * mm, "PHASE 1A R6 ADDENDUM • 16 AUGUST 2026")
    canvas.setStrokeColor(GOLD)
    canvas.setLineWidth(1.1)
    canvas.line(18 * mm, 16 * mm, width - 18 * mm, 16 * mm)
    canvas.setFillColor(MUTED)
    canvas.setFont("Helvetica", 7.3)
    canvas.drawString(18 * mm, 10.5 * mm, "Sections 57–64 • Google evidence gate and dark broker implementation")
    canvas.drawRightString(width - 18 * mm, 10.5 * mm, f"R6 addendum page {doc.page}")
    canvas.restoreState()


def table(sty, headers, rows, widths, font_size="table"):
    data = [[Paragraph(markup(value), sty["table_head"]) for value in headers]]
    for row in rows:
        data.append([Paragraph(markup(str(value)), sty[font_size]) for value in row])
    result = Table(data, colWidths=widths, repeatRows=1, hAlign="LEFT")
    result.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#C7D4DE")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, PALE]),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    return result


def callout(sty, text, background=PALE, border=BLUE):
    result = Table([[Paragraph(markup(text), sty["callout"])]], colWidths=[163 * mm])
    result.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), background),
        ("BOX", (0, 0), (-1, -1), 0.8, border),
        ("LEFTPADDING", (0, 0), (-1, -1), 9),
        ("RIGHTPADDING", (0, 0), (-1, -1), 9),
        ("TOPPADDING", (0, 0), (-1, -1), 9),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 9),
    ]))
    return result


def bullets(story, sty, items):
    for item in items:
        story.append(Paragraph(markup(item), sty["bullet"], bulletText="•"))


def section(story, sty, title):
    story.append(PageBreak())
    story.append(Paragraph(markup(title), sty["section"]))


def build_addendum(output: Path) -> None:
    sty = styles()
    doc = BaseDocTemplate(
        str(output), pagesize=A4, rightMargin=18 * mm, leftMargin=18 * mm,
        topMargin=25 * mm, bottomMargin=22 * mm,
        title="CloudTMS Candidate App Current Decisions — Phase 1A R6 Addendum",
        author="Arthur Rai / CloudTMS",
        subject="Sections 57–64: Google evidence gate and dark Candidate Daily contracts",
    )
    frame = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height, id="main")
    doc.addPageTemplates([PageTemplate(id="r6", frames=[frame], onPage=page_canvas)])

    story = [Spacer(1, 11 * mm)]
    story.append(Paragraph("Current Decisions", sty["title"]))
    story.append(Paragraph("Candidate Daily availability integration", sty["sub"]))
    story.append(Paragraph("Later-controlling Phase 1A R6 addendum • Sections 57–64", sty["sub"]))
    story.append(table(sty, ["Authority", "Current fact"], [
        ["Decision date", "16 August 2026"],
        ["Base authority", "Accepted 61-page Phase 0 R5 Decisions PDF; AV-001–AV-154 remain controlling"],
        ["R6 additions", "Google Evidence Gate complete; AV-155–AV-180; Phase 1A source implemented dark"],
        ["Runtime status", "Local source/tests only; no Worker deployment, SQL installation, Google change or feature enablement"],
        ["Next gate", "Independent R6 review before Phase 2 additive SQL/RPC authoring"],
    ], [42 * mm, 121 * mm]))
    story.append(Spacer(1, 6 * mm))
    story.append(callout(sty,
        "Precedence: Sections 57–64 add evidence and Phase 1A implementation decisions. They do not weaken or replace AV-001–AV-154. If a historical status statement says Phase 1A is not yet authorised, the accepted independent R5 GO plus this R6 implementation record supersede only that status; all substantive R5 constraints remain intact."))
    story.append(Spacer(1, 5 * mm))
    story.append(Paragraph(
        "The first 61 pages are the accepted R5 Decisions document reproduced unchanged in the combined PDF. This addendum begins on the next page and is the current authority for the effective Google evidence and Phase 1A source state.", sty["body"]))

    section(story, sty, "57. Google Evidence Gate")
    story.append(Paragraph(
        "The authorised read-only Google Evidence Gate is complete for the Availability API and NEW MASTER ROTA System. No Sheet data, Apps Script file, manifest, property, trigger, deployment or OAuth configuration was changed; no function was executed. Property values and unredacted source remain excluded from audit distribution.", sty["body"]))
    story.append(table(sty, ["System", "Effective source/deployment evidence"], [
        ["Availability API", "Sheet 1BSom…; current Head SHA-256 eacd1875…bbb3b3f; exact certified-source match; active web version 215 matches Head; 0 installed triggers; 50 property names inventoried without values."],
        ["NEW MASTER ROTA", "Sheet 1eEnr…; current Head SHA-256 c3ae9c48…19fa0a8; exact certified-source match; active web version 100 SHA-256 f41dad2e…682099d differs from Head; 12 trigger entries; 28 property names inventoried without values."],
    ], [43 * mm, 120 * mm]))
    story.append(Spacer(1, 4 * mm))
    bullets(story, sty, [
        "Effective project file order and duplicate-function ownership are known and must be rechecked immediately before a later Google edit.",
        "Master trigger execution uses current Head while the deployed web version is older; Phase 3 must distinguish trigger-source and web-deployment authority.",
        "`ai_startDailyPings` has no declaration or installed trigger. Its one historical reference is not revived or repaired by Candidate Daily.",
        "The Google projects do much more than Candidate Daily; unrelated functions remain outside scope and untouched.",
    ])
    story.append(callout(sty,
        "Binding legacy rule: Do not modernise the legacy app. Contain it. Preserve the old browser/UI/login and internal msisdn behaviour; strengthen only the later trusted server-to-server Apps Script-to-CloudTMS boundary.", PALE_GOLD, GOLD))

    section(story, sty, "58. Phase 1A source authority")
    story.append(Paragraph(
        "Phase 1A implements the frozen public/private transport and policy contracts from the sole merged R5 OpenAPI. It does not implement the Phase 2 database/RPC authority or any Phase 3 Google adapter. The exact baseline is cloudtms-backend origin/test commit 5386d3d2504d86a0366d66f4096a2f7a8912b2e9.", sty["body"]))
    story.append(table(sty, ["Source owner", "Bounded responsibility"], [
        ["candidate-daily-contract-v1.js", "Closed 24-route catalogue; policy, limits, correlation, idempotency, capability and error authority."],
        ["candidate-daily-hmac-v1.js", "Raw-target canonicalisation; HMAC v1; key overlap; private R2 nonce owner and cleanup."],
        ["candidate-daily-phase1a.js", "Additive disabled bootstrap and effect-free dark dispatch."],
        ["candidate-app-backend.js", "Authenticated Candidate Daily dispatch plus additive bootstrap only."],
        ["candidate-private-worker.js", "Private signed-system prefix, service auth, HMAC verification and nonce cleanup."],
        ["candidate-broker.js", "Credential-free signed-system forwarding; Candidate transport/rate/deadline boundary; stable safe responses."],
        ["candidate-broker/wrangler.jsonc", "Four TEST rate-limit bindings; no HMAC secret value."],
    ], [54 * mm, 109 * mm]))
    story.append(Spacer(1, 4 * mm))
    story.append(callout(sty,
        "Dark means safe: all Candidate Daily calls are globally disabled, signed system calls cannot reach business work, and no Daily table/RPC or Google executor exists. A successful HMAC check is not product enablement."))

    section(story, sty, "59. Routes, policies and limits")
    story.append(Paragraph(
        "The runtime catalogue matches all 24 new merged-R5 operations exactly: 11 Candidate routes and 13 trusted Google-system routes. Existing bootstrap is the additive 25th Daily-related merged operation.", sty["body"]))
    story.append(table(sty, ["Route family", "Policy/class", "Count", "Bound"], [
        ["Candidate Daily reads", "CANDIDATE_SURFACE / CANDIDATE_DAILY_READ", "7", "60/min/Candidate; 6 in flight; 32 KiB; 12 s"],
        ["Candidate Daily commands", "CANDIDATE_SURFACE / CANDIDATE_DAILY_COMMAND", "4", "12/min/Candidate; effects 6/min; 1 operation/effect; 32 KiB; 10/20 s"],
        ["Legacy compatibility", "LEGACY_COMPAT_READ/COMMAND", "4", "Signed system transport; 120/min/key; 8 in flight; 256 KiB"],
        ["Signed Google sync", "SIGNED_SYSTEM_READ/COMMAND", "9", "Signed system transport; 120/min/key; 8 in flight; 256 KiB; 10/12/20 s"],
    ], [43 * mm, 55 * mm, 14 * mm, 51 * mm]))
    story.append(Spacer(1, 4 * mm))
    story.append(Paragraph("The four policy authorities are fixed:", sty["heading"]))
    bullets(story, sty, [
        "BASELINE_BOOTSTRAP preserves the accepted Candidate bootstrap.",
        "CANDIDATE_SURFACE requires readable inputs, global exact true, explicit entitlement, source identity readiness and authority readiness.",
        "LEGACY_COMPAT is independent of the Candidate global flag and requires signed transport, nonce, trusted environment, stable operation identity, approved mapping, compatible mode and transition readiness.",
        "SIGNED_SYSTEM_SYNC is also independent of the Candidate global flag and requires signed transport, nonce, trusted environment, source-scope readiness, compatible mode and transition readiness.",
    ])
    story.append(Paragraph(
        "Distributed in-flight enforcement is not simulated with unsafe Worker-isolate counters. The cardinalities are frozen in the route authority; Phase 2 receipts/leases and Phase 1B integration must enforce them before any route can be activated.", sty["body"]))

    section(story, sty, "60. HMAC, nonce, correlation and replay")
    story.append(Paragraph(
        "The public broker has no Google-system secret and no replay store. It bounds and forwards the exact signed bytes through the existing private service-authenticated binding. The private Worker alone verifies HMAC and atomically consumes the nonce.", sty["body"]))
    story.append(table(sty, ["Invariant", "R6 decision"], [
        ["Signed bytes", "Method, normalised path/query, timestamp, nonce, body SHA-256, Idempotency-Key, correlation ULID, blank separator and exact raw body."],
        ["Key rotation", "One PRIMARY and one optional OVERLAP key slot; unknown/misconfigured IDs fail generically."],
        ["Clock", "Private-server ±300 seconds."],
        ["Framing", "Exact Content-Length, UTF-8 JSON content type, no transfer/content encoding, no BOM, strict path/query/header grammar."],
        ["Nonce", "Atomic create-if-absent under candidate-daily-google-nonces/v1/{environment}/{key_id}/{timestamp}/{nonce}; retained at least ten minutes."],
        ["Business retry", "Fresh nonce/signature; same caller Idempotency-Key. Phase 2 receipt owns exact business replay."],
        ["Correlation", "Candidate value may be generated before processing; signed system value is mandatory, signed and never replaced."],
    ], [43 * mm, 120 * mm]))
    story.append(Spacer(1, 4 * mm))
    story.append(callout(sty,
        "No raw password, token, Candidate identity, Script Property value, HMAC secret or Google payload is written into the nonce record. Only bounded creation/expiry metadata and the signed-message digest are retained."))

    section(story, sty, "61. Legacy, Master Rota and Emergency coexistence")
    story.append(Paragraph(
        "The temporary legacy system must remain fully functional until the new Candidate App is proven. Phase 1A deliberately changes no legacy client or Google source. The later Phase 3 adapter translates existing server-side behaviour into bounded signed CloudTMS requests and translates results back into the existing browser response shape.", sty["body"]))
    story.append(table(sty, ["Boundary", "Must remain true"], [
        ["Legacy browser", "No new login/session/token/UUID/Supabase/HMAC knowledge; existing UI and msisdn lookup preserved."],
        ["Availability Apps Script", "Smallest server-side compatibility facade; stable request identity across timeout/reload; no direct browser CloudTMS write."],
        ["Master Rota", "Generation publisher/consumer barriers added only in Phase 3; existing triggers and unrelated consumers preserved."],
        ["Emergency/specialist", "Both old and new clients ultimately use one CloudTMS receipt/effect authority; no blind resend after uncertainty."],
        ["Decommissioning", "Legacy browser and adapter are removed only after successful proving and separate authorisation."],
    ], [43 * mm, 120 * mm]))
    story.append(Spacer(1, 4 * mm))
    bullets(story, sty, [
        "Do not repair unrelated legacy defects during coexistence work.",
        "Do not infer a Candidate UUID from browser input; the later trusted source mapping resolves and revalidates it server-side.",
        "Do not revive ai_startDailyPings or add a trigger merely because a historical reference exists.",
        "Do not let Emergency, running-late, cannot-attend, leave-early, DNA or acknowledgement actions acquire a second effect receipt.",
    ])

    section(story, sty, "62. Verification and safety result")
    story.append(table(sty, ["Verification gate", "Final result"], [
        ["Focused production-module TAP", "13 passed; 0 failed"],
        ["Complete backend TAP", "576 passed; 0 failed"],
        ["Runtime/OpenAPI route parity", "24/24 exact; merged API SHA-256 1e4362f3…f765fa"],
        ["HMAC Node/Python", "3 positive, 2 route-valid, 24 negative, 5 query, 20 raw-parser — PASS in both runtimes"],
        ["Source identity Node/Python", "6 positive, 5 normalisation, 2 malformed, 9 negative, 2 bindings — PASS in both runtimes"],
        ["R5 pack validator", "154 decisions, 25 operations, 4 policies, 63 effects, 28 adapters — PASS"],
        ["Worker dry builds", "Candidate public, Candidate private and normal TEST Worker — PASS"],
        ["Syntax/diff", "All changed JavaScript parses; git diff --check PASS"],
    ], [58 * mm, 105 * mm]))
    story.append(Spacer(1, 4 * mm))
    story.append(table(sty, ["Safety action", "Result"], [
        ["Google data/source/deployment mutation", "None"],
        ["Supabase/SQL/RPC mutation", "None"],
        ["Candidate flag/entitlement change", "None"],
        ["Worker/frontend deployment", "None"],
        ["External effect or communication", "None"],
        ["Production access", "None"],
        ["Finance/Banking Pay/Policy X change", "None"],
    ], [73 * mm, 90 * mm]))
    story.append(Spacer(1, 4 * mm))
    story.append(Paragraph(
        "The locked dependency installation reported inherited npm advisories. No broad dependency rewrite was authorised or performed. They remain a separate repository-maintenance concern and do not alter the Phase 1A source diff.", sty["small"]))

    section(story, sty, "63. Decision register extension AV-155–AV-180")
    story.append(Paragraph(
        "AV-001–AV-154 remain in the accepted R5 ledger and matrix. R6 adds 26 decisions. The full row-by-row wording and proof owner are in the R6 compliance matrix included with this PDF.", sty["body"]))
    story.append(table(sty, ["IDs", "Decision family", "Disposition"], [
        ["AV-155–AV-160", "Google gate, certified source/deployment relation and ai_startDailyPings", "Evidenced; no mutation"],
        ["AV-161–AV-162", "Minimal-change legacy containment and dual-client Emergency compatibility", "Binding; existing behaviour preserved"],
        ["AV-163–AV-167", "Sole merged OpenAPI, exact routes, additive bootstrap and dark capability", "Implemented and tested"],
        ["AV-168–AV-173", "Correlation, idempotency, body/rate/deadline and distributed concurrency ownership", "Implemented except durable in-flight owner deferred to Phase 2/1B"],
        ["AV-174–AV-177", "HMAC v1, key rotation, private nonce and separate business replay", "Implemented and vector-tested"],
        ["AV-178–AV-180", "No deployment/mutation, Phase 2 boundary and remaining full-app phases", "Gates retained"],
    ], [31 * mm, 90 * mm, 42 * mm]))
    story.append(Spacer(1, 5 * mm))
    story.append(callout(sty,
        "No R6 decision authorises Daily activation, SQL installation, Google editing/deployment, Candidate UI publication, communication effects or production. A later GO must be granted at each named gate."))

    section(story, sty, "64. Remaining phases to full implementation")
    story.append(table(sty, ["Phase", "Required outcome", "Current status"], [
        ["Independent R6 review", "Audit manifests, source diff, HMAC vectors, Google evidence and AV-001–AV-180.", "Next"],
        ["Phase 2", "Author 12 additive tables and 13 service-only RPC owners; PostgreSQL 17/18 disposable verification; separate TEST-install approval.", "Not started"],
        ["Phase 1B", "Wire all 24 operations to Phase 2, including receipts/leases, concurrency, Google projection, effects and real public-private-RPC tests.", "Blocked on Phase 2"],
        ["Phase 3", "Minimal Availability/Master server-side adapters, consumer barriers, outage/recovery proof; no legacy browser redesign.", "Evidence gate complete; edit not authorised"],
        ["Phase 4", "Daily UI for responsive web, iOS and Android from the same broker contract; no client business authority.", "Not started"],
        ["Phase 5", "Controlled TEST authority cutover, explicit global/cohort enablement and rollback rehearsal.", "Not authorised"],
        ["Phase 6", "Full Emergency/specialist/workflow acceptance through both old and new clients with explicit effect approval.", "Not started"],
        ["Phase 7", "Gradual cohorts, continuous gates, then separately authorised legacy app/adapter decommissioning.", "Not started"],
    ], [27 * mm, 106 * mm, 30 * mm]))
    story.append(Spacer(1, 5 * mm))
    story.append(callout(sty,
        "Current verdict: Phase 1A source is ready for independent review, but the Candidate Daily product remains disabled and incomplete. The full Candidate App includes the already accepted Weekly workflows plus every remaining Daily, Google coexistence, native/web UI, specialist, cutover and rollout phase above.", PALE_GOLD, GOLD))

    doc.build(story)


def merge(base_path: Path, addendum_path: Path, output_path: Path) -> tuple[int, int, int]:
    base = PdfReader(str(base_path))
    addendum = PdfReader(str(addendum_path))
    if len(base.pages) != 61:
        raise RuntimeError(f"Expected accepted R5 base to contain 61 pages; found {len(base.pages)}")
    writer = PdfWriter()
    for page in base.pages:
        writer.add_page(page)
    for page in addendum.pages:
        writer.add_page(page)
    writer.add_metadata({
        "/Title": "CloudTMS Candidate App Current Decisions — Phase 1A R6",
        "/Author": "Arthur Rai / CloudTMS",
        "/Subject": "Accepted Phase 0 R5 decisions plus Google Evidence Gate and Phase 1A dark implementation",
        "/Keywords": "CloudTMS, Candidate App, Daily, Availability, Apps Script, HMAC, Phase 1A",
    })
    with output_path.open("wb") as handle:
        writer.write(handle)
    return len(base.pages), len(addendum.pages), len(base.pages) + len(addendum.pages)


def verify(base_path: Path, output_path: Path) -> None:
    base = PdfReader(str(base_path))
    output = PdfReader(str(output_path))
    if len(output.pages) <= len(base.pages):
        raise RuntimeError("Combined PDF contains no R6 addendum")
    for index, page in enumerate(base.pages):
        if (page.extract_text() or "") != (output.pages[index].extract_text() or ""):
            raise RuntimeError(f"Accepted R5 page text changed at page {index + 1}")
        if page.mediabox != output.pages[index].mediabox:
            raise RuntimeError(f"Accepted R5 page dimensions changed at page {index + 1}")
    appended = re.sub(r"\s+", " ", "\n".join(
        (page.extract_text() or "") for page in output.pages[len(base.pages):]
    ))
    required = [
        "57. Google Evidence Gate",
        "58. Phase 1A source authority",
        "59. Routes, policies and limits",
        "60. HMAC, nonce, correlation and replay",
        "61. Legacy, Master Rota and Emergency coexistence",
        "62. Verification and safety result",
        "63. Decision register extension AV-155–AV-180",
        "64. Remaining phases to full implementation",
        "Do not modernise the legacy app. Contain it.",
        "candidate-daily-google-nonces/v1",
        "576 passed",
    ]
    missing = [item for item in required if item not in appended]
    if missing:
        raise RuntimeError(f"Required R6 decisions missing from combined PDF: {missing}")


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
        f"DECISIONS_R6_PASS|base_pages={base_pages}|addendum_pages={addendum_pages}"
        f"|total_pages={total_pages}|sha256={sha256(output)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
