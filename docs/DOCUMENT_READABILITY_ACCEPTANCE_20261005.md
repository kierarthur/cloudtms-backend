# Timesheet and expense document readability — 5 October 2026

Status: implemented, locally verified and deployed to the normal TEST Worker family. Existing persisted documents remain immutable.

## Reviewed existing documents

The actual submitted electronic Timesheet for Kier Arthur / Arthur Rai Medical Services, week ending 5 July 2026, was downloaded from the authenticated normal TEST Office Evidence panel and visually inspected. The generated current expense summary and approved attached expense evidence for the same Timesheet were also downloaded and visually inspected. No Office, Candidate, manager, payment or evidence mutation was performed during inspection.

Existing persisted evidence is immutable and is not overwritten by these changes. New document generation uses the updated renderer. Local visual QA copies are explicitly not new submissions or proof of deployment.

## Presentation changes

- Timesheet day/date, start/finish, breaks, numeric hours, hours in words, Band and booking reference grow with the actual cell height, up to 12 pt. Width fitting preserves the complete value and the existing readability floor.
- Candidate surname, first name, job profile, Client and site/ward values use up to 11 pt. Dense detail panels retain enough space below the final row; labels are proportionate and remain separate from values.
- Timesheet overall hours use up to 11.5 pt, and schedule headings and signature dates are enlarged without changing declaration wording or signature layering.
- Expense claim and current-record summaries use 14 pt identity text, wrapped without silently truncating names. Amounts use up to 18 pt, with totals up to 20 pt; amounts are right aligned within their value column.
- Receipt/photo evidence has 12 pt claim detail text and a 16 pt claim total above the unchanged source evidence. Claim categories and their values are kept together when wrapping. The source image/PDF is contained below that area, without changing its bytes, digest or ownership.
- Mileage evidence uses a 15 pt fitted mileage banner. The existing 88 pt paper-return QR and its clear area remain unchanged. Manager name/position/date values are fitted within the approval area and do not overlap the signature.
- Excessively long expense identity text fails explicitly before crossing the content/footer boundary rather than being silently cut off. No financial or approval rule is changed.

## Verification

328 tests passed across document readability, official Timesheet PDF, Candidate backend, Daily official document, paper/QR pack and asynchronous invoice suites. These include one-page capacity, dense layouts, horizontal fitting, deterministic rendering, actual frozen-model immutability, signatures as the final layer, long expense identities, large amounts and unchanged source ownership/digest guards. PNG and JPEG evidence are checked separately, including rejection of an incorrect digest on both manager-review and paper-return layouts.

Visual QA renders cover ordinary and dense Timesheets, a paper-return Timesheet with long Client/site values, receipt manager review, receipt paper-return QR, mileage evidence, claim summary and current-record expense summary. They are stored under `codex_outputs/document-readability-20261005` and must be visually inspected again after any subsequent renderer edit.

An uploaded image extracted from existing downloaded TEST evidence was additionally rendered through the current image path with illustrative claim values, in both manager-review and paper-return layouts. It fits below the enlarged figures and outside QR/signature areas. This local QA reuse does not alter the original evidence or claim. The screenshot gallery is `codex_outputs/document-readability-20261005/SCREENSHOTS.md`; private QA artifacts must not be committed as public repository content.

Wrangler 4.125.0 compiled the real TEST private Worker and synthetic TEST private Worker successfully with `deploy --dry-run`. These checks did not upload or deploy any Worker.

No database definitions, pay values, Timesheet grouping, approval eligibility, invoice-source policy or Banking Pay interfaces are changed by this presentation batch.

The independent release copy is based on the previously verified `23aa32eef75ea219afffbbb9b2fe2faa7fba38c8` database source. Only the two renderers, their focused tests and this record are included; unfinished joint Source/Banking edits and private visual artifacts are excluded. Its complete backend suite passed 1,448 tests, the focused renderer suite passed 328 tests, database source integrity passed with unchanged 256 migrations/684 repeatables, and dependency/credential guards passed. Hosted publication still requires the automatic TEST coordinator and current connection proof.

## Protected TEST publication

The automatic coordinator released exact commit `f7576bdf1f72ad18bfd3c89850d7639654b56607` on 5 October 2026. Protected database run `37377049578` returned `VERIFIED_OR_EXACT_UNCHANGED`; this renderer batch installed no new SQL. The normal backend, real private Candidate API, synthetic private Candidate API and public Candidate broker then deployed in that order, completing at 21:44 UTC. Fresh Cloudflare API reads independently proved all four active versions at 100% and their successful Git builds at that exact commit.

Normal backend `/healthz`, Candidate broker `/healthz` and signed private-service readiness through `/readyz` passed. These are deployed-runtime probes, not a newly submitted claim or new manager decision. Private visual QA screenshots show newly generated renderer output; historical signed evidence was deliberately not regenerated, overwritten or relabelled. Office and unfinished joint Source/Banking definitions were not included in this release.
