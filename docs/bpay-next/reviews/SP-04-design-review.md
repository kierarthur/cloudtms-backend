# SP-04 independent design review

12 October 2026. Source/design only; no product changes, execution, SQL/RPC, tests, fixtures or hosted activity. Reviewed root `design/SP-04-routing.md` SHA256 `615f1da75ed366434b85fe03fa05e1646a949aa19b4cd77246b047ea664f3a59` against full card `05_SOURCE_AND_PROTECTED_PAY.md:188–218`, checklist `IMPLEMENTATION_CHECKLIST.md:4083–4099` and current native owners. Exact source census is saved in `design/SP-04-routing-investigation.md`.

The named-owner reuse, receipt-before-currentness, original signature preservation, exact NOT_MANAGED fallback, retained unknown retry, native rollback qualification and scoped per-row result presentation are appropriate. No new SQL owner, command, table or protocol is justified. The ordinary Weekly admission/refusal-audit tradeoff is explicit and preserves the installed Source lock gate.

**Two essential qualifications remain before product authoring:**

- **S04-D01 — prior unknown probe failure.** Design paragraph “probe failure before a write” must distinguish first attempt from retry of a previously dispatched request. On that retry, failed/malformed advisory availability cannot prove the old mutation refused or did not happen. Retain UNKNOWN and exact original request; do not block native owner receipt reconciliation behind fresh availability.
- **S04-D02 — genuine contractless Daily ordinary route.** Universal request-first routing cannot produce NOT_MANAGED for a genuine contractless Daily: `0600:436–463` requires nonnull Contract Candidate before native withdrawal checks. Add a narrow authoritative native non-Weekly/Daily exclusion; do not interpret arbitrary context failure as ordinary ownership. Possibly Source-managed Weekly/unknown roots retain receipt-first original request.

Root's separately identified caller corrections are necessary: NEXT missing/untrusted original signature refuses rather than preflight replacement (`main.js:276537`); selected bulk render_signature is not mutation authority. Stale rows use only exact W9 reasons ROW_SIGNATURE_MISMATCH / EXPECTED_TIMESHEET_MISMATCH / EXPECTED_ROW_SIGNATURE_BLANK, not all INTEGRITY refusals.

**Verdict: design review OPEN pending S04-D01/D02 clarification.** These are bounded router decisions; they do not require new money authority or installed SQL changes. Product source, A4 and joined flow remain NOT_RUN.

## Fresh design amendment

Root design SHA256 `6231cdc0f9061f155b7884a1bd8031515e2239e27f952ca40d5abd035865cbb4` explicitly makes all availability failures advisory and always reaches the original owner. **S04-D01 closed**; actual router's advisory catch agrees.

S04-D02 needs precise qualification rather than a general WEEKLY-only assertion. Existing `05102026_1210_weekly_source_office_authorise_scope_v1.sql:43–45` excludes non-WEEKLY/non-HOURS/contractless in the Office routing surface. However final public first-authorise2018:3559–3593 and private core2223 contain no independent WEEKLY check; approval_route0952:108–109 admits self-bill HOURS without that check. These do not prove that any requested DAILY row can safely bypass a historical Source receipt. Root-authorisation table SELECT is owner-only, so no direct history read or new grant is proposed.

Existing service reader `public.weekly_source_office_timesheet_presentation_v1(jsonb)` (`15092026_1534_weekly_source_read_projections_v1.sql:4256`) accepts actor_user_id/timesheet_id, reads the exact current nonrevoked/nonarchived physical row at4348 and returns native applicable:false/scope:DAILY at4352–4360. This is the card-permitted Office presentation fact, not a historical receipt census. It can support a narrow original-request-first ordinary Daily fallback after the known context-unresolved result plus exact native contract_id NULL; never override an unknown owner call, payment refusal or actual Source receipt. This preserves original receipt ordering without a new SQL owner.

## Final scoped design closure

Reviewed current root design SHA256 `c5105c44a77e72b4aa6bdf0f2679c780a839ea90cda7f9c7491ac814f3de09b4`. **S04-D01/D02 closed; DESIGN PASS.** The exact original owner is attempted despite advisory failure. Only its positively qualified contractless Daily context limitation receives the existing native presentation exception; possibly managed or unknown scope keeps original receipt-first authority. There is no denied history read or new SQL/grant.

Native single atomic1235_30:36–44/179–198 pair refusal and final0556:2546–2558 exact owner-false exception prove transaction rollback before a pair can be partially withdrawn. This supersedes the investigation's earlier preview-before-Source recommendation. Root's common and selected-bulk ordinary pair lanes instead use native confirmed preview membership and read-only complete qualification, bounded100, before the original bulk atomic owner. AUTHORISE is unchanged. Original receipt replay is not gated by fresh pair discovery.

The design retains native paid/retention/frozen-history protections, exact original mutation operands, finite native result/error contracts and unknown outcomes. No financial owner, command, table, protocol or producer expansion is required. This is design/source closure only; native result-window A4 and joined application acceptance are separate and NOT_RUN here.
