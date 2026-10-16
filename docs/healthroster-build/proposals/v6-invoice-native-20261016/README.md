# V6 invoice native source checkpoint — 16 October 2026

Owner-authorised source development only. No database SQL, release, email,
payment, Office publication or Banking worktree mutation was performed.

The complete amended native functions in `forward/` are derived from exact I2
`a934ee1d460b444939234e812babccb90dc3ddff`, recursively and chronologically
resolved by `read-current.mjs`. `CURRENT_NATIVE_INPUTS.json` records the exact
owner definitions. `SOURCE_MANIFEST.json` seals the forward bytes. This directory
is self-contained; the unchanged paused preservation predecessor is retained in
`reference/facts_predecessor.sqlinc`, rather than depending on another worktree's
uncommitted file. All prior paused invoice, Agency and Source work is preserved.

`node build.mjs` regenerates full owner definitions using unique-hunk assertions.
`node --test source.test.mjs` passes 17 source regression checks. `parse-source.py`
passes SQL, PL/pgSQL and inner SQL grammar checks for all 45 files using pglast
7.20 installed only in `C:/tmp/healthroster-v6-invoice-parser-20261016`.
These checks do not prove catalog binding, monetary output, ACL execution,
concurrency, rendered documents, installed API behavior or phone acceptance.
Native qualification remains `REQUIRED_NOT_RUN`.

The second source checkpoint also proves PostgreSQL AST equality for all 28
operands of the original native invoice-line projection, ignoring only source
locations. Native generation factors that one projection into
`native_planned_lines`; it does not calculate an alternative price or rounding.

Implemented source includes immutable original Source facts/certificate,
before-first-final handling, original-A admission and presentation, exact
component/current claim resolution, closed DRAFT_ONLY scoped credits, native
generation/document retry dispatch, and actual-credit checks at both Issue
phase transitions and the final invoice update. Mixed authority stays per slice.
Source and uninvoiced scopes create no credit, replacement or native job.

Owner-only ABI (all new functions revoke PUBLIC, anon, authenticated and
service_role; existing native function ACLs are preserved):

- `weekly_source_invoice_preservation_facts_v1(uuid[],uuid[],boolean) -> jsonb`
- `weekly_source_invoice_certificate_v1(uuid[],uuid[],boolean) -> jsonb`, exactly
  `{phase,hash,source_row_ids,accepted_final_member_ids,original_invoice_timesheet_ids,invoice_ids,net,vat,gross}`.
  Before a final report, final/invoice arrays are empty and money is NULL.
- `weekly_source_invoice_certificate_assert_v1(jsonb,boolean) -> jsonb` rebuilds
  original invoice facts and compares the complete certificate.
- `invoice_contract_move_source_certificate_v1(uuid,boolean) -> jsonb` reads the
  accepted review's saved original certificate; true also verifies current facts.
  Later normal invoice lifecycle changes do not rewrite the saved certificate.
- `invoice_contract_move_inspect_v1(jsonb,uuid,boolean) -> jsonb` consumes actual
  physical scope; `invoice_contract_move_preflight_v1(uuid,uuid,boolean) -> jsonb`
  consumes its immutable accepted review. Known blockers precede Ready.
- `invoice_contract_move_prepare_v1(uuid,bigint,uuid,timestamptz) -> jsonb`
  requires actual same-transaction placement and registers durable native work.
- `invoice_contract_move_obligation_assert_v1(uuid,bigint) -> void` verifies the
  complete slice partition and exact native child registrations.
- `invoice_contract_move_invoice_after_seal_v1(uuid,bigint) -> bytea` writes
  `operations.after_invoice_hash` from actual postconditions. Pure Source uses
  actual certificate.hash and requires before_invoice_hash in that same domain.
  Mixed/ordinary uses `CONTRACT_MOVE_AFTER_INVOICE_OBLIGATION_V1`, sealing actual
  placement, Source certificate, slices, scopes, groups and native child IDs.
  Mixed operation hashes must never be compared with an unrelated Source hash.
- `invoice_contract_move_billing_read_v1(uuid,uuid) -> jsonb` returns exactly
  `{status,revision,group_id,visible_summary}`. `group_id` is the genuine native
  preparation operation UUID. No invoice access means NULL ID and REDACTED
  non-NULL summary. NOT_REQUIRED always has NULL ID and summary.
- `invoice_contract_move_prepare_advance_v1(jsonb,timestamptz) -> jsonb` consumes
  the existing native four-part fenced claim; no second queue or lease.
- `invoice_contract_move_issue_context_v1(uuid,uuid,boolean) -> jsonb` resolves
  the complete actual correction invoice group for deliberate native Issue,
  including retained earlier issued credits referenced by current native Draft
  edits. Its internal `operation_ids` array contains the complete bounded set.
- H104 `timesheet_contract_source_coverage_v1(uuid) -> jsonb`, H105
  `timesheet_contract_move_invoice_certificate_v1(uuid,boolean) -> jsonb`, and
  H228 `invoice_credit_contract_move_scope_context_v1(uuid,boolean) -> jsonb`
  project actual original Source/native line/document/settlement evidence.
  A certificate's scope_id is NULL until an actual ordinary invoice scope is
  allocated; preserved Source invoices never acquire a fabricated scope UUID.
- `invoice_contract_move_prospective_source_v1(uuid,uuid,uuid)` supplies only
  genuine destination settings/Contract facts to the existing native grouping
  and reference kernels. No preview B/financial/invoice row is allocated.
  `invoice_contract_move_group_facts_v1(uuid,jsonb,boolean)` normalizes actual
  native member/segment/week identities for the post-allocation comparison.
- `invoice_contract_move_selected_drafts_prepare_v1(uuid,uuid,timestamptz)`
  calls native invoice_apply_edits for exact selected removal and actual B
  addition, or native relocation registration. It verifies unselected lines and
  current hold purpose. Exact old financial lock events use the real attached
  `private.timesheet_contract_move_financial_row_guard_v1`, not the evidence
  guard (which is attached to timesheet_evidence only).
- `invoice_source_original_claim_enter_v1(jsonb,timestamptz)` validates the
  existing native four-part claim and actual invoice actor before recording
  SQL-derived backend/transaction provenance on that same leased chunk.
  `invoice_source_original_claim_v1(uuid)` rechecks all of those actual facts.
- `invoice_source_original_line_plan_v1(uuid,jsonb)` seals the unchanged native
  intended rows, original Source certificate/financial seal/canonical members,
  and original native claim. It runs in the existing deferred-header path.
  Existing consolidation Drafts use an equivalent defer-once path before any
  Source segment lock or line insertion. No second queue, phase, context store,
  GUC or config authorization was introduced.
- `invoice_source_original_line_allowed_v1(jsonb)` checks the saved immutable
  intended row and current native claim, actual original financial authority,
  duplicate coverage and replay. Exact duplicate same-invoice rows retain the
  native ON CONFLICT retry behavior; changed replay rows are refused. Current
  native claim stamps are removed before return and roll back on exceptions.
  Normal native retry control changes retain the immutable original witness;
  current claim/control still must match the actual parent and leased child.

Integration still required: the newest physical owner source/schema (including
the actual financial guard and accepting_v1 predicate) before these bodies;
genuine Banking accepted valuation/current financial cut and publisher;
replacement B document/evidence consumption of the physical owner's certified
original-evidence source; Daily same-ID native revision integration; and native
catalog/ACL/security/financial/document/concurrency proof. Ordinary preflight
reports INVOICE_CONTEXT_UNAVAILABLE specifically when the genuine accepted
native valuation is absent. Native selected Draft/repeated Unissue logic,
operation-wide entitlement partition, prospective grouping/reference adapters,
Source original-A collection, B PAY-only exclusion and the legacy intended-line
witness are implemented source and remain unqualified until native execution.
`NATIVE_QUALIFICATION_CASES.md` records the actual required hostile/retry cases;
none is reported as a database pass.

The existing worktree `npm run db:check` baseline rejects the prior unrecorded
`13102026_1344_weekly_source_self_bill_report_schema.sql`; no whole-worktree lock
refresh was used to bless unrelated prior migrations. Root owns integration and
the protected commit-bound release process.
