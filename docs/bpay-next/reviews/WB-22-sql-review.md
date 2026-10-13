# WB-22 SQL source review — 2306 migration and native acceptance seams

10 October 2026. **SOURCE PASS for the two-table migration, four capture/default helpers, native Create/seal successors and private memory guard. H01–H05 are closed in fresh saved source; no actionable finding remains in this finite scope.** Reviewed against BD077, the full WB-22 card/checklist, WB-22-component-acceptance-seams.md and canonical native 1832/2232/1236 owners. The owner reports sole-local PG17 compilation of one migration/four closures and db:check 332/862; this reviewer performed no execution and does not independently certify those runs. The larger common/public reader successor and JavaScript integration are separate review scopes; this is not whole WB-22 or in-app approval.

## Findings

### H01 — explicit SUBSET widened to ALL — CLOSED in fresh source

The initial choices helper allowed a line absent from an explicit selected Work SUBSET to fall back to ALL_MATCHING's true default. For a two-component Work with only A explicitly selected, B could be selected too, and Create's bool_and then made the Work ALL. Fresh helper now has a dedicated selected SUBSET branch returning the actual component selected flag with absent=false, after Candidate/Work false dominance. This closes that widening for current explicit input; retained negative overrides still win through their existing branch.

### H02 — same-channel financial target changes — CLOSED

Fresh leaves join the actual Candidate and Umbrella and include target_umbrella_id, target_umbrella_vat_chargeable and min_take_home_wtd. Same-channel target-company or PAYE-floor changes therefore participate in exact equality. These are the native settings already captured by this Create into original Rule-B selection_candidate; no global financial_view_revision/name/bank heuristic is used for persisted leaf equality.

### H03 — required HEADER context accepted SQL NULL — CLOSED

The migration's HEADER row-shape check now explicitly requires accepted_context IS NOT NULL before its object/type/size terms. The earlier nullable CHECK result cannot admit a missing required header context. LEAF context remains explicitly NULL.

### H04 — heterogeneous trigger NEW.row_kind planning — CLOSED

The final guard now tests the mode-memory table name first, then accesses NEW.row_kind in that separate branch and records v_header. The exclusion-memory INSERT path never plans a row_kind field absent from its table. UPDATE of exclusion rows is refused before any mode-only OLD field expression; DELETE/TRUNCATE are refused before NEW access.

### H05 — native pay-hold absent from exact changed basis — CLOSED

Fresh leaves no longer reads the unpopulated i.pay_hold placeholder. Its pay_hold operand is the original positive native predicate: private.bpay_next_work hold_work → public.timesheets hold_sheet ON hold_sheet.booking_id=hold_work.booking_id AND hold_sheet.is_current=true → public.timesheets_financials hold_financial ON hold_financial.timesheet_id=hold_sheet.timesheet_id AND hold_financial.is_current=true, restricted to this Work and hold_financial.pay_on_hold IS TRUE. This is exactly the existing Create E1 join and positive flag semantics. It does not borrow an old retained physical Timesheet or assert a separate financial unheld certificate. A true/false native hold change now participates in leaf equality; no WB40 writer/trigger is built early.

## Reviewed correct seams

The migration creates only the two private owner tables with namespace/mode/header/typed leaf and deliberate four-kind intent. It has bounded JSON/key fields, positive native sequences, typed-kind row shapes, natural PKs and native namespace/Candidate indexes. FORCE RLS, canonical logical postgres owner/policy and revoked browser/service grants match the existing release adapter pattern. No public trigger, business backfill, command kind or financial snapshot authority is added.

Capture now stores one native eligible member per bounded LEAF object, keyed by real Work/component/member-kind/member-key or Case-component UUID. The choices helper groups those rows into the actual approved-line/component selection unit and checks both additions/changed basis and disappearance against the original member set. Current-member joins bind native leaf_key and exact basis; the reverse prior-member check catches a disappeared eligible member. Inherited false overrides require equality in both directions against their original basis_ref_run_id, so copying a later baseline cannot requalify an old exclusion. These comparisons are scoped to the actual approved-line or Case-component unit: another changed sibling does not release an unchanged sibling's exclusion. Work revision/line changes are detected by the retained immutable fields even when stable component/member keys remain the same. Native item uniqueness and AVAILABLE/ZERO checks supply the actual member identity/eligible portion. It does not aggregate arbitrary segment arrays into one bounded JSON cell. Names/references, bank-warning presentation, projection/adoption metadata versions and global Candidate financial_view_revision are excluded from persisted leaf comparison. Source/ordinary captured revision identities remain explicit. The Case component/rule field names used by the helper exist in 0300's stored schema. Native monetary values are comparison facts, not recalculated values.

Resolve distinguishes omitted/default latest lookup from explicit run_id:null detached defaults, validates finite reference shape and qualifying scope, and chooses acknowledged matching namespace by original agency_sequence. Capture stores the original raw bounded intent and every current eligible leaf before accepted descriptor installation. Inherited rows retain their original basis_ref_run_id; copying them does not requalify old false choices against today's facts. The inherited INSERT now excludes exact identities overridden by current raw input before insertion; no DELETE/replacement operation bypasses append-only memory guards.

Choices default new/missing/different precise leaves true; unchanged NONE false; unchanged ALL applies an inherited false only when the original referenced leaf basis equals this leaf. Thus an unchanged old excluded sibling stays excluded when another sibling changed; an old basis reference is not refreshed by a new generation. Candidate false dominance and explicit current input precede default selection. H01's fresh branch preserves selected Work SUBSET identity.

Create keeps original descriptor validation, pure exact replay before fresh Source admission and locks, sorted scoped Candidate/Umbrella/Work/revision/Case-parent preholds, earlier-command checks and original single command clock. It keeps native held-parent/context validation, immutable choice membership, original E1 approval/current+applied/future-week/CURRENT native hold/snooze-cut/Source-query checks, Rule-B selected Work/Candidate/query snapshot objects, empty/no-command behavior and original return envelope. The optional memory branch is distinct; legacy five-field descriptors keep their existing branch.

The implicit subset path now publishes genuine work_choice_count and actual currently SEALED count after native Work choices exist, before calling unchanged component append/seal. This respects 1241's sealed_work_choice_count<=work_choice_count check. Each actual page carries 1..100 distinct original approved-line IDs under the expected revision, then seals from the true native page/count. It does not replace component-selection owners or directly forge their rows/counters.

The descriptor is installed before explicit unchanged materialise_choice_segments calls for server-sealed subsets. Materialisation runs in the same original Create transaction and uses complete original selected-component segment inventory plus the run's retained snooze control sequence/business date. Native zero-segment handling remains through the existing boundary/count trigger. Matching/overall seal cannot run with missing segment seals. Source snapshot and captured Candidate settings remain the original Rule-B authority.

Seal retains the actual current 2232 body's count/upload/exclusion/Case/query/segment closure, exact sealed replay and legacy Rule-B branch. Memory acknowledgement is before original selection_state SEALED and uses only this run's retained command/sequence; it does not refetch or choose another financial cut. Exact sealed replay returns before acknowledgement. Ordering by accepted sequence prevents a late earlier seal from taking precedence over a newer accepted generation. Empty Create attaches no fabricated command or sequence: its HEADER attachment UPDATE is restricted by v_sequence IS NOT NULL.

The final private guard locks the genuine run, requires PREPARING/OPEN and no accepted descriptor for new capture rows, binds actor/date/current installation/NEXT owner epoch, binds leaf/exclusion namespace to its HEADER, and validates actual Work+approved-line/Candidate or Case-component/Candidate ancestry. Header updates freeze every field except native command/sequence and acknowledgement: command attachment is one-time and matches accepted_choice_command_id plus genuine run_command/PREPARE/module epoch/sequence; acknowledgement is one-time. LEAF/exclusion updates and every delete/truncate are refused. Trigger attachments are only on the two new private memory tables; no public producer or financial owner is added.

## Pending batch boundaries

The two-table migration, fresh guard and native basis now close all five findings from this review. Required repository gates/local compile and seal/coupling remain the owner's verification work; this source review did not execute them.

The common/public reader, Worker/Office transport and local-baseline adoption/count/default behavior are outside this finite acceptance/guard checkpoint. Original component-seal replay can acknowledge a genuinely server-sealed implicit subset without a new raw ID upload; post-descriptor first-time component append/seal cannot accept inferred IDs. The real sequencer must use retained native sealed state accordingly.

Full application reopen/NONE/Keep/new-date/changed-unit/large-membership journeys are NOT_RUN. No SQL compile, database query, test/gate, runtime/browser/render tool, fixture, secret, hosted call or product edit was performed by this reviewer.

## Fresh source SHA-256

| Saved source | SHA-256 |
| --- | --- |
| 10102026_2306_banking_pay_next_actor_selection_memory.sql | 51cdfb58b668afb18bfa83083d2a14ac3c33b701a6d635feb2b4e592db8c49bc |
| 10102026_2306_banking_pay_next_actor_selection_memory_v1.sql | 3f77bbb57500f1320d3873935c8504264a46e50287be9740267f838f8d1dc750 |
| WB-22-selection-memory.md | 42d250250f540508c8c27e3d2e265dd1609781e0fbb9f722b934b9b70dbeea77 |
