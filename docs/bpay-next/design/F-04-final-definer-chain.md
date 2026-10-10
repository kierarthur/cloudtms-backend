# F-04 final-definer and changed-closure design

Read-only investigation, 9 October 2026. Source reviewed: current BUILD-BE saved files; RESET reference; plan file 01 F-04, its master chain table and BUILD-BE checklist F-04-D01–D08. No SQL/product/gate implementation, tests, fixtures, database runtime or hosted calls. Policy X: no drift. Source findings do not establish TEST installation.

## Current census and the chosen authorities

Paths below are under `supabase/repeatable/`. Signatures are input identities, not parameter names/defaults.

| Exact routine | BUILD-BE physical definers now | Required final authority and disposition |
|---|---|---|
| `private.bpay_next_value_source_for_target_v1(numeric,text,text,boolean,uuid,date)` | `03102026_1500_banking_pay_next_target_valuation_v1.sql:7` | Port `06102026_1157_banking_pay_shared_selected_quote_v1.sql:932` from RESET through F-06. Its target wrapper uses `private.pay_finance_selected_quote_v1` and the shared converters. BUILD-BE 1500 computes its own ERNI fractions/valuation at lines 56–72, so it is not the approved final body. RESET has exactly two physical definers (1500/1157), not three. |
| `private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer)` | `04102026_0360_banking_pay_next_case_preparation_v1.sql:8` | Keep ordinary 0360 until F-07 authors the genuinely comparison-free ordinary successor. RESET's `08102026_0549_banking_pay_next_overpayment_week_applicability_v1.sql:5` is a second physical definer; its line-29 `%rowtype` and later comparison-typed helpers require absent E-only types. Do not port raw 0549 or bless a runtime refusal with retained comparison declarations. The later ordinary/comparison chain replaces the designated final metadata per item. |
| `private.bpay_next_source_receipt_publish_v1()` | `03102026_1570_banking_pay_next_source_receipt_hook_v1.sql:10` | Port complete `06102026_1301_banking_pay_next_source_receipt_binding_v1.sql:9` with 1245/1246's source-binding dependencies. Retain the trigger's exact attachment to `private.weekly_source_entitlement_publication_receipts`, AFTER INSERT FOR EACH ROW, and reassert attachment in the successor. 1301 adds lockset/component binding; it preserves Candidate checking and publication order. |
| `public.weekly_source_first_authorise_v1(uuid,uuid,text,uuid)` | `17092026_0600_weekly_source_first_authorisation_v1.sql:2464`; `26092026_0202_banking_pay_stage2_source_authorisation_v1.sql:3387`; `03102026_1560_banking_pay_next_source_first_owner_hook_v1.sql:8`; `04102026_0557_banking_pay_next_source_owner_completion_v1.sql:93` | 0557 remains final after every earlier changed physical definer or root closure containing one. Preserve retained Source/Candidate/invoice semantics and actual financial admission. |
| `private.bpay_next_source_current_lock_inventory_v1(uuid,integer,uuid,uuid,uuid,date,date)` | `04102026_0505_banking_pay_next_source_current_hooks_v1.sql:93`; `04102026_0557_banking_pay_next_source_owner_completion_v1.sql:29` | 0557 is also this routine's explicit final authority. Its top-level dependency guard requires the 0505 predecessor to exist. Include this exact signature in F-04 metadata, not only the four headline routines. |

The static BUILD-BE NEXT-named physical-definition census found only the last row as a currently duplicated `bpay_next_*` name. Future feature successors will add duplicate ownership; they must enter the registry in the same change. This census is a source-level name census, not a substitute for the signature-aware gate.

Recursive roots matter: current `28092026_1234_stage2_hosted_final_authority_reassert_v1.sql:22` includes `26092026_0202_banking_pay_stage2_source_authorisation_v1.sql`. Thus an edit to that root/any transitive child can reapply an older first-authorisation definition despite no change to its physical defining file. The 0557 final has the later real date. Keep an ordered root/physical-file relationship for each signature.

## Apply-time predecessors and removal decision

Current 1500 defines/revokes only the target valuer. Searches of BUILD-BE migrations/repeatables show runtime calls in `03102026_1620_banking_pay_next_fresh_positive_hold_v1.sql:186` and `04102026_0360_banking_pay_next_case_preparation_v1.sql:350`; no inspected top-level DO/SELECT/trigger attachment invokes or resolves this valuer between 1500 and 1157. Recommend replacing 1500's obsolete definition/ACL statements with a truthful relocation-only repeatable once 1157 is in the same reviewed compilation batch, leaving exactly one physical target-valuer definition. Preserve the old file path/chronology; do not drop the installed routine. Re-scan top-level dependencies immediately before that edit because later build files can change this finding. A retained temporary predecessor requires a recorded reason and mandatory final closure whenever touched; it is a transitional state, not F-04's final one-definer acceptance.

Do not remove first-authorisation predecessors merely to reduce counts. 0557's top-level `DO $installed$` requires both `private.bpay_next_source_current_lock_inventory_v1(...)` and `public.weekly_source_first_authorise_v1(...)`, plus Source lock/stage/pair-publication routines and the exact deferred cleanup trigger. It then installs two complete successors while preserving public ACL. The receipt hook's predecessor creates its real trigger; 1301 explicitly recreates it, so keep the historical predecessor under the documented final-closure rule. The case page's actual runtime caller is `04102026_0280_banking_pay_next_financial_job_service_v1.sql:358,376`; retain the ordinary base until F-07 supplies its compile-safe successor.

## One fail-closed source gate and shared selection metadata

Reuse `inventory()`/`sqlDateKey()` from `scripts/cloudtms-db-release-lib.mjs`; reuse canonical include resolution (`closureFor` / `readManagedClosure`) and LF canonical bytes. The existing Stage-2 reassertion pattern demonstrates source-owned recursive final closures; it is not a generic changed-signature registry. Add one F-04 source gate under `tests/`, invoked by the existing source workflow and protected database source gate, with one shared data-only final-authority registry consumed by that gate and F-23's local loader. Do not add a second selection ordering implementation.

Each registry entry needs: exact schema/name/input-type signature; designated physical definer; designated final executable root; allowed historical physical definers; predecessor dependency reason when retained; ordered editor item chain; defining closure paths/hashes; optional required final ACL roots; owning item/last completed chain link. Every successor item updates the entry in the same source/contract commit. Keep planned files separate from executable entries: a missing future final must fail when required, not be silently treated as installed authority. F-25/F-26 add only exact signatures actually changed.

Suggested gate algorithm:

1. Enumerate all repeatable roots through current `inventory()`, including new untracked local roots; resolve each recursive include in execution order. Extract top-level CREATE FUNCTION/PROCEDURE identities while keeping comments, quoted strings and dollar bodies opaque. Parse parameter modes/types, ignoring parameter names/defaults and OUT-only arguments; preserve array/schema/type identity and overloads. Refuse an unsupported/ambiguous header rather than guessing. Do not count function text inside dynamic strings/comments as physical definers.
2. Build both physical-definer and ordered-root-definer inventories. A root can define the same signature repeatedly through includes: the last statement in that closure is relevant. For every registered signature, require the registered physical source to define it, require the final root to contain its chosen definition as that root's last definition, and require the final root to occur last among all roots defining that signature in canonical inventory order. Unknown competing definitions of registered/touched signatures fail closed. Preserve unrelated existing authorities by explicit baseline disposition rather than a blanket duplicate exemption.
3. Derive the changed path set separately for HEAD commit, explicit CI comparison base/range, tracked working tree and untracked SQL. Validate any chosen base is an exact ancestor. Deletion/rename cannot silently remove an authority. A root is pending when its own path or any recursive child is changed; at installation, also calculate closure hashes against the loader checkpoint or actual installed repeatable ledger. Never infer pending from editor-chain completion alone.
4. For each signature any pending root can redefine, find its registered final root. If an earlier root is pending, require that final controlling root to be pending too and selected after the predecessor. A commit merely changing an unrelated helper inside the final file is not enough if the actual recursive executable closure does not contain the final definition; check closure membership/order. A metadata-only force-selection entry is useful to the local loader but cannot satisfy protected UPGRADE's changed-hash selection unless the release selector consumes that same explicit authority. With the current managed release selector, change a source-owned final closure so its canonical hash is pending.
5. Keep NEW and UPGRADE selection assertions separate. NEW validates all actual roots/order; UPGRADE validates the selected changed roots and required final/ACL reassertions. A clean NEW success cannot prove changed-only correctness. Expand required finals until stable because a final root may redefine additional registered signatures. Detect cycles, unavailable roots and later unregistered competitors. Keep the final selected list filtered from canonical inventory; never append roots in metadata order after sorting.
6. Inspect top-level apply-time references/trigger attachments before removing predecessors and record exceptions as exact signatures/files. Check owner, security mode, fixed search_path, intended ACL and attachment source alongside definition ownership; a last body with stale grants is not sufficient.

Source diff inputs must cover more than `git diff HEAD`: that omits untracked files and says nothing about the HEAD commit itself. The protected workflow needs the reviewed release range/checkpoint as well as HEAD verification. Neither `db:check` nor `db:contract:coupling` proves final-definer selection. Source gates are allowed by start-sheet §3; do not introduce fixture/runtime exercises or a financial proof programme.

## Authoritative editor chains

The master table at `<!-- CHAIN-TABLE-SYNC -->` in plan file 01 is controlling. Preserve every row, including the one-editor row and intake rows, as chain metadata; expand file groups to exact signatures from current source before gate registration. No card-local chain may override it. Current primary source families and orders are:

| Family | Ordered editors |
|---|---|
| 0570 selection append/seal/work-choice guard | WB-36 → WB-25 |
| 1600 accepted cut | F-05 → F-25 → WB-36 → WB-40 |
| Case-page and later allocation plan | F-25 → CM-02a → WB-27 → DR-04 → DR-11 → DR-12 → FC-10 → WB-41 → CM-13a → CM-02 → CM-10 → CM-12 → CM-13 |
| 0140 bind destination | EX-06 → CM-13a → CM-13 |
| 0145 transition guard | EX-17 → EX-15 → EX-13 |
| 0150 issue CSV | EX-11 → EX-32 → CM-13a → CM-13 |
| 0200 settlement posting | DR-38 → EX-14 → EX-15 → EX-19 → EX-22 → EX-25 → CM-13a → SP-29 → CM-13 |
| 0230 returns | EX-31 → EX-25 |
| 0240 reissue | EX-32 → PS-10 |
| 0260 simple cancel | EX-27 → EX-13 → EX-23 → CM-15 |
| 0430 week outcome/cancelled week | EX-27 → EX-13 → EX-23 → CM-13a → CM-14 |
| 0460 case cancellation | EX-27 → EX-13 → EX-23 → CM-13a → CM-14 |
| 2102 destination cancellation | EX-27 → EX-13 → EX-23 |
| PS_C02 cancel-fact owners | EX-27 → EX-13 → EX-33 |
| 1157 shared selected quote | CM-04 → CM-13a |
| Workbench refresh work/case classifier | WB-36 → WB-08 → WB-02 → WB-07 → WB-40 → WB-41 → WB-29 |
| Selection append/work choices | WB-36 → DR-01 → WB-40 |
| 0490 internal zero-cash settlement | EX-34 → CM-13a |
| 0510 expiry | DR-08 → DR-05 → FC-15 → CM-13a → CM-15 |
| 0120 begin transfer | DR-23 → DR-22 → DR-26 |
| 0390 case PAYE net | DR-37 → DR-23 → DR-22 → EX-13 → CM-13a |
| 0410 case transfer | DR-23 → DR-22 → DR-26 → CM-13a → CM-13 |
| 0130 transfer members | PS-01 → DY-09 → DR-38 → DR-22 → CM-13a |
| 1519 collection/capture guards | FC-15 → SP-09 → CM-13a → CM-13 → CM-21 |
| 1610 position capture | F-25 → WB-36 → SP-14 → SP-20 → DR-38 → CM-13a → CM-03 → CM-10 |
| 1620 fresh positive hold | F-25 → WB-36 → SP-20 → DR-38 → WB-29 → CM-03 → CM-19 |
| 1530 approval | WB-01 → WB-36 → WB-02 → DY-04 → DY-06 → DY-10 → DR-38 |
| 1430 position writer | WB-06 → CM-10 |
| 07102026_0018 Daily evidence | DY-02 → DY-08 |
| Approval route | WB-03 → DY-04 |
| 06102026_1206 payment history | PS-07 → EX-41 → CM-18 |
| Source gate-input writers | SP-12 → SP-07 → WB-02 → WB-05 → SP-15 → SP-09 → SP-28 |
| 17092026_1200 Source audit/export | SP-25 → SP-27 → SP-28 |
| 05102026_0512 classifier | SP-17 → SP-34 |
| Worker `routes.js` handleCommand | SP-10 → CM-08 |
| 0310 case guards; 0355 reissue cancel; 2120 approval basis; 0580 status; 0100/0110/0420 PAYE-net owners | One editor each as master table assigns; no competing chain |
| 0270 financial intake and 0575 preparation service | Each item's own dependency order; keep all previous command/page kinds |

WB-36 sequence48's current1100 exact predecessors and newly authored private support signatures are recorded in `WB-36-successor-provenance.json` and the final-definer registry. The thirteen native replacements merge1009 ordinary approval;0570 choice guard;2018 preparation args/intake/capture/hold/reconcile;1550 Source staging;1548 frozen-pair detail;0345 protected staging;1519 collection/capture guards. Later editors merge these1100 bodies. Existing0200/0490 common payroll and0316/1647 recovery/forgiveness receive private share callbacks beneath their native owners; their original money bodies and0230/0240 cash owners remain current. WB-08/WB-19/WB-44/WB-45 retain projection/dispatcher/stale-refresh/resolved-warning obligations. Rule-B query snapshot and post-click revision REVIEW remain unchanged; no parked F-25 authority participates.

Preserve every explicit master-table row, including the Worker row, one-editor grouped row and intake row. The checklist's shorthand "all 33 editor chains" must not cause a later master row to be dropped; a count alone is not semantic coverage.

From WB-27 the chain edits also belong in new STABLE, write-free `private.bpay_next_case_allocation_plan_v1`, including read-only instruction derivation. F-04 must create `case-allocation-chain.md` and `intake-successors.md` with this controlling order and initially empty branches-added lists. Those are separate F-04 deliverables, not claimed complete by this investigation note. F-07 owns the ordinary successor; F-06 owns the local 1157/1301 imports; later CM items own comparison promotion.

## Checklist completion boundaries

F-04-D01/D07 require the ordinary case successor to be compile-safe; D02 requires mandatory final 0557 selection; D03/D05 require one shared-calculator target valuer; D04/D06 require 1301 and its dependencies/trigger attachment; D08 requires the signature/closure gate plus all changed-signature coverage and both mode-selection checks. Build-status evidence must distinguish the current source census, planned successor and actual gate/SQL completion. Local schema compilation and sealed contract do not establish post-release function read-back or app acceptance. The later authorised release retains its full verifier inventories.

WB-36 final replay closure: `10102026_1236_banking_pay_next_segment_authority_final_v1.sql` replays the installed 1100 segment authority and then issues the required PostgREST schema reload. All 40 physical definitions remain 1100; later editors merge those definitions and retain this final-root ordering.

The 1236 final successor is packaged atomically with the exact 1100 definitions and ACLs, followed by reload inside its transaction; all 40 final physical definitions are therefore 1236, and 1100 is retained as its installed predecessor. This supersedes the provisional include-closure packaging described above.
