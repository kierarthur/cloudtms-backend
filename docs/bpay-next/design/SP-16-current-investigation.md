# SP-16 current source investigation

11 October 2026. Read-only investigation for approved sequence **114**, immediately after SP-15. Owning card: approved plan `05_SOURCE_AND_PROTECTED_PAY.md:548–572`; build order `08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:198`. Working checklist: `docs/bpay-next/IMPLEMENTATION_CHECKLIST.md:3367–3370` and CSV exact SP-16 ITEM/D01/D02 rows. Dependencies remain F-03, F-05, F-06 and SP-13. No product/checklist changes, business calls, SQL execution, fixtures, tests, hosted calls, captures or commits were performed. All future joined/application checks remain **NOT_RUN**.

## Decision and exact delivery boundary

**No confirmed NULL-as-absence caller defect was found. SP-16 needs verify-and-record delivery; no SQL, Worker or screen change is recommended.** The card explicitly says DB verify-and-close/no SQL change, permits only a proved caller bug, leaves 0645 unchanged, and assigns subsequent classifier edits to SP-17 then SP-34 (`05:564–568`). Do not implement a new named ambiguity code, absence payment hold, removal action, reader, zero-calculation protocol or later Withdrawn change.

The exact current roster ambiguity branch is **0645:383–388**, where the witness counts CURRENT Finals covering the event date and returns NULL when more than one exists. The card's approximate `~75` also returns NULL for ambiguous current source rows, but it is not the precise covering-Final branch. NULL remains unavailable: C1 and accepted-removal evidence refuse it; the classifier never turns it into an absence hint.

OR-38 replaces the historical two-chat agreement with recording the existing interface version and SHA256; no additional approval is needed for this verify-only card (`01_BUILDER_RULES_AND_FOUNDATION.md:145`). OR-45 preserves genuine Source/invoice/Candidate contracts and financial facts, not old Banking engine operation (`05:559`). The older `addendum/A2_FLOWCHARTS_WHAT_REACHES_WORKBENCH.md:371,404` named-code proposal conflicts with the current explicit card `05:557,564–565` and final Fable correction; it must not introduce extra work.

## Current owners and source provenance

All paths below are relative to BUILD-BE `C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-build-backend-20261009`.

| Alias / current physical source | Exact owning signatures |
| --- | --- |
| **W** `supabase/repeatable/05102026_0645_weekly_source_selected_source_witness_v1.sql` | `private.weekly_source_selected_source_witness_v1(uuid,uuid,uuid)` at 5; STABLE SECURITY DEFINER, fixed search_path, logical postgres owner and revoked PUBLIC/browser/service execution at 7–8,450–452. |
| **K** `supabase/repeatable/05102026_0512_weekly_source_operational_category_v2.sql` | `private.weekly_source_timesheet_category_v2(uuid)` at 211; STABLE factual classifier. |
| **E** `supabase/repeatable/05102026_0650_weekly_source_accepted_removal_query_evidence_v1.sql` | `private.weekly_source_accepted_removal_query_evidence_v1(uuid,uuid,uuid,uuid,uuid)` at 4; STABLE owner-only accepted-removal evidence. |
| **C** `supabase/repeatable/10102026_0853_weekly_source_pay_query_entry_admission_v1.sql` | Current registry C1 owners `public.weekly_exceptional_pay_stage_c1_request_v1(jsonb)` at 979 and `public.weekly_exceptional_pay_complete_c1_publication_v1(jsonb)` at 1991. Their earlier full parent was `15092026_1534_weekly_source_protected_pay_c1_publication_v1.sql`; do not cite that parent as the current final owner. |
| **L** `supabase/repeatable/04102026_1257_weekly_source_local_protected_decision_v1.sql` | `public.weekly_exceptional_pay_complete_local_v1(jsonb)` at 42, retained Source-local completion and atomic query-resolution caller. |
| **Q** `supabase/repeatable/05102026_0428_weekly_source_manual_review_commands_v2.sql` | Current registry `public.weekly_source_manual_review_resolve_v1(jsonb)` at 325; original 02102026_1601 is superseded. |
| **A** `supabase/repeatable/15092026_1534_weekly_source_read_projections_v1.sql` | SCX-056 `public.weekly_source_no_shifts_attest_atomic_v1(jsonb)` at 5388; original distinct client-cycle attestation. |
| **Z** `supabase/repeatable/04102026_0475_banking_pay_next_source_current_selector_v1.sql` | Current registry NX-011 `private.bpay_next_source_zero_scope_v1(uuid,uuid,uuid)` at 384; binds a real event/Contract/current ordinary root, not an absence certificate or new financial writer. |

The current final-definer registry positively selects C, Q and Z above and records the classifier editor chain SP-17 → SP-34. W/K/E/L/A have one saved defining occurrence each in the current repeatable source census; they are not separate rows in the registry's `chains` collection. Their existing Source ownership is also recorded in the port inventory and SP-13 gate census. Do not fabricate registry entries or mistake absence from this registry for absent Source functions. Fresh installed catalogue/body/ACL proof is a separate builder check; this reviewer did not query a database.

## NULL/currentness and closed-union evidence

| Card/checklist clause | Current exact source evidence | Narrow delivery |
| --- | --- | --- |
| D01 / SRQ-106 / T-12: multiple roster Finals never certify absence | W:383–388 counts only current-pointer CYCLE Finals with exact group/Client/date coverage; `v_count>1` returns NULL. W:63–75 separately refuses multiple qualified current source rows. | Record both exact branches in BUILD_LOG; preserve W bytes. |
| D02 classifier NULL unavailable, never absence | K:324–333 calls W and adds `waiting_import` only if kind is positively CERTIFIED_ABSENCE, exact selected event matches and source_present is false. A SQL NULL makes the IF predicate nontrue and appends nothing. K:335–337 adds the invoice-only hint only after positive evidence; K:249–252 explicitly adds no Candidate-pay gate. | Record this positive-only predicate; it does not emit a new named ambiguity response and does not need one. |
| D02 accepted-removal evidence NULL unavailable | E:79–82 explicitly rejects NULL, changed proposal or wrong group with `WEEKLY_SOURCE_ACCEPTED_REMOVAL_SOURCE_UNAVAILABLE`; E:89–101 admits only CURRENT_ROW or CERTIFIED_ABSENCE and otherwise refuses. | Record the explicit error and closed union; no caller fix. |
| D02 current C1 publication NULL unavailable | C:1338–1354 qualifies only native WITHDRAW/RECONCILE. It rejects NULL, changed source proposal, non-Final CURRENT_ROW, nonzero/incorrect certified absence, and every other/NULL kind with `WEEKLY_PROTECTED_C1_SELECTED_SOURCE_UNAVAILABLE`. | Record current0853, not only the historical1534 parent. |
| Missing/stale/partial/overnight ambiguity is not absence | W:91–117 refuses rebuilding/failed/partial authority; 133–178 requires exact current publication/Final pointers; 185–202 refuses unresolved/nullable mappings; 283–290 refuses compatible/overlapping or missing clocks. W:315–316 requires the explicit event proposal false/0 before any certificate arm. | Retain the existing conservative evidence qualification; no duration-only proof or newest-row substitution. |
| NO_IMPORT_YET tightly qualified | W:426–438 requires genuine OFFICE_PROTECTED_SHIFT, source_observed=false, and absence of all event resolution/source-link/snapshot/movement history before assigning this existing basis kind. | Record exact identity/history prerequisites; ordinary omission is not this arm. |
| NHSP omission never certifies | W:317–357 requires real sealed CURRENT physical positive and full-negative history, both flags true and no extra unmatched current movements, before NHSP_FULL_REVERSAL. W:292–300 retains actual NHSP report-scope seal; W:426–438 is only the genuinely history-free Office-created arm. | Retain genuine reversal versus omission distinction. |
| Roster omission/cancel positively certified | W:359–378 validates exact cancellation/previous event snapshot; 383–423 validates unique covering Final, London confirmed coverage, omission-authorising profile, current cycle pointer and exact Client manifest before CERTIFIED_ABSENCE. | No bare date, absent row or provisional empty result is a certificate. |

The existing witness interface remains **WEEKLY_SOURCE_SELECTED_SOURCE_WITNESS_V1**, with seven top-level keys `schema_version,scope,kind,basis_kind,source_proposal,basis,basis_sha256` (W:442–447). It is distinct from SP-13's five-key pay-query gate and ReceiptV3. E emits its existing eight-key accepted-removal result `lane,scope,approval_id,generation_id,publication_request_id,request_sha256,result_sha256,selected_source_witness` at 102–105, with the native selected witness nested. No signature, field or hash-domain change is proposed.

## Removal, resolution, replay and immutable facts

| Required behaviour | Actual source evidence / verification boundary |
| --- | --- |
| SRQ-090: protected4h → currentSource3h, never neutral remove payable | C:1333–1354 requires the exact current Source proposal on WITHDRAW/RECONCILE; CURRENT_ROW must be Final and source_present=true. E:89–96 requalifies this same proposal/source revision for accepted review evidence. No removal-as-NOT_WORKED conversion exists in this branch. Actual4→3 journey remains NOT_RUN. |
| SRQ-091: Source10h + absent protected3h → only selected event zero | W:38–59 binds the exact family/Client/Contract/date/event and one selected client source; K:326–328 requires the exact selected event. C:1629–1648 distinguishes the selected ACCEPTED_SOURCE event from unrelated latest WAIT siblings; C:1777–1790 appends the family event using only `v_work_event_id`, and C:1823–1828 completes pending targets for that exact event. E:50–64 requires the latest exact event and its matching approved Source snapshot. The whole-family approved vector remains owned by the original calculator/publication path; do not zero the family or rewrite it here. Actual10+3 journey remains NOT_RUN. |
| SRQ-104: real open review, accepted closed union and same-action resolution | E:22 requires actual OPEN review; 26–37 binds native WITHDRAW/RECONCILE run, approval, generation, actor and full event tuple; 38–70 requires completed local receipt/latest ACCEPTED_SOURCE/target event and post-opening action. Q:384–396 requires and locks OPEN review. Q:397–407 requires both approval and generation, forbids the alternative current-row hash and obtains E; Q:408–449 separately admits one exact qualified CURRENT_ROW for the original row arm. Q:499–508 updates RESOLVED, audits it and saves the immutable command result. |
| Completion before resolution; no second UI command | L:423–429 saves native request/run/local completion; 438–450 saves event and completion step first in the same outer transaction. L:451–460 invokes existing Q only when an actual OPEN review was found and uses OFFICE_ACCEPTED_SOURCE only for WITHDRAW/RECONCILE. Any refusal rolls the outer action back. No review means no fabricated review/evidence/resolve command. |
| Exact replay retains original cut-off/result | C:1230–1253 returns exact staged identity/hash receipt before its fresh selected-witness check; L:103–123 and 134–141 return exact retained actor/hash/key receipt with original action's report authority. Q:359–382 similarly returns the stored exact resolve result before OPEN/currentness checks; changed bytes collide. A later import cannot reprice an identical completed action. |
| SRQ-110/RQ-224 genuine zero and paid/held facts immutable | Only positive CURRENT_ROW/CERTIFIED_ABSENCE qualifies removal (C:1342–1354,E:89–101); synthetic Final-only zero is explicitly not an absence proof. W/E/K are STABLE factual reads. C appends new approval/generation/event records while retaining prior vectors/hashes (C:1617–1618,1727–1790); neither W/E nor Q rewrites payment/hold facts, and Q changes only review/audit/command metadata. Z:389–418 requires real native root identity and changes only Source-current scope metadata. Existing economic publication/ledger owners remain untouched. Genuine zero and unchanged paid/held balances across joined publication are later acceptance, NOT_RUN here. |
| SCX-056 no-shifts attestation remains separate | A:5397–5447 has its own closed actor/cycle/group/Client/version/text input; 5450–5460 locks the cycle and requires original FINALISE_WEEK authority. A:5467–5475 replays the exact same attestation, 5483–5539 refuses existing Final/source/unresolved rows/open issues, and 5563–5571 writes only the native NO_SHIFTS_TO_IMPORT client-cycle completion. It is not RECORD_NOT_WORKED or selected-shift removal. |
| Explicit RECORD_NOT_WORKED remains distinct | C:1629–1635 maps only RECORD_NOT_WORKED to NOT_WORKED and refuses source_present; WITHDRAW/RECONCILE maps ACCEPTED_SOURCE. C:1653–1661 has distinct pending/payment-event kinds. No false NOT_WORKED/date ban/Restore is added by this card. |

## Remaining delivery and checks

1. D01: builder records W exact current line388, original interface version/hash, final source/include/current catalogue and owner/config/ACL checks using its authorised source/local installation workflow. W is retained, not replaced.
2. D02: builder records K:324–333, E:79–82/89–101 and current C:1338–1354; record Q/L completion, closed union and exact replay plus A distinct attest action. Current source supplies all required behaviour; no confirmed caller patch is indicated.
3. No Worker/Screen deliverable exists in this corrected card. SCX-116/117 remain existing callers; no new screen, button, error label or A4 fixture is required by SP-16. Do not advance SP-17/SP-34 or modify unrelated Source producers.
4. Keep full later acceptance explicit:4→3 removal, unique selected event zero, two covering-Finals refusal, open-review atomic resolve, exact replay after later imports, certified zero with unchanged paid/held facts, and separate NOT_WORKED/attestation. These are **NOT_RUN**; source/installed witness status cannot stand in for the joined journey.

## Fresh saved-source SHA256

These hashes identify local reviewed files only; they are not fresh database-definition or deployed-source proof.

| Alias / artifact | SHA256 |
| --- | --- |
| W | `1D455CA1A9088CE591B046E9C4659B5994967A5F901808FAF5AB7FBD1CE04C8D` |
| K | `602DB4EDB834BE6A6B3428B5D47828B62F4C7FF3589738BC36D756B30A1B8700` |
| E | `EB2C87DAB3FDE48A9239750D409AD31894498BB12D0E7E8ADA1047C3A6E35144` |
| C | `ABC874B2D4ED90529AD591EA08A0A5131A6BC32C86109911631C8516E18B2E94` |
| L | `2A4F0D619EC2800F3D5A880459D0C0421F41C890BB6706750EB0464CAB24FB03` |
| Q | `1E9F16241FFA5E9249A7E8BF796693B81B633C2268CD1864147223451628E8D6` |
| A | `DACE15A8DF0B37DECF261BEB3CD99AC5C59EA362F22114B382C1C0148D4BF0E4` |
| Z | `BECDEC3D6892A241DEC27995B1C1DBA592DA1372FB043C911DD11C55F94FCCAE` |
| `docs/bpay-next/final-definer-closures.json` | `BBA34606FE384DC8F28D649A575CB5145905FC1521A6CA97D045F4692879084D` |
