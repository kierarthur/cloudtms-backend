# F-25 Rule B: selection snapshot seams

10 October 2026, read-only saved-source investigation. The owner's binding `MESSAGE_TO_BUILDER_F25_SIMPLIFY.md` supersedes the producer/version-evidence requirement. Stop the protocol work. This note proposes only the authorised selection snapshot and three consumer read changes; it does not authorise product edits or claim installation/runtime acceptance. No SQL, tests, fixtures, hosted reads or product edits were performed.

## W10 base and exact latest retained seams

W10 (`02_WORKBENCH.md:46`) and the master F-04 chain (`01_BUILDER_RULES_AND_FOUNDATION.md:429–448`) require the latest BUILD-BE branch to be retained and the small hunk merged there. After the specified F-25 parking, the retained controlling bodies are:

| Owner | Actual retained source and hunk |
|---|---|
| `private.bpay_next_seal_selection_v1(uuid,bigint,bigint)` |1590:188–235. Same-key SEALED replay214–217 precedes all new capture. Preserve count/component/case-only/status validation. Add fresh capture after228 and before229–231 changes `selection_state='SEALED'`. |
| `private.bpay_next_claim_prepare_job_v1(uuid,integer)` | Latest F-05 foundation2018:633–738, not the older1600 full body. Replace only live Candidate/Umbrella SELECT697–701 with the `(run_id,candidate_id)` selection-candidate snapshot. Preserve2018's complete command/member/module checks and earlier-Candidate-command barrier687–693. |
| `private.bpay_next_pin_selected_work_v1(uuid,uuid,bigint,bigint)` | Retained1600:104–465. Replace only gate fetch379 with exact saved snapshot lookup. Preserve early STEP admission/replay/lease machinery, selected revision REVIEW227–257, Source origin qualification265–375 and380–451 validation/pin INSERT. |
| `private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer)` | Latest F-07 ordinary successor1958:11 onward. Replace live Candidate floor SELECT148 (older0360:120) with the same run/Candidate selection-candidate floor. Preserve149–152 invalid-floor refusal and all ordinary applicability, week, case/rule/period, live balance, capacity, hold and cursor branches. |

The currently saved pre-parking registry still designates2236/2308 for consumers. Those are the owners expressly parked by Rule B; importing them would reintroduce the rejected architecture. Move each exact signature's final root/physical/closure to the new small successor in the same reviewed change. Do not overwrite2018's other owners or revert1958 to raw0360/0549. No1610,1620,0280, enroller, pin guard, Source/query producer or Worker change belongs to this hunk.

## Exact schema and types

One new migration adds `private.bpay_next_run_work_query_snapshot`:

```sql
run_id uuid not null
work_id uuid not null
expected_revision_id uuid not null
physical_timesheet_id uuid not null
gate_result jsonb not null
captured_at_utc timestamptz not null
primary key (run_id,work_id)
foreign key (run_id,work_id)
  references private.bpay_next_run_selection(run_id,work_id) on delete restrict
foreign key (work_id,expected_revision_id)
  references private.bpay_next_work_revision(work_id,id) on delete restrict
```

The actual referenced keys are1324:455 and168. A direct physical FK can use `public.timesheets(timesheet_id)` (actual UUID key already used by1324:142), and the already-existing choice key supports a narrow `(run_id,work_id,expected_revision_id)` FK to `bpay_next_work_choice`1241:28 if desired. There is no existing revision/physical three-column unique key: do not name an imaginary composite FK. The capture SELECT and exact PIN lookup bind physical UUID to the selected expected revision. Keep gate_result raw; a nonnull-object CHECK must be null-safe (`jsonb_typeof(gate_result)='object'` with NOT NULL). Preserve all original detailed gate validation in PIN. No fabricated CLEAR/default gate, request blob, new financial counter or history is required.

Add nullable, no-backfill/no-default capture columns to `private.bpay_next_selection_candidate` (OPEN rows exist before seal):

| New column | Exact source/type |
|---|---|
| `target_pay_channel text` | `upper(public.candidates.pay_method)`; Candidate source is NOT NULL text. |
| `target_umbrella_id uuid` | Actual `candidates.umbrella_id`; FK target `public.umbrellas(id)`. Normalize NULL for PAYE as original worker INSERT2018:710 does. |
| `target_umbrella_vat_chargeable boolean` | Actual `umbrellas.vat_chargeable`, NOT NULL boolean on an existing Umbrella. Normalize NULL for PAYE as original2018:711–712 does. |
| `min_take_home_wtd numeric(12,2)` | Actual NOT NULL Candidate floor; same precision as02032026:167/current contract. Preserve later invalid-floor refusal; missing capture is not zero. |

Existing selection-candidate PK is `(run_id,candidate_id)`1324:466. It covers case-only Candidates as well as selected work Candidates. No extra Candidate selection row is manufactured. Existing private ACLs are owner-only; preserve them. New snapshot table must be private, postgres/logical-owner mapped, RLS with no public policies, all PUBLIC/anon/authenticated/service_role privileges revoked, and protected from UPDATE/DELETE/TRUNCATE. A narrow immutable guard on this new table is permitted; no trigger on a Source/query/Candidate/Umbrella table is introduced. Capture columns are written once by fresh seal under its existing run lock; SEALED retry never updates them.

## Fresh capture order and exact physical gate

At1590 after the retained replay and validation, before SEALED:

1. `perform private.weekly_source_pay_query_admit_v2();` Existing55P03 try-lock failure rolls back this transaction. Do not newly acquire admission on SEALED retry.
2. One UPDATE of all `selection_candidate` rows for this run from actual Candidates LEFT JOIN actual Umbrellas fills the four columns, with the original channel normalization. Validate the complete selected Candidate population using the run's existing selected_candidate_count; a missing Candidate/snapshot must refuse, never yield a partially filled accepted run. PAYE has null Umbrella fields. Unsupported channel retains the original named refusal, rather than silently defaulting PAYE.
3. In original selection_no order, join exactly the selected work and its SEALED work choice to its **expected** revision. Apply precisely the existing PIN predicate: `w.work_kind='SOURCE' AND r.source_kind IN ('SOURCE','PROTECTED')`. Use `r.id=choice.expected_revision_id AND r.work_id=w.id`, and snapshot `weekly_source_pay_query_gate_v1(r.physical_timesheet_id)` in a separate statement after admission. Save that physical UUID and exact expected revision with the raw result. Never choose `work.original_timesheet_id`, latest revision or current Timesheet as fallback. The gate itself0308:12–19 delegates to the existing Source facts owner and removes only hours/manual detail arrays.
4. The existing SEALED UPDATE and reply follow. Existing public intake2018:3748–3752 calls seal then enqueue in the same transaction; preserve that original PREPARE agency sequence, without INPUT_VERSION/query commands or another acceptance receipt.

An unapproved/null expected revision retains the existing later selected-work REVIEW path, which occurs before query lookup. Do not silently adopt a newer current revision to populate its snapshot. The required query population is exactly the rows which can reach the Source/protected PIN branch with a selected expected revision; ordinary/expense/adjustment rows do not get invented query evidence.

PIN lookup must match all four identities `(run_id,work_id,expected_revision_id,physical_timesheet_id)` against its actual selected/captured revision. Use ordinary SELECT plus FOUND so absence raises exactly23514 `BPAY_NEXT_PIN_QUERY_SNAPSHOT_MISSING`; INTO STRICT alone would produce an unrelated NO_DATA_FOUND code. There is no live-reader fallback. The unchanged380–451 code still qualifies exact five-key JSON, scope tuple, root physical version, booking/Candidate/client/Contract/week/family identity, defined unavailable shape, blocked/clear disposition and genuine financial authority, then inserts the existing immutable pin. Source disposition/financial completion continues to consume that pin.

## Meaningful source-gate/acceptance concerns

- Assert fresh seal admission/capture lies **after** the SEALED replay return and **before** SEALED, and rollback cannot leave capture without acceptance. Exact retry must reuse original capture even if queries/channel/Umbrella/floor subsequently differ. Conflicting existing counts/key remain original refusals.
- Assert the selected Source/protected population is complete and physical/expected-revision exact. An INNER JOIN must not omit a valid expected revision; missing snapshot must fail explicitly at PIN. Test ordinary, protected, unavailable/blocked/clear and case-only capture contracts when permitted, without inventing fixtures or observations now.
- Assert CLAIM no longer joins current Candidates/Umbrellas for its three captured values, and1958's floor no longer reads current Candidate. Neither read may coalesce missing capture into PAYE/false/zero. Existing same-worker replay and per-Candidate earlier-command barrier remain.
- Assert the selected revision change still returns whole-Candidate REVIEW before any snapshot lookup/hold; it neither reprices the Draft nor pays an older figure after source authority changes. Preserve every existing origin validator and380–451 gate parser; adding a snapshot does not weaken physical provenance.
- The scalar Source gate is STABLE. Perform it as a distinct fresh SQL statement after the admitted transaction decision, rather than hoisting it into the earlier selection discovery statement. captured_at_utc is a record timestamp, not an as-of history reader or browser-clock proof. Do not claim that all producer paths use a lock merely from the admission helper's existence; Rule B records the actual committed raw gate observed at this seal and forbids producer rewrites.
- Current Worker `safeError`350–375 already makes raw55P03 a retryable503 dependency error through its final branch. It is not the structured QUERY_BUSY409 unless SQL supplies that existing typed envelope. Preserve Worker bytes and identical retry body; do not claim a specific QUERY_BUSY label for the raw exception. Dormant ACCEPTED_* pages must remain unused by Office.
- Static source checks should compare exact retained bodies/hunks and registry final ownership. Local schema compilation and contract/ACL coupling may prove syntax/schema only; they do not prove concurrent before/after click behavior or performance. No timings/counts are asserted by this note.

Reviewed baseline SHA256:1590 `47a0df6a06bd7816b0102c1aa5fca0bddd97ebc08a0a04d786254db3179967b3`;1600 `94cd02fa3ffeb152b0281a840c08249ad9c3e0d26e2525d5fc3d769da4dd4de5`;2018 `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b`;1958 `c6f7d426fa60d3c6cf5db4d5c386aeb8111f974e455bd475a10dcfecf0e5851b`; current-contract type evidence `ddfbb76030233c0eb395c40162e8f19abd34460296ff77305905cbd2bcd0ee9c`.

Rule B/BD019 supersedes the old resolver/producer requirements in the F-25 card/checklist. This design does not revisit any parked work or alter full-F-25 status; the root owns the authorised small implementation and its final review.
