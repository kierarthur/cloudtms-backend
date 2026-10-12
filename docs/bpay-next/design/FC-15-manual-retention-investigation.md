# FC-15 manual-retention investigation

Status: READ-ONLY INVESTIGATION; implementation and joined acceptance NOT_RUN. Root owns all product changes. This note records the confirmed boundary first; the exact Workbench/effect points below will be completed by source inspection.

## Controlling conflict

The full owner message `cloudtms-backend/codex_outputs/claude-stage2-continuation-20260925/MESSAGE_TO_BANKING_PAY_BUILDER_NOW_FC15_SP10.md` requires every genuinely paid reduction, across ordinary and Source origins and pay channels, to retain one net-difference liability. Where automatic collection is unavailable, it requires a manual collection obligation rather than dropping the amount or substituting REVIEW. The approved FC-15 card (`03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:1346,1356–1362`) limits this card's automatic implementation to PAYE and leaves ordinary Umbrella to CM13a and Source Work to SP-09. Record this coverage conflict as BLOCKED; do not widen automatic financial consumers silently. Manual retention can preserve the new owner requirement without enabling either later automatic owner.

The card's `:1349` withdrawal rule remains material: withdrawal is not an authorised zero. A removed shift in a genuine newly authorised Source approval must be distinguished from a withdrawal or unknown current approval. Neither an absent current line nor a revoked head alone proves a paid reduction.

## Confirmed native storage boundary

`supabase/migrations/04102026_1519_banking_pay_next_work_collection.sql:9–51` already has one unique `(work_id, component_key)` collection owner, immutable actual-paid effect/run-line/revision/approved-line/transfer anchors, and an optional unique Case/component pair. Reuse this owner. Do not create a second Manual Debt, negative Work, command, or parallel liability when an automatic Case already exists.

The current schema fixes `original_source_pay_channel` to PAYE and requires TAXABLE on qualified/Case-linked rows. A manual mode therefore needs an explicit finite mode/reason/outstanding representation and null-safe qualification rules; a missing original tax is unknown, not invented TAXABLE/NON_TAXABLE. Preserve recovered, written-off, reserved and actual-paid facts. The final financial owners remain unchanged while automatic consumption is blocked.

## Exact current source owners to inspect

- Position application: `12102026_0016_banking_pay_next_stale_snooze_publication_v1.sql`, `private.bpay_next_apply_position_page_v1(uuid,uuid,bigint,text,text,integer)`, line 100. Current LINE and authorised MISSING paths call the existing collection reconciler at lines 200 and 253.
- Collection reconciliation/guard: `11102026_0814_banking_pay_next_preparation_discard_v1.sql`, `private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid)` at 885 and collection guard at 1208.
- Origin: `10102026_1236_banking_pay_next_segment_authority_final_v1.sql`, `private.bpay_next_work_collection_origin_v1(uuid,uuid,uuid)` at 4461.
- Work projection: `11102026_2223_banking_pay_next_shift_query_exclusion_v1.sql`, `private.bpay_next_workbench_refresh_work_core_v1(uuid)` at 1536; bounded refresh wrappers remain `10102026_1438_banking_pay_next_workbench_origin_projection_v1.sql:318,354`.
- Case projection: `12102026_0207_banking_pay_next_origin_wait_recovery_v1.sql`, `private.bpay_next_workbench_refresh_case_v1(uuid)`.

The core derives recovery from maintained Position realised minus approved, while its selectable offer derives positive approved minus realised minus held. Manual retention can reuse its distinct informational/action recovery portion. The concrete seams and limitations follow.

## Completed source findings and smallest supporting changes

### Native Workbench visibility

1. Current `2223:1679–1688` derives the maintained native A/C/H split; `:1793–1796` already inserts `portion_kind='RECOVERY'`, `direction='INFORMATIONAL'`, `selectable=false`, normally ACTION with `RECOVERY_MANAGED`. Its `nominal_source_ex_vat` is still the positive payment value (zero for a pure reduction). Thus today's primary amount is zero, and the wording promises a finance Case even if no ledger/Case exists. A manual ledger point can qualify a finite `MANUAL_COLLECTION` reason and its exact retained informational amount. Do not change positive payment, accepted selection or RESERVED branches.
2. `11102026_0113...:585–629` already emits both nominal and recovery source amounts for ACTION_REQUIRED; ISSUE_DETAIL/GROUP_LINES have the same existing operands. Reason is bounded text rather than a closed database enum (`10102026_1252...summary.sql:105`). Worker/FE `code` validation already supports a named reason. No new read kind, route, command, financial field or table is needed merely to explain the manual liability. BE and FE must agree on the exact new reason and its unselectable/informational qualification.
3. FE `js/bpay-next-workbench.js:684–710,1159,1245` needs plain “Manual collection” and the native manual amount as the primary value, without a repeated identical Recovery subline or generic “Payment needs review” fallback. It must not calculate the liability. AMOUNT ordering already uses native `nominal_source_ex_vat` in both the SQL reader (`0113:848–849,872`) and closed Worker/FE key checks; make the stored display operand agree with the amount shown.
4. Candidate WORK groups require one additional existing display-owner seam if they must show the manual amount there: `private.bpay_next_workbench_refresh_group_display_v1(text)`, current `10102026_1946...:60`, excludes INFORMATIONAL from `group_amount`. A narrow manual informational display case is needed; changing nominal alone leaves the group amount at zero. Its existing server AMOUNT keyset then follows the same maintained value. Do not make an informational item PAYMENT/DEDUCTION just to display it.
5. The item CHECKs (`10102026_1252...summary.sql:137–143,147,170`) enforce Work items without Case IDs and recovery items as informational/nonselectable. Keep these identities. A linked Case is a relation through the one collection owner, not a second Work liability.

Unsupported Source member qualification can make the original `amount_known=false` (`2223:1668,1677–1678`); the original RECOVERY branch then does not run. An exact ledger-backed manual informational portion therefore needs its own positive native qualification rather than relying only on that branch. Preserve the ordinary unbound Evidence/positive-offer guards and unknown summary currentness. A known manually retained liability is not permission to certify a positive offer, source attribution, missing original tax, or a zero amount.

### One liability, existing Case, retained reservations

The native contribution owner separately records Work recovery source amount and Case deduction balance; it does not add informational nominal to Ready pay (`private.bpay_next_workbench_group_contribution_v1(text)`, current `10102026_1332...`; original formula in `10102026_1252...:70–80`). Preserve that distinction. Do not introduce a second manual monetary contribution alongside the same Case balance. Choose one visible liability amount owner for a Case-linked manual collection, with the other row a factual reference to the same owner; do not present them as two collectible debts.

Current Case projection `0207:477–509,541–547` defaults REVIEW components to `CASE_REVIEW_REQUIRED`; a linked manual collection must not inherit this generic review substitute. The only permissible new future-offer annotation is the exact collection mode/reason on its unaccepted AVAILABLE portion. Preserve its accepted and RESERVED `v_base` (`:565–588`). Disable new automatic selection/estimate/DERIVE for MANUAL even if a stale earlier `qualification_state='READY'` survives; mode must be part of native automatic origin qualification. Never rewrite or release existing captured holds merely to switch presentation. Total outstanding and currently unreserved manually collectible amount must remain distinct when a real ACTIVE Case hold exists. Keep the existing Case/component pair immutable.

### Refresh and lock boundary

FC-15 card `:1363` requires Work and Case projection in the same actual owner transaction on create/principal change. `0814` reconciliation currently ends with the ledger observation update (`:1196–1204`), without either refresh. Native `refresh_work_v1` (`1438:318–350`) refreshes linked Cases only on a Work WAIT transition, so it is insufficient for an ordinary amount/mode change. Reuse `refresh_many_v1(uuid[],uuid[])` (`1438:354–408`) with the exact changed Work and optional linked Case; its envelope is 100 identities/16KiB and its locks are Candidate→Work→Case. When the parent already owns a Case lock, acquire its exact Work before that lock or use the parent’s already-proved canonical prelocks; do not add a late Case→Work inversion. The existing ORIGIN task fallback remains the bounded path for a large genuine related set. Bump/refresh only actual changed observations and use exact replay, not a second side-effect on a replayed page.

### Actual paid anchor and finite lookup

`0814:1034–1058` shows the exact retention hole: no existing ledger + POSITION_APPLY (`p_effect_id=NULL`) returns at `:1039`; unsupported original settlement also returns. The current INSERT guard at `:1240–1243` only admits a settlement job whose command equals the original effect operation. Recording an already-paid unsupported reduction at its new authorised POSITION_APPLY therefore needs one explicit native INSERT branch. It must prove the current owned POSITION_APPLY publication/revision/phase and Position observation, independently prove the old positive PAYROLL_SETTLED effect, its REALISED hold, exact captured run-line/approved-line/revision, actual original transfer and settlement owner, and preserve original identities on every UPDATE. A new job cannot be falsely required to equal the old settlement command. The old effect remains immutable payment evidence.

The existing index is `bpay_next_effect_work_history_idx(work_id,component_key,occurred_at_utc,id)` (`03102026_1324...core.sql:1011–1012`). It gives a scoped seek, but there is no finite number-of-effects theorem. For a bounded deterministic first actual paid anchor, the smallest genuinely useful added index is that tuple with a literal partial predicate for positive `PAYROLL_SETTLED`, followed by exact point joins; do not scan and aggregate the whole history or all agency effects. Native effect identity uniqueness is only `(operation_id,operation_item_id,effect_kind)` (`core.sql:389–415`), so repeated genuine payments per component are legitimate.

One selected anchor is provenance, not the total liability or proof that all paid effects share a tax/channel/classification. Current native Position (or a positively qualified segment/source book) owns the realised total. Preserve existing `paid_basis_qualified=false` for incomplete/mixed historical evidence; current guard forbids promoting it (`0814:1230`). MANUAL may retain a known source amount with tax NULL. Actual-paid reference quantities/amounts must not be substituted for the maintained total, and a bank cash return is not a paid-source reversal (existing reconciler `:1162–1168`). Missing real paid evidence is an explicit qualification gap, never a fabricated effect/backfill or a silently absent liability.

### Authorised removal, original classification and OR47

Position Apply `0016:230–253` sets a formerly present component to approved zero only under the actual new publication revision and then reconciles it. Generic withdrawal uses the distinct WITHDRAWN projection (`2223:1616–1654`) with retained native A/C/H and zero new recovery, and Work WAIT explicitly means WITHDRAWN plus realised money (`10102026_1332...:5–13`). Preserve these separate semantics.

`0814:1083–1091` explicitly rejects interpreting an absent DAY key as removal when a changed AGGREGATE/date representation may retain the entitlement. Its existing complete segment basis (`1236:4100–4139`) is restricted to ORDINARY/PAYE/TAXABLE and validates all segment totals against the parent. Do not broaden it to unsupported Source or Umbrella merely to obtain a numeric manual amount. Use exact existing authorised same-identity/qualified book/segment evidence where present; unknown representation or attribution remains explicit. This is a genuine manual-retention qualification boundary, not permission to restore parked producer/resolver work or infer shift removal from an absent key.

Current Work members `10102026_1321...:29–62` include Position-only missing components, but original kind/tax fallback is only through an existing Source binding classification line. Ordinary removed lines may therefore have current line/kind/tax NULL. The exact collection original paid approved-line can supply informational original kind/tax when the current line is absent. Do not assign its historical approved_line_id to the current revision FK, overwrite actual current NULL tax, or fabricate the original classification. OR47 selected/captured segment attribution and query exclusions remain unchanged; future Source representation/correction machinery remains its named owner.

## Scope verdict

The smallest support is the existing collection row’s explicit AUTOMATIC/MANUAL retention mode and exact amount/reason, null-safe immutable paid-anchor admissions, a finite native point index if needed, and the named Work/Case/display/issue seams above. It does not enable new automatic Umbrella/Source consumers. The owner expressly permits manual collection: qualified PAYE-only automatic collection plus actual manual retained liabilities can satisfy the all-origin requirement. There is no all-origin automatic acceptance requirement. Record the earlier scope conflict explicitly, but only genuinely unbound amount/removal authority remains BLOCKED; do not label the whole card blocked merely because later automatic consumers remain assigned to CM13a/SP-09. Runtime, business data, joined application and performance were not exercised.

## Reviewed saved source hashes (SHA-256)

| Saved file | SHA-256 |
|---|---|
| `11102026_2223_banking_pay_next_shift_query_exclusion_v1.sql` | `B3D7B39A31D35F573DBCC4EA21E9FC0015C801D09A02F1508FE8C9B3ED4535DD` |
| `11102026_0814_banking_pay_next_preparation_discard_v1.sql` | `29F3099EBFB8A7E11D8C23A8996C5351EB9330316D8BDB6FD6D3A5136D3F379C` |
| `12102026_0016_banking_pay_next_stale_snooze_publication_v1.sql` | `2D9822DA9881179DACE5A2FF97C65FCAAF1DFBAAD920337A4701133D8FE487AE` |
| `10102026_1321_banking_pay_next_workbench_removed_source_classification_v1.sql` | `4F07A5AB56FD01AA40CCDB9163F3458C66A838AF8E056F35987BE2C24CB70542` |
| `10102026_1236_banking_pay_next_segment_authority_final_v1.sql` | `F74EA457BF77F0D85CE7F3F21089BDECF9D9D9121D4005850DE430D50AA7379D` |
| `11102026_0113_banking_pay_next_case_choice_readers_v1.sql` | `7CBC3FB757E2FDB4493D50815E5F0C558C4882DCB5028B49F0F69D7FBA9FB313` |
| `12102026_0207_banking_pay_next_origin_wait_recovery_v1.sql` | `15518A516D7E4E1264E44B949EE432B694418DAAFD9DF9FDAF6FED22ABCA9F27` |
| `10102026_1438_banking_pay_next_workbench_origin_projection_v1.sql` | `B8D445C4A4CFC02978E8AC77A159350B0E1978AAFF9A1172DA8E7E157A6414F5` |
| FE `js/bpay-next-workbench.js` | `60B716265FBB2F775A4869019F8C5B9020C9F32F838777F800CE58F6EB68A77B` |
