# WB-36 Source/protected segment persistence

Read-only source investigation, 10 October 2026. No product edits, SQL, runtime, tests or fixtures were executed. Rule B and the original Source publication/handoff remain the boundary. Only the named Banking Pay Source/protected detail producers need segment persistence; no `weekly_source_*` owner replacement or Pay Query/protocol change is proposed.

## Exact current owners

| Function | Current final physical source | Native detail seam |
|---|---|---|
| `private.bpay_next_stage_source_current_v1(uuid,uuid,uuid)` | `03102026_1550_banking_pay_next_source_stage_v1.sql` | WORKED_TIME detail INSERT 507–522; first TSFIN matching 456–466, chosen HEAD matching 468–487 |
| `private.bpay_next_capture_chosen_source_detail_v1(uuid,uuid,bigint,jsonb,jsonb,uuid)` | `03102026_1548_banking_pay_next_source_chosen_detail_v1.sql` | Actual weekly event qualification 92–103, flattening into chosen detail 115–136; exact copy-head branch 55 onwards |
| `private.bpay_next_capture_source_pair_before_detail_v1(uuid,bigint,uuid[],uuid[])` | same 1548 | Frozen prior Source line/shift qualification 570–669; reviewed ordinary target TSFIN detail 670–766 |
| `private.bpay_next_stage_protected_source_v1(uuid,uuid,uuid,jsonb)` | `04102026_0345_banking_pay_next_protected_source_v1.sql` | Exact approved generation/vector, genuine `work_event_id` component identity 379–397, shift INSERT 399–407 |

These are the current registered final owners. The new 1100 segment authority is being authored concurrently; this note does not certify its full body or claim whole WB-36 acceptance.

## Source key operands

In **first snapshot** stage1550, `v_segment` is the one exact raw `v_tf.invoice_breakdown_json->'segments'` entry whose `segment_id` equals the canonical component's `segment_id`. Five bucket equalities and work date are already checked before INSERT. The actual weekly work-event operand is:

```sql
nullif(v_segment #>> '{weekly_source,work_event_id}', '')
```

When present as a genuine UUID, persist its canonical UUID text as `segment_key`. When this original segment has no actual work event, keep NULL/WHOLE; do not manufacture one from component UUID, same date, display ordinal, fingerprint, or Source event/head ID. Original first-snapshot evidence validation must remain in place. A malformed claimed event must refuse rather than silently fall back to another identity.

In a **normal chosen HEAD**, 1548 already requires nested `weekly_source.work_event_id` to equal canonical `component_member_identity` and stores it as the **top-level** `work_event_id` JSON member. The exact operand is:

```sql
nullif(v_segment ->> 'work_event_id', '')
```

The corresponding `calculation_fingerprint` is compared with canonical `segment_stable_key` as evidence, not shift identity. Stage1550 still verifies the exact head/component/bundle/revision, component digest, chosen-detail digest and date. Preserve all of those checks and its existing accepted clocks/breaks/rates; do not fetch current Source rows.

### Existing pair-before/copy shapes are different

1548's `from_frozen_pair` branch qualifies the exact prior Source revision/approved line/rates and **one** shift detail, then saves its actual date, local/UTC clocks, overnight, breaks and rates. It currently retains `prior_approved_line_id`, `prior_revision_id`, `prior_work_id`, prior Source/TSFIN/root/version/family IDs, but does **not** save the new shift `segment_key`. A successor can append the exact already-frozen `v_shift.segment_key` to **newly created** pair detail (for example, a typed `segment_key` member); Source stage must read that member only in that exact copied shape. Do not relabel an ordinary stable key as a manufactured work-event UUID.

The reviewed, not-yet-authorised B branch uses the **actual ordinary raw TSFIN segment** whose `segment_id` equals `v_component.segment_id`, with exactly one match. It explicitly says no Final/movement/work-event/Bank identity is manufactured. Its chosen detail already retains `before_segment_id`. This is a genuine ordinary stable-key operand for this narrow shape; it is not a nested weekly work event. A new explicit `segment_key` can retain `v_segment->>'segment_id'` at this exact creation seam, or stage can use the existing `before_segment_id` only with `from_reviewed_target_tsfin=true` and the already-saved exact scope. No `work_event_id` fallback should cover this branch.

The existing copy-head branch copies `v_existing.detail_json` byte-for-byte with its original digest. Preserve that behavior. Do not enrich an old chosen HEAD's JSON from today's Source or Candidate state. A retained old detail without a genuine saved/native immutable segment binding remains indivisible; it must not acquire a guessed key.

## Narrow stage1550 INSERT additions

Keep existing `SOURCE:<component UUID>` approved-line parents, `v_hours`, `v_component_amount`, signed HEAD bucket hours, dates, clocks, rates and break logic. Append only:

```sql
-- New target columns, in matching INSERT/VALUES order:
segment_key, segment_pay_ex_vat, pay_excluded,
hours_day, hours_night, hours_sat, hours_sun, hours_bh

-- Existing canonical operands (key is the branch-qualified operand above):
v_segment_key,
v_component_amount,
(v_component ->> 'exclude_from_pay')::boolean,
coalesce((v_component ->> 'hours_day')::numeric, 0),
coalesce((v_component ->> 'hours_night')::numeric, 0),
coalesce((v_component ->> 'hours_sat')::numeric, 0),
coalesce((v_component ->> 'hours_sun')::numeric, 0),
coalesce((v_component ->> 'hours_bh')::numeric, 0)
```

The exact component, rather than chosen detail JSON, owns these five hours and independently authoritative pay amount. The chosen detail owns clocks/rates/presentation evidence. Stage computes `v_component_amount=0` for actual `exclude_from_pay`, otherwise canonical `pay_ex_vat`; do not calculate this amount from hours × rates or substitute an earlier TSFIN amount for a committed HEAD.

**Current 1100 reconciliation issue:** stage1550's approved-line `approved_quantity` remains raw `v_hours` even for `exclude_from_pay=true`; only source gross becomes zero. Ordinary1009's day parent has payable-hours semantics. The newly authored segment seal presently sums `CASE WHEN pay_excluded THEN 0 ELSE approved_hours END` and compares it to every WORK parent quantity. That rejects a genuine excluded Source component if its raw signed hours are nonzero. Preserve Source raw bucket/approved hours and reconcile each parent's actual original quantity semantics; do not zero Source hours to satisfy the new seal. Signed negative HEAD hours must remain signed.

## Protected target vector INSERT

0345's protected stage reads the exact saved approved generation `complete_next_vector_json->'components'`; public owner validates original work-event UUID and clocks before this call. Each WORK component builds its existing Source component from that `work_event_id`. Append to its shift INSERT:

```sql
segment_key, segment_pay_ex_vat, pay_excluded,
hours_day, hours_night, hours_sat, hours_sun, hours_bh

(v_component ->> 'work_event_id')::uuid::text,
v_amount, false,
(v_component #>> '{hours,day}')::numeric,
(v_component #>> '{hours,night}')::numeric,
(v_component #>> '{hours,sat}')::numeric,
(v_component #>> '{hours,sun}')::numeric,
(v_component #>> '{hours,bh}')::numeric
```

Its genuine target vector does not carry an exclusion flag; do not introduce a request input or infer one. Preserve WORK versus PROTECTED_WORK classification, component ID, original pay amount, approved quantity and per-bucket rate insertion. The segment seal must explicitly include supported PROTECTED_WORK in relevant checks, rather than assume every keyed child is ordinary WORK. The current copied-clock fields are start/end instants, local start/end, overnight, date, approved hours and original breaks; all must remain exact, including NULL UTC when not supplied.

## Source company is not captured by these original stages

Current stage1550 captures channel from first TSFIN `pay_method`, falling back to `contracts.pay_method_snapshot`; committed HEAD uses the contract channel. Its revision INSERT 378–393 does not include `source_umbrella_id`. Protected stage0345 also saves only `upper(v_contract.pay_method_snapshot)` and no company. The native `timesheets_financials` structural table has `pay_method`, worker policy/rate snapshots and VAT snapshots, but no dedicated source `umbrella_id` column. No subsequent migration providing such a TF column was found in this narrow census.

Ordinary1009 **does** populate the new revision `source_umbrella_id` using Candidate state under its current approval lock and explicit channel validation. That ordinary implementation is not evidence of an old Source company. A copied exact immutable prior revision may carry a genuine company once its actual original owner saved it; copy that value only under the exact predecessor binding. The current Source/protected stages have no native old-company operand to add. Leave it unknown or refuse a command requiring positive company evidence; do not look up today's Candidate and label it the old Source company, and do not open a Source producer/protocol rewrite for this task.

## Source hashes and limits

- Stage1550: `5388ce61af1cbd27acfdd4eeb69cc9654247d818e18745200bdd9557e83584b2`.
- Chosen/pair detail1548: `c8b8c83d183eb392d7339f8d9cce92a8adadc401d8469fa74082ad1e9921a819`.
- Protected0345: `0ea79ae265cffb2370926e1728f483b8846e7539ffcac428f5d5fa557a56ac44`.

Original owner ACL/security and all replay/publication boundaries must be copied unchanged into any named successor. Full item acceptance and application scenarios remain NOT_RUN. This is a finite source-operand map, not a new command, receipt or proof framework.

## Amendment: finite captured-company writer census

Follow-up source-only census of the actual structural table and native snapshot writers found **no positive captured Source-company authority** for these Source/protected stages:

- Native `public.contracts` structural definition (2123–2173) has `pay_method_snapshot` and rate/schedule settings but **no umbrella_id/company field**. No later contract migration adding that field was found in this census. Native TF structural definition (4364–4465) likewise has channel/VAT/policy/rate JSON, not a company identity.
- `public.tsfin_load_context_batch` returns current `out_candidate`/`out_umbrella` by joining Candidate.umbrella_id (36:97/226–227). That calculation context is not an immutable original-source company receipt, and those returned objects are not themselves stored by the TF writer.
- `public.tsfin_write_current_snapshot_single_bounded` (37:671–672) stores supplied `policy_snapshot_json` and `rate_source_refs_json`; its policy normalization around 491–516 derives ERNI/channel terms. It does not establish or synthesize an original company ID. The ability to pass opaque extra JSON is not proof that actual Source owners capture a typed company field.
- Actual Source first-authorisation projection's `v_expected_rate_source_refs` (1534 ordinary projection:2831–2844) records mode, root, Final revision and exact Final/client/unit/expense/active-segment manifests. Its policy is the original frozen settings `values` without audit `resolved_at_utc` (3200); the existing settings owner has no company authority. Managed-zero refs (4571–4578) contain cycle/group/family/mode/profile/target/root/policy SHA. Neither construction contains a Source company.
- Actual Worker `loadPolicy` (index:165240) records effective settings values/fingerprint/relevant date/source/applicability. Its current contract-based TF `rate_source_refs_json` objects contain only mode and contract ID. Neither is an immutable company capture.
- Correction policy resolver's actual TF payload (01:1059–1076) records pay method, VAT applicability/chargeability/rates and evidence references. Its Candidate→Umbrella lookup at 968–972 only determines current VAT applicability; the immutable payload does not preserve the company ID. VAT boolean/rate cannot identify a company.
- Protected action context appears to offer `provider.umbrella_id` and `provider.target_umbrella_id` (1534 protected orchestration:1085/1161–1164), but obtains both using `to_jsonb(v_contract)->>'umbrella_id'`. In the actual native contract row described above that member is absent, so the fields are NULL. Its provider hash cannot convert absence into positive company evidence. This is not a usable captured-company operand.

Therefore Source UMBRELLA `source_umbrella_id=NULL` cannot be treated as a complete worker-financial basis or proof that company identity is unchanged. Exact copied revision company is reusable only when its actual immutable original owner already captured it. Otherwise this is a real missing authority: record/refuse that unsupported company-dependent scope rather than labeling today's Candidate company as original Source or inventing an old-company receipt. This finite investigation does not authorize a Source producer rewrite.

Additional inspected source hashes: TF writer37 `9f82588a4c16e4cbbf7285560b5ee1701b021eee51c854658d92c50ed08650aa`; Source projection1534 `665ddd27108a867f92e1f04dd346130736df27031a577f108b7f9c1bf6807900`; correction-policy01 `d02fbe1e53489a97a9f7eb0c5b538c5afa19290da65a1d8b9a995fb2c6ac59b7`; protected orchestration1534 `9d29792128720bb20ae4155d0ce78f43ee100ddf885ac2fab9283fa533f78d19`; structural baseline `aec4d8985fed34c08cfd61521db4ad27b273a85e90251ccd8007eee0489b7730`.

## Independent bounded 1100 producer review

Reviewed the three complete extracted successor bodies against their current complete predecessor bodies, and read the exact new INSERT/metadata blocks. Snapshot reviewed: `10102026_1100_banking_pay_next_segment_authority_v1.sql`, SHA `6fbaa883945a63835d7fa4ce94bccc7f73e9cce29d16d6e07729844cd3bb0fe9`. This review is limited to Source/protected producers and their segment-parent reconciliation, not the entire concurrently authored WB-36 batch.

The successor additions preserve the original business bodies, Source component parent IDs, independently authoritative canonical amounts and signed bucket quantities, existing clocks/breaks/rates, replay, security and private ACL. First snapshots use the actual nested weekly event; ordinary copied shape uses actual before_segment_id; newly constructed frozen-pair detail retains `v_shift.segment_key`; normal chosen HEAD uses flattened work_event_id. Protected target uses actual approved-vector work_event_id/pay/hours and false exclusion. Copy-head still copies exact old JSON unchanged. The revised seal sums Source raw hours while excluding ordinary excluded hours and includes WORK/PROTECTED_WORK, closing the earlier finite findings.

**H01 — open in these reviewed bytes:** the normal event-key UUID validation/canonicalization has SQL NULL semantics:

```sql
if v_segment_key is not null and not (
  v_segment->'from_frozen_pair'='true'::jsonb
  or v_segment->'from_reviewed_target_tsfin'='true'::jsonb
) then
  v_segment_key:=v_segment_key::uuid::text;
end if;
```

For normal first-snapshot/chosen-detail objects both optional flags are absent; the condition is NULL and PL/pgSQL skips the cast. Thus the intended event UUID validation and canonical stable-key representation are not enforced. Small correction: require each optional flag `IS DISTINCT FROM 'true'::jsonb` instead of `NOT (NULL OR NULL)`, preserving both exact non-UUID copied shapes. No broad parser, owner or protocol change is needed. This section is source FINDING pending fresh corrected bytes; no execution claim.

H01 fresh closure: re-read later1100 SHA `90af931bb262bdc6417fe280fe274636b39712423901aae34660a95d61376bc8`. Both optional shape flags now use `IS DISTINCT FROM 'true'::jsonb`; the genuine normal event UUID validation/canonicalization executes when the flags are absent. H01 is resolved. Source-company absence remains a separate real authority gap; this is not whole WB-36 acceptance.

## Scope correction — BD056 supersedes the company admission conclusion

The finite census correctly found no frozen Source-company operand. WB36 card1300 and R2 §1 do not require one: genuine native Source work/event identity, channel, tax, quantities, used worker rates and gross remain the required snooze basis. Actual legacy1235_43 has no company admission gate. Therefore absence is retained honestly as NULL, and an optional genuinely captured company remains in the basis where supplied; it cannot justify refusing otherwise valid Source UMBRELLA snooze. The draft SOURCE_COMPANY_UNAVAILABLE gate has been removed. No Source owner gains a new field, Candidate guess or protocol. Earlier company-dependent recommendations above are superseded only for this admission conclusion; the original source-operand census and stable-key mappings remain valid.
