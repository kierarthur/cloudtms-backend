# WB-18 export SQL source review

Verdict: SOURCE PASS for the finite reviewed SQL scope; no open SQL finding. Both bounded-order and accepted-search findings are closed in the fresh2128 source. This review covers only the current2128 SQL and supporting index. Worker/Office export completion, actual CSV bytes, app journeys and timing are not certified here.

## Snapshot and scope

Read-only source comparison against private common2105 and public reader1946. No product changes, SQL execution, tests, fixtures, data or hosted operations. The registry currently places both common/public final physical/root definitions at2128. The private EXPORT_ROWS wrapper remains the existing fixed wrapper; no new RPC, command, digest or choice protocol is introduced. The accepted choice descriptor belongs only to Worker transport, as directed by the owner.

| Source | Raw SHA-256 |
|---|---|
| `supabase/repeatable/10102026_2128_banking_pay_next_workbench_export_v1.sql` | `bf91fea98d86269da06e7d9050c46d85bd35d4a63831c1e00efa65c10f236ba4` |
| `supabase/migrations/10102026_2128_banking_pay_next_workbench_export_seek.sql` | `a7b4ce1897e0dbd065853fe330e1e16bb052ef0216b4506a85a3d0890cf8428f` |
| `docs/bpay-next/final-definer-closures.json` | `6906e4d0bc40bdf1c95950aed7f8ab4495b43c2c02292688ff232873fa21a22f` |

## Findings and closures

1. **F1 CLOSED — bound before presentation order.** Earlier draft used `row_number()` over the whole source walk before LIMIT. Fresh lines65–66 now materialize only native item identity, row key, payment label and eligibility metadata under `ORDER BY ... LIMIT p_limit+1`. Lines127–130 sort only these at-most101 identities using the exact typed seek tuple with `seed.row_key`. Numeric, date, boolean, C-collated text and UUID ordering remain native; there is no JSON numeric-string ordering or unbounded ordinal operation.
2. **F2 CLOSED — unsupported free-text export scope now refuses.** The previous early branch ignored public-accepted search text. Fresh lines26–27 explicitly reject nonempty export search with `22023/BPAY_NEXT_WORKBENCH_REQUEST_INVALID` before constructing the page. The actual export controls supply normalized Candidate/Client/channel filters and no free-text filter. A caller cannot now receive an apparently successful unfiltered export for a nonempty-search request. Existing other-kind literal-prefix search remains unchanged.

## Verified finite behavior

- Twenty literal C_/G_ sort variants share matching `v_tuple`, ORDER BY, serialized key and typed seek. Every tuple ends in native item UUID; Candidate variants also retain Candidate UUID, and group variants retain presentation kind/UUID. Both directions apply consistently to all tuple fields. Exact cursor membership is checked within the same scope before strict seek. Public closed validation covers each key shape and decimal/date/UUID types; malformed or stale cursors receive existing typed refusal.
- Candidate ordering uses maintained native Candidate summary/metadata sort values; group ordering uses maintained ALL presentation rows. No bank values enter export data. Unknown amounts remain NULL with their factual flags, rather than fabricated zero. Zero placeholders are used only inside explicitly flagged native sort keys.
- Item/group IDs are selected before factual joins. Enrichment joins the exact item revision/work, approved line/component, approved segment/key and shift detail PKs. The Timesheet ID, Role and Band therefore come from that retained item revision, rather than the current navigation revision. Native Work/Case display structure supplies one compatible group display row per item: WORK, OVERPAYMENT CASE, or exact standalone ROW component. The ordinary native Case fallback GROUP_EVIDENCE row is emitted only when no component rows exist; this does not create a CASE-plus-ROW duplication under the current owner.
- Export `Amount` follows the existing factual WB15 display authority: unknown→NULL; selected→known selected amount; RESERVED→actual reserved amount; READY PAYMENT→included amount; WAITING GROSS_ADD PAYMENT→gross due; otherwise native nominal. No ratio, NET inference or new allocation is added. A Case deduction nominal remains its native outstanding balance and must not be treated by Worker as this Draft's allocated payroll deduction. Mixed-stage Included-total application proof remains a separate actual caller/join requirement.
- The projected source is sufficient for the approved19-column mapping: Included is Worker local choice/eligibility; native member/portion facts give Line/Breakdown type; retained Candidate ref/name and Client give display; week/work date and channel give native labels; Amount is the SQL fact above; exact physical Timesheet, component/member identities give provenance; immutable revision Role/Band and actual shift Start/Finish give detail; native detail label/evidence ref supplies Detail; item UUID supplies Preview row ID. **Break** has no scalar in this projection and must remain honestly empty/documented rather than inferred from duration minus worked time. No field requires joining a bank account or rewriting money owners.
- The new index is only `(group_key,candidate_id,case_component_id,id)` on the existing private maintained item table. It supplies the native group/component walk without changing schema economics, producers or frozen authority. Actual planner/timing evidence is outside this read-only review.
- Page limit1–100, `limit+1` completion, whole-row byte refusal110000 and public envelope cap120000 are preserved. Cursor/context carries original agency/actor/epoch/date/workbench/adoption/scope. No partial page masquerades as complete on byte overflow.
- Static function comparison finds only the early EXPORT_ROWS branch (plus insignificant blank-line changes) in private common. Public changes are finite export sort/key validation; all other-kind dispatches, admin/service authority, module/adoption readiness, existing request shape, exception envelopes, owner/revoke/service grant and notification are retained.

## Remaining item joins

This is not whole WB-18 acceptance. Worker must freeze and validate the complete local choice descriptor, apply hierarchical Included without browser money reconstruction, stream all bounded native pages under one exact context and reject late/truncated body completion. Office must publish a download only after true whole-stream success, preserve selection/actor/frame and provide both header and exact Candidate controls. No in-app or1000-row CSV journey was run in this review.

## Fresh closure receipt

The last source reread verified the explicit nonempty-search refusal and typed bounded final order in raw SQL SHA `bf91fea98d86269da06e7d9050c46d85bd35d4a63831c1e00efa65c10f236ba4`. This supersedes the earlier638a5450 snapshot with F2 open; the fix is a two-line comment/refusal addition before export sort selection. No execution or whole-item app claim was added.
