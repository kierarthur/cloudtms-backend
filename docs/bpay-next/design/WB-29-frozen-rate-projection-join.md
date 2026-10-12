# WB-29 frozen rate/advice projection join

Date: 13 October 2026. Read-only design recommendation for the two named current public readers. Root owns product implementation. This is the narrow projection join to the authored 2357 captured composer and 0005 capture/hold/recorder successors; neither runtime acceptance nor whole WB-29 completion is claimed. No product edit, SQL/RPC, test, fixture, browser or hosted action was performed.

## Exact owners and insertion seams

| Owner | Current physical parent and seam |
|---|---|
| `public.bpay_next_frozen_detail_page_v1(uuid,text,uuid,uuid,uuid,integer,integer,text,text,text,integer)` | `supabase/repeatable/12102026_0620_banking_pay_next_frozen_used_line_detail_v1.sql:85`. Keep the fixed RATE_DETAILS SQL at 154–156 and original private SRF. After original guarded immutable line/revision and `used_line` lookup at 164–178, perform the RATE_DETAILS-only captured treatment projection before metadata byte charging at 180. |
| `public.bpay_next_original_csv_advice_page_v1(uuid,uuid,text,uuid,uuid,uuid,uuid,bigint,integer,integer,text,text,text,integer)` | `supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql:2610`. For RATE_DETAILS only, extend the existing actual transfer-member/WORK/run-line ownership lookup at 2773–2781 to obtain that line and exact RunPosition. Prepare the same bounded treatment projection before the original fixed query/packing loop; keep RATE_DETAILS source rows at 2838–2845. |

No change to `private.bpay_next_simple_rate_detail_page_v1(uuid,text,integer)` is needed. Its current 0025:111–136 RETURNS TABLE remains exactly `(rate_detail_id uuid,bucket text,approved_hours numeric,source_pay_rate numeric)`. Its native bucket/id order, bucket seek and limit remain intact. No DROP/recreate, public signature or new kind is justified.

## Smallest public shape

Keep every existing source RATE_DETAILS row unchanged: `rate_detail_id`, `bucket`, `approved_hours` decimal text, `source_pay_rate` nullable decimal text. Add **one RATE_DETAILS-only top-level `selected_treatment` object or NULL**, rather than duplicate the component quote on every bucket row or change the SRF return type. The object supplies the complete <=5 native bucket facts; private prefix targets and the potentially 8 MiB working set never enter a public response. For all other kinds keep the current envelope unchanged. New Worker/Office schemas should admit this property only on the exact RATE_DETAILS branch, including empty row pages.

Recommended closed shape, all monetary/quantity/rate operands emitted as native decimal **text** or their qualified NULL; no JSON-number/browser conversion:

| Property | Authority and qualification |
|---|---|
| `run_line_id`, `run_work_id`, `run_worker_id`, `candidate_id`, `captured_revision_id`, `approved_line_id`, `component_key` | Exact already-authorised frozen RunLine → RunPosition → RunWork/Worker identity. No current Work/revision replacement. |
| `source_pay_channel`, `target_pay_channel`, `target_umbrella_id`, `target_umbrella_vat_chargeable` | Frozen line channel plus the exact captured Worker's target tuple. PAYE has NULL Umbrella/VAT target; UMBRELLA has the native non-NULL tuple. |
| `valuation_policy_id`, `valuation_window_id`, `valuation_date` | Frozen line policy/window and the Run's captured pay date. Match the composer context, do not substitute today's official date or current policy/window. This is internal factual metadata, not permission to display a pre-execution date. |
| `source_consumed_ex_vat` | Exact native full component Source consumption, once. Never sum repeated bucket diagnostic Included values as component Source. |
| `component_quote: {ex,vat,inc}` | Must exactly equal `run_line.frozen_ex_vat/frozen_vat/frozen_inc_vat`; include penny conservation. |
| `buckets` | Only the composer's actual `bucket_details`, projected explicitly to the fields below; <=5, unique `(rate_family,rate_code)`. |

Each bucket row should retain the existing 2357 fields without new financial derivation:

`rate_family`, `rate_code`, `decision_id`, `decision_version`, `treatment`, `resolution_mode`, `approved_units`, `included_units`, `source_rate`, `source_ex_vat`, `manual_replacement_rate`, `manual_amount_ex_vat`, `quote: {ex,vat,inc,rate}`, `valuation_policy_id`, `valuation_window_id`.

`decision_version` is native decimal integer text; the ID/version pair is jointly non-NULL for `PINNED`, jointly NULL for `NATIVE_DEFAULT`. `NATIVE_DEFAULT` is the exact uncovered bucket's native equivalent-basis qualification, not a fabricated decision or CM receipt. An existing all-manual intact bucket can have honestly NULL attributed `source_ex_vat` while component Source remains known; do not fill it with parent Source or use it as a zero. `approved_units` and `source_rate` describe original approval; `included_units` and `quote.rate` describe the captured usable/selected target basis and remain distinct. True zero keeps ex/VAT/inc zero and quote rate NULL. Mode-specific raw manual operands remain nullable as supplied by the immutable saved decision; do not infer one from amounts or units.

The small per-bucket policy/window fields already emitted by 2357 can be retained to make closed validation explicit; all must match the enclosing frozen context. No Note, current labels, Source query facts, live availability schedule or new authority field is required here.

## Native qualification and pseudocode

For each reader, after its **existing** actor/NEXT/subject admission:

1. Read the exact native RunLine. If `accepted_resolution_refs IS NULL` (historical) or is the original empty `[]`, `selected_treatment=NULL`; leave all original source rows/used_line/packing behavior. Do not call the new composer on these paths, so genuine historical/Source unknown used-rate rows retain their original handling.
2. For nonempty refs, point-read RunPosition on the existing unique `(run_work_id,component_key)` and require its worker/Work/captured revision/approved line equal this line. Obtain Worker and Run by that identity; retain their immutable target/policy/date. No ACTIVE state predicate, current Candidate pay-method read, current Position reread or live quote is introduced.
3. Call `private.bpay_next_captured_component_resolution_quote_v1(actual_run_position_id,NULL)` **once**. Require exact returned `accepted_resolution_refs = line.accepted_resolution_refs`, returned `included_source_component_ex_vat = line.source_consumed_ex_vat`, `component_quote = frozen line target`, returned policy/window = line policy/window, and native Worker target/Run date = each bucket's qualified context. Require the existing captured composer classification (TAXABLE actual component) and the closed <=5 unique bucket shape. A nonempty pinned attachment that fails any qualification refuses through an existing/named typed invariant; it never silently falls back to the old source-only display.
4. Build the explicit small public object from those frozen/native facts, casting numeric JSON cells to text individually, and keep the existing source row query intact. No helper returning private working JSON to the caller is needed. The two named public bodies can use the same small fixed construction; a new generic projection framework is unnecessary.
5. Charge the **whole selected_treatment encoding once** into metadata/reply budget before adding rows; keep existing row-before-append budget, positive first-row refusal, incomplete short-page behavior and exact-full-page extra request semantics. Final public reply remains <=120000 bytes under the existing reader budget; no captured 8 MiB allowance is promoted to public transport.

The optional original bucket cursor does not filter the composition first: native full captured qualification precedes the source row page. Metadata is identical across the same frozen line's RATE_DETAILS pages. Cursor/order still use original native source `bucket`/ID, never target rate, selected mode or bucket plan array position. Adding the envelope therefore cannot alter existing continuation keys.

## Advice-specific safety

2018 original advice already proves exact instruction → original BANK transfer → sealed immutable projection/Worker/Run and exact WORK member line. Reuse that proof for the new metadata. **Do not call the READY-only 0025 simple frozen line guard from advice**: original issued advice deliberately survives current worker/transfer/outcome changes and projection retirement (2018 comments at 2750–2751). The projection is determined by the original instruction and its actual member line, not current DRAFT readiness, current target, current bank version or newest NET projection. Preserve its full Candidate/Umbrella original execution/destination qualification and all existing HEADER/WORKS/MEMBERS/SHIFTS/BREAKS/RATE_SCHEDULE branches.

The standalone 0620 frozen reader retains its existing READY/run REVIEW-or-DRAFT gate exactly; this support does not broaden its availability. Advice and standalone detail have different existing admission owners and must not borrow one another's status guard.

## Nonstandard/default/historical behavior

- STANDARD source rows still come from native RateDetail. Match a displayed row to metadata only by exact `(STANDARD,bucket)`; an approved zero-hours bucket may have no composed positive-approved bucket entry. Do not invent target rate/units for it or copy another bucket.
- Additional/Adjustment nonsegment components can have nonempty pinned refs and composer bucket metadata with their actual native code, while RATE_DETAILS source rows are empty. Preserve 0620 `used_line` and 2018 MEMBERS line's actual quantity/unit/rate; the new envelope can explain selected target treatment without creating a Standard bucket row, assigning `DAY`, adding zero-placeholder source cells or rewriting approved unit rate.
- Reimbursement/Case/no-ref paths stay original. Do not call this taxable captured composer merely because a line has a Source channel or target Umbrella.
- Cleared decisions remain reachable by the frozen exact ID/version. Captured display must not require current ACTIVE decisions or return a newly compatible decision. Failed retained quote qualification is an honest unavailable/refusal, never historical repricing.
- RATE_SCHEDULE stays the immutable original available schedule. This metadata does not overwrite it with replacement rates or present current availability as approval.

A later renderer may label Original/Selected and display the captured facts; it must not recalculate conversion, infer paid units or divide target amounts to manufacture rates. This task requires the two named reader joins and their closed transport additions, not a new screen or unrelated owner.

## Current read-only evidence

| Source | SHA-256 |
|---|---|
| 0620 frozen public reader | `96a26c83328dbb7253e8526c789dd11544d04d1b8a9dcf1b421e946fcd7dbc65` |
| 2018 advice public reader | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
| 0025 private four-column SRF/guard | `d7433fd91345a02ea75a596577c66514465c007d622dd0d225ca7edf96708ce9` |
| Authored 2357 captured composer | `1c4c166c42101dd9da9721be323b1468f9c8723b5608c2b8197fc6453aa176b4` |
| Authored 0005 hold/line refs join | `01dc28f60fde34213dee5064708bb68b968d41d57f688a8f4fd3c8e730483060` |

This author also wrote the preceding captured edit map; the relationship is disclosed. Final concrete SQL/closed transport must receive actual source review. Performance and business joins remain NOT_RUN.
