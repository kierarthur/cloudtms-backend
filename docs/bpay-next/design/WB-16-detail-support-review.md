# WB-16 narrow detail support review

Read-only source investigation, 10 October 2026. Scope: named Case component0585 and public preparation2018 successors, strict Worker/Office mirrors and WB15 Candidate expansion. No SQL, runtime, tests, fixtures, hosted requests or product writes. This note refines `WB-16-current-reader-investigation.md`; no WB29/WB45 decision schema, estimate, calculator or command is proposed.

## Scope conclusion

The planned native line/role/band, stored schedule/approved charge and exact stable shift additions fit the named readers. Keep the existing request kinds/cursors and current Work/revision/line checks. The implementation can expose a bounded **snapshot**, without live Contract substitution or an unbounded revision schedule array. Current Case component facts may add actual stored rule fields, not calculated affordability.

## Actual schedule schema and nulls

Core1324:262–279 defines `private.bpay_next_rate_schedule`: id/revision_id/rate_family/rate_code are nonnull; family is STANDARD/ADDITIONAL/MILEAGE; unique `(revision_id,rate_family,rate_code)`; nullable unit_label/paye_rate/umbrella_rate/charge_rate. WB01 migration1009 removes the old nonnull-rate check, widens rate_code to **245 characters**, and adds nullable approved_pay_rate and approved_charge_rate (numeric18,6). Thus an existing row with every rate NULL is legal and meaningful. Missing joined row and existing all-NULL row must remain distinguishable by its native schedule ID.

The canonical1009 producer always returns five STANDARD rows and a MILEAGE:MILE row, even all-NULL; configured ADDITIONAL codes are upper(trim(code)), unique only within that family. An Additional code can equal DAY or MILE: never join by code alone. Ordinary stage1236:611–632 stores approved_* from the actual TSFIN: STANDARD pay/charge bucket, MILEAGE native mileage terms, ADDITIONAL exact used-unit row. Source stage1236:1660 supplies only available schedule columns; its approved_* remain NULL. Do not backfill them from available rates or use amount/hours division.

Current frozen Office rateCode permits128 characters; any reused new factual validator must permit actual245. Do not silently truncate codes. Stored rates/hours keep signed decimal text up to six places, not two-place money rounding; quantities are native numeric (not a browser float or automatically rate-sized magnitude). Keep native SQL/Worker byte bounds as the packet bound.

## Minimal bounded projections using existing shapes

1. **RATES:** retain the existing rate_detail rows and bucket cursor. Left join exactly one STANDARD schedule row:

   ```sql
   left join private.bpay_next_rate_schedule s
     on s.revision_id=v_revision and s.rate_family='STANDARD'
    and s.rate_code=r.bucket
   ```

   Extend each row with schedule ID, exact family/code and nullable unit/available PAYE/umbrella/charge plus approved pay/charge. This is a one-to-zero-or-one join under the compound unique constraint and cannot duplicate a rate_detail. Preserve r.source_pay_rate as the actual **used** rate. Neither available nor schedule-approved pay overwrites it. Source null used/approved charge remains null.

2. **COMPONENTS:** expose the real `approved_quantity`, `unit_label`, `approved_unit_rate`, `evidence_ref`, revision `job_title`, `band_label`, `timesheet_reference`. For ordinary Additional/Mileage the writer proves exact native keys `ADDITIONAL:<upper(trim(actualcode))>` (1236:966) and `MILEAGE:MILE` (1236:1006). Join ADDITIONAL by the full suffix **after that one prefix**, preserving embedded colons; join MILEAGE only exact MILEAGE:MILE. Include family in every match. Never split at the last colon, match a label or infer an Expense's hourly code.

3. **Unused STANDARD availability:** existing RATES contains only actual rate_detail rows, so RATES join alone does **not** display unused buckets. If required by SCR10, add one nested `standard_rate_schedule` projection to relevant COMPONENTS rows with an exact STANDARD filter, native family/code/ID and at most five rows. Keep fixed business bucket order for display; keep actual existing RATES cursor order. This is bounded independently of arbitrary Additional schedule length and still uses COMPONENTS, not a new kind. Include all-NULL stored standard rows. The existing 120000 SQL bytes/128KiB Worker envelope may end a component page earlier; do not advertise more rows than actually emitted.

4. **Do not embed every revision schedule.** ADDITIONAL cardinality has no five-row bound. One exact line-associated row is enough for ordinary Additional/Mileage support. Source uses `component_key='SOURCE:'||component_id` (1236:1677), including Additional; its line unit/rate is genuine but its key does not contain a Contract Additional code. Return null schedule association unless an actual retained typed code is demonstrated. Unit label/equal amount/Source component UUID cannot identify an available Additional row. This is an honest limitation, not permission for a new Source producer or rate mapper.

For nullable joins use one closed shape with schedule_id nullable; if absent, all associated schedule fields null. If present, family/code are nonnull and exact intended identity; individual rates may all be null. No nullable family/code guessing or zero fill. Mirror schema and these relationships in Worker/Office before rendering. IDs and money text remain facts, never browser economics.

## Native correction/original-reference semantics

The structural baseline1500:4337–4347 **does** contain actual physical `timesheets.is_adjustment`, `parent_timesheet_id`, `correction_id`, `correction_kind`, `adjustment_origin`. This refines the previous note's statement that no correction carrier was seen. A minimal factual join can attach them to COMPONENTS through **the exact validated revision's physical_timesheet_id**, and point-read its actual parent UUID/reference after verifying parent Contract's Candidate matches the current Work Candidate. No latest Timesheet/booking/version lookup or recursive chain sweep is necessary. A missing/mismatched parent reference stays null; do not substitute Work.original_timesheet_id.

`bpay_next_work.original_timesheet_id` is the first physical row recorded when a Work is created for a booking (ordinary1236:575–579; Source1236:1633–1637). It is not a correction parent or proof that a later revision replaced that Timesheet. Revision_no>1, negative amount or ADJUSTMENT component is also insufficient for a Correction/Replaces badge. A supplied parent link may support **Original reference**; it does not alone prove full replacement economics.

Import correction reversal/replacement children are explicitly **factual/invoice only** under current route1450:52–77. Typed markers require original import-classification provenance and the route returns FACTUAL_IMPORT_CORRECTION; NEXT publishes the approved complete Source root. Therefore do not make those retired children selectable Work or display the Source root as a new correction child. Do not call the broad legacy `_ctms_import_correction_classify_v1` from this detail read merely to make a badge: it reads current TSFIN/settings/envelopes and is not a narrow retained display projection.

If transporting raw correction fields, label only the actual supplied factual type. `is_adjustment=true` alone means Adjustment, not necessarily Correction. A native known replacement kind can support that wording only in its actual physical relation; parent reference is not automatically the reference of a Work's original revision. No Original/New rate/money comparison can be derived from these flags. A real correction relation may legitimately be absent from current eligible Work; omit the decorative badge then.

## Case and lifetime boundaries

0585 already checks Candidate/Case OPEN-or-PAUSED scope and limits component ordinal pages to100/120000 bytes. Add stored rule_id and actual rule scheduling/cap/floor fields through its native component rule relationship, with nullable absent rule; no per-run recoverable, insufficient-pay calculation or active-period creation. Preserve Case parent/component identity and unchanged status/amount counters.

The new expansion reads through the existing qualified preparation transport under retained Workbench context + exact current revision. The fixed schedule join is part of that same response scope, not a later live Contract fetch. Reject stale revision/type/schedule identity before one DOM adoption; retain the parent table/scroll/ticks and independent child request generation. Frozen Draft route/run-line schemas remain separate; no ACCEPTED_* or F25 parked files are used.

## Source receipt

| Source | SHA-256 |
|---|---|
| preparation2018 | 7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b |
| Case pages0585 | ec0ab1ec0ce95b690f50c711023a6dda431e562ed075a9716361d1ede2aac436 |
| segment final1236 / stored rate producer | f74ea457bf77f0d85ce7f3f21089bdecf9d9d9121d4005850de430d50aa7379d |
| canonical rate rows1009 | 07843dcef992a74c2dd2c8c40185d17ce70aba9c33c1c53db5bbcbf6d6952fe2 |
| schema rate migration1009 | 071cf3af4a6ae9ef1c63c93eb32d33b00c9bc93fbc51558d0598d3da5f00b0b4 |
| route1450 | a8f9dc118ad75b62b3863820e38bd29cbc4c05aa47facf12c11e45b9013bfc74 |
| Worker preparation | c04cdd54164ac1a534f2ddb366485b3b842568fd88fdb8a187476ecd925404b0 |
| Office at inspection | 8e6ba73244cf62fca4ad5c6d271cd60aecf3f9d0e47cc7916829f05f64561309 |
| Workbench at inspection (WB15 edits concurrent) | 0d6c5cb787e0d98d26b991698b5b4e9a5458ace3428983f99f1d7968c688580c |

These are an investigation boundary, not an implementation or A4 screenshot verdict. Re-read fresh files before authoring/reviewing successors.
