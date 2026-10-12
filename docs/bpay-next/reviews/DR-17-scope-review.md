# DR-17 scope review

12 October 2026. **DESIGN SCOPE PASS**, subject to the concrete transport and byte-preservation requirements below. This reviews the proposed narrow boundary against the complete card03:449–471, every current DR-17 ITEM/D01–D08 checklist row, current registry and saved DR-17 investigation. It does not review newly authored product bytes or mark implementation/app acceptance complete. No SQL/business calls, tests, fixtures, hosted actions or captures were run.

## Required scope and existing owners

D01 remains `public.bpay_next_frozen_detail_page_v1(uuid,text,uuid,uuid,uuid,integer,integer,text,text,text,integer)`; its current complete parent is `11102026_1005_banking_pay_next_frozen_client_origin_v1.sql`:84–196. Extending the same parent's `private.bpay_next_payment_client_origin_v1(uuid)` is a small supporting change for the card's frozen typed origin. Preserve the four existing kinds, exact request identities, service/admin/NEXT qualification, REVIEW/DRAFT + READY guards, immutable approved-line membership, existing continuations and ACLs (1005:104–165/170–196; private0025:21–32/162–177). No new route, detail kind, producer, capture store or money owner is needed.

D02 retains the unconditional immutable-effect guard/four existing attachments; no replacement is justified. D03–D05/D08 are context/buildTask:false, not instructions to rebuild capture/publisher/hold. D06/D07 reuse the existing native five-tab dialog and exported launcher with an explicit initial-tab option and five READY Candidate-expand controls. DR31 consumes this exported seam at its later sequence158; its tree is not an early DR17 deliverable. Card03:453/463 and the contextual checklist rows explicitly preserve the existing immutable inputs and direct missing capture families to their actual owners.

## Used values and source/protected identity

The proposed nullable closed `used_line` on RATE_DETAILS can expose the exact approved-line component kind/key, stored unit label, quantity and unit rate. Ordinary Additional is already captured with all those values at publisher1001:700–709; mileage is captured at740–745. Read them by the existing guarded run-line → approved-line point, retaining original captured revision identity. The existing standard rate-detail rows remain authoritative for standard bucket quantities/rates, including mixed-rate Work whose line-level aggregate unit rate is NULL. Do not divide line money by quantity, multiply into a new payment amount, infer a rate from availability or treat a NULL field as zero.

**No additional actual-used-charge authority is required by this card's literal contract.** Card455 requires separate PAYE/Umbrella/charge **available** buckets plus used quantity/rate per line; card469 requires the five frozen pay/charge buckets. The proposed line pay rate/quantity plus the already frozen available charge rates meets this distinction. Source0005:617–620 freezes actual used pay rates, not a used charge rate. The earlier investigation's conditional suggestion to expose an ordinary approved charge field is optional provenance, not a required extra owner or permission to invent Source charge. The supplied Used-rates picture does not override card data authority.

The exact captured revision's `source_kind` is sufficient typed Source/protected provenance alongside the existing captured line component kind. Core1324:136 permits ORDINARY, SOURCE, PROTECTED, EXPENSE and ADJUSTMENT. Source0005:374–388 freezes SOURCE/PROTECTED, while its actual WORKED_TIME approved line is component_kind WORK (431–447); it must not be relabelled ordinary from WORK alone. Use frozen source_kind for that distinction. The exact same line's shift/break membership already carries the native Source/protected worked facts (0005:513–620; private line guard0025:21–32). No separate per-shift origin column, live Source label or producer rewrite is necessary. Preserve existing frozen Client/Daily authority and honest NULLs for absent metadata.

## Exact storage bounds: important implementation requirement

Core `03102026_1324_banking_pay_next_core.sql`:188–212 defines:

| Approved-line field | Actual constraint |
|---|---|
| component_key | NOT NULL TEXT, 1–256 characters (:192) |
| component_kind | NOT NULL, WORK / PROTECTED_WORK / EXPENSE / MILEAGE / ADDITIONAL / ADJUSTMENT (:194) |
| unit_label | Nullable TEXT, **no field-length CHECK** (:196) |
| approved_quantity | Nullable unqualified NUMERIC, **no declared precision/scale or finite-value CHECK** (:197) |
| approved_unit_rate | Same unqualified NUMERIC (:198) |
| source_pay_ex_vat | NUMERIC(18,2) (:201), a different field |
| expected_rate_detail_count | 0–5 (:199–200) |

The rate-schedule unit-label <=512-byte CHECK at core272 applies to that other table; it is not an approved-line unit bound. Revision child guard1342:99–117 enforces insertion before seal and does not add numeric/label size constraints.

Consequently the successor must preserve the stored nullable `::text` numeric value without rounding, fixed-scale casting or guessed precision. Include the entire new `used_line` metadata in the existing SQL page byte accounting and final closed response/Worker envelope bounds, even on a zero-row RATE_DETAILS page. Reject genuinely unrenderable/oversize/nonfinite source data through the existing explicit unavailable/error path; never shorten it, coerce to zero or return a false complete page. BE/FE finite decimal validation must not assume approved quantity/rate is NUMERIC(18,2)/(18,6). This is an output-shape/byte guard requirement, not a schema or producer change. Source Additional already captures native numeric unit_count/unit_pay_rate with nullable unit_label (0005:431–447); preserve that NULL rather than supply a guessed unit.

Canonical helper1009:29–74 permits code length245 and legitimate all-NULL nonstandard availability. Correct the existing BE and FE 128-code ceiling and all-NULL non-STANDARD rejection; retain all five standards, each actual Additional/mileage code, and separate nullable channel/charge fields. This is the concrete existing consumer gap, not authority to add/fill rate rows.

## Presentation and closure boundary

Display approved quantities, not paid hours; distinguish standard hours from actual stored nonstandard units. The five buttons call the same native dialog; Source/protected wording comes from captured source_kind. Maintain actor/NEXT/frame validation and the existing unavailable/error state. Keep original fixed detail/break/bucket/family-code continuations. `SCREEN_DESIGN_POLICY.md`:172 (SDP34) expressly exempts dialogs, expanded child rows and fixed business order: **no new sorting system, sort arguments, indexes or reader is needed for this native detail dialog**.

SCR47/47b/47c/47d/47e are the direct supplied designs. Card455 owns Additional/mileage availability even where the picture is five-standard-only; record that narrow visible difference, without additional reader facts to fill unsupported picture columns. No other design feature or state-independent History/SP22 route is added here.

No scope blocker requires a new financial/capture owner. Concrete product source, BE/FE closed-schema parity, original byte ceilings and five launchers still require review after authoring. Immutable later-edit behavior, joined native read journeys, queue/runtime and in-app acceptance remain **NOT_RUN**.
