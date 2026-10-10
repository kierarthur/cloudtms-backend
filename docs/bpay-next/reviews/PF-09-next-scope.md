# PF-09 sequence23: current typed financial transport scope

10 October 2026. Read-only source inventory/design for root. No product/checklist edits, code/module/gate/unit/SQL/runtime execution, fixtures, hosted calls or secrets. Current saved BUILD files and the final-definer registry are authoritative; earlier RESET evidence is provenance, not a whole-file restore instruction.

## Exact card and all required rows

The current normal R3 plan06 card1852–1871 and build-order08:107/408 place PF-09 at sequence23, dependent only on F-06 and F-09; no separate design or owner decision is required. BUILD checklist.md563–584 and the three exact CSV deliverable rows retain their own Built/Reviewed/Installed/App-tested boxes. R6 maps them to R6-D0418/D0419/D0424. No SQL product routine or screen is named as a new deliverable here.

| Row | Exact required object | Current reuse and necessary remaining work |
| --- | --- | --- |
| PF-09-D01 | typed transport/exact decimal/reply-key source check | Add a small targeted check to the existing scripts/bpay-next-source-gates.mjs/F-23 invocation and focused positive/negative controls in its existing source unit. Current gate covers existing source boundaries and SQL-special-construct legality, not the complete PF09 projection/float contract. Restrict opaque/select-star detection to transported interface projections; retain legitimate private point-key rowtype locks/guards. Reject float/real/double financial DDL and untyped whole-row financial replies; check actual explicit reply lists/decimal transport. |
| PF-09-D02 | broker/src/bpay-next-command.js :: exact typed command transport | Already built current source: closed INPUTS/OUTPUTS, scalar field validation, exact key count, scope matching, decimal-string/BigInt validation, byte caps, actor-bound fixed RPC transport and actual index route. Reuse and verify this; no identified reason to rewrite pricing or add a SQL owner. Extend only an actual demonstrated decoder gap and the necessary narrow source/unit coverage. Source integration is not installation/readback acceptance. |
| PF-09-D03 | PF-09 future joined in-app TEST checklist | Save the explicit future joined checklist, including large/negative/zero decimal transport, outside-allowlist refusal, no float column and the 300-Draft line/hold/worker/transfer penny agreement. Worker/queue/minute-tick and in-app paths remain NOT_RUN. Do not invent a local fixture/hash/scenario programme or omit this required document because D01/D02 have source coverage. |

Card rules require named typed financial fields, closed allowlists, bounded text, exact decimals and one DB calculation. Source ex, paid/held credit, target ex/VAT/inc, payroll NET, bank cash and Case principal remain distinct. RQ-248/RQ-249/RQ-308/SRQ-186 apply. OR45 retains actual admission/security/epoch/history/obligations and unrelated Source/invoice/Candidate/MyTMS contracts; it does not require old Banking Pay engine parity.

## Actual current transport and final physical authority

- POST `/api/banking/pay/next/command`: broker/src/index.js198786–198808 invokes handleBpayNextCommand with the authenticated Office user and createBpayNextFinancialCommandRpc. Accepted responses retain the existing ENROLL wake/recovery behavior (198793–198806); PF09 must not convert a wake failure into rejected accepted economics or introduce a new drain/clock.
- broker/src/bpay-next-command-rpc.js has only fixed public financial intake and fixed original-credit bind endpoints. It injects authenticated actor, service headers, routeClass OPERATION_NUDGE and AbortSignal; returns a raw Response; no RPC selection, buffering or retry.
- Current final public.bpay_next_financial_intake_v1(uuid,text,jsonb): supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql3044–3323. It independently checks service/active admin/NEXT, closes action/arg lists, requires string args and constructs named v_output fields rather than returning an arbitrary v_result. Numeric reply counts/sequences are text. Keep its installed immutable source; PF09 introduces no new financial definition unless a real current interface defect is found.
- Current final public.bpay_next_bind_stored_credit_v1(uuid,uuid,uuid):04102026_1827_banking_pay_next_candidate_destination_group_v1.sql. This fixed identity-only exception is already in command INPUTS/OUTPUTS and actual RPC adapter; do not drop it.
- Current final public.bpay_next_preparation_page_v1(uuid,text,jsonb):2018:3776 onward. Its COMPONENTS/WORKERS/LINES project separate source/target fields with explicit `::text` (3878–3916), and delegates other closed kinds to actual final private readers. RuleB F25 choice.expected_revision_id, selection_candidate, captured query snapshot and pin_v2 remain built authority; do not reinstate parked accepted-cut/resolver readers.
- Delegated current final readers: private current_work_page and worker_status_page in04102026_0580; current_case_page/current_case_component_page in04102026_0585; return_cash_page in05102026_0001; channel_worker_page in06102026_0236. These are reused existing projections, not new PF09 money owners.
- Current final public.bpay_next_payment_history_page_v1(jsonb):06102026_1206_banking_pay_payment_history_page_v1.sql. Its52 literal section/field allowlists generate only named `t.%I::text` fields (363–385), and Worker bpay-next-history.js135–168 enforces exact manifest/envelope/data keys and STRING_OR_NULL values. Its constrained dynamic field projection is legitimate; do not replace it or broadly ban dynamic format/read-side SQL.

## Current scalar separation to preserve

| Financial meaning | Existing current transported fields |
| --- | --- |
| approved/realised/held/residual Source ex | preparation COMPONENTS approved_source_ex_vat, realised_source_ex_vat, held_source_ex_vat, residual_source_ex_vat |
| consumed Source and frozen target amounts | LINES source_consumed_ex_vat separately from frozen_ex_vat/frozen_vat/frozen_inc_vat; WORKERS/CHANNEL_WORKERS have those three frozen totals |
| whole-worker NET/projection | STATUS frozen_gross_ex_vat/vat/inc, entered_paye_net, accepted_recoveries, accepted_net_additions, projection_id/input_kind and cash_amount remain separately named and honestly nullable when pending |
| bank cash/returned cash | STATUS cash_amount; RETURN_CASH original_cash_amount and amount_owed/held/reissued_paid/available remain separate two-decimal text fields |
| Case principal | CURRENT_CASES principal_approved/funded/recovered/written_off and active_hold_amount; CASE_COMPONENTS separate approved/funded/recovered/written_off Source ex and active payout/recovery Source ex |
| immutable hold/history facts | history HOLD/CASE_HOLDS retain source_reserved_ex_vat separately from target_amount_ex_vat/vat/inc and their declared amount_scope; history is not new entitlement or browser pricing |

Command money input is nonnegative decimal string with at most16 whole digits/two decimal places; case writeoff uses exact arbitrary numeric text under existing input byte limits. Preparation factual approved liabilities permit signed money with up to six decimal places; cash fields remain two decimals. BigInt is used for exact int64 identities/count validation and pennies equality checks, not JS Number pricing. Number(content-length), bounded page limits and Date checks are legitimate metadata controls; do not impose a Number/BigInt/SELECT keyword ban.

Command field bounds include receipt256UTF8 bytes, reason1024, filename256, code256 and CSV120000, plus2KiB request/128KiB response/8s envelopes. Preparation and history preserve their existing whole-envelope and selected-type bounds. An exact decimal string is not an opaque free-text financial record.

## Existing ordinary source units to reuse, not execute in this inventory

- tests/bpay-next-command.test.js188–196 already sends 9999999999999999.99,0,0.00 without digit loss;351–362 rejects numbers/out-of-range ints and extra receipt keys;435–452 checks actual final SQL owner/string projection and denies direct return v_result. Its existing units also cover arbitrary RPC/money/actor rejection, bounded UTF8/evidence, timestamps, CSV bytes, replay and uncertain outcomes.
- tests/bpay-next-command-rpc.test.js verifies the fixed actual adapter, actor binding, raw response, signal and actual index seam, including stored-credit exception.
- tests/bpay-next-preparation.test.js179–195 rejects numeric/exponent money and preserves zero/signed approved liabilities, nullable position and original child zero facts. Existing LINES/WORKERS page fixtures are ordinary isolated source units with named fields; no fixture runner or hosted scenario is authorised by this note.
- tests/bpay-next-payment-history-caller.test.js verifies all52 allowlists against actual SQL registry and exact decimal/int64 string transport.
- Current F-23 gate has no PF09-specific transport/decimal/type check in this reviewed source. A bounded textual migration census identified only prose uses of real, not a float declaration; that is not an executable gate receipt.
- The card names assertNoBrowserFinancialFacts as reuse guidance. That literal helper name is absent in the chosen current BUILD-FE js and top-level tests/unit files; do not create a new product helper merely to match a historic name. Reuse current Office display-only source controls and the existing bpay-next-office.test.cjs/payment-history source units. Main's unrelated financial code is outside a broad browser-keyword ban.

## Small finite gate recommendation

Use existing managed SQL statement/declaration tokenization and current physical final definitions. For NEXT-owned migration/return declarations inspect actual type tokens; comments containing real/float and ordinary legacy columns are outside the gate. For financial transported projections inspect the selected/replied value: arbitrary row JSON, `SELECT *`/alias.* as the transported record or unfiltered v_result return must fail; SELECT alias.* INTO a private typed row variable used for keyed lock/validation must pass. Keep already constrained history field-template projection and public wrappers' typed private delegation valid.

Bind command/preparation/history request and reply schemas to the actual closed decoder path, and keep the SQL projection keys/string casts checked by existing units. Add only finite controls for numeric money, extra reply key, float DDL, opaque whole-row return and legitimate private point-key rowtype guard. The later line/hold/worker/transfer conservation assertion is a joined future business journey, not a source gate's invented success claim. No general callgraph, additional financial protocol, synthetic database, provider scenario, broad Source/invoice/Candidate edit or new cleanup owner belongs to PF09.

## Reviewed source hashes

| Current file | SHA-256 |
| --- | --- |
| broker/src/bpay-next-command.js | 95f082f6fef61932b0498ea007d47966692ec15094d954fc9cbbf4bd1d7ed059 |
| broker/src/bpay-next-command-rpc.js | 060ce9bdab50576aec731d0b9b3c6499a608e3cd71c17a3c9bdf54483db1b68e |
| broker/src/bpay-next-preparation.js | 9271ec0086bffdd9779707bf44407eba0923b1392317eb142e46c8f8e0338749 |
| broker/src/bpay-next-history.js | 81d4ca2fb90b1bbec7dea3a41698933f70b306833079484d45a89e4e77738451 |
| scripts/bpay-next-source-gates.mjs | 4521c19791a30845cfb65a35e743117c8f717994887145f26e3dce2ff87c0f61 |
| docs/bpay-next/final-definer-closures.json | 06b12aca8673b4d5b113684e21c744ba92d29618de43b70e585b71042cab3309 |
| supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql | 7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b |
| supabase/repeatable/06102026_1206_banking_pay_payment_history_page_v1.sql | d963c739f743e1526647ef101db7b9460dad88be3e2f56c2775c199aeddc2c73 |
| tests/bpay-next-command.test.js | b1051f3dcf536c0616d8699ee7bf98c4fe4c4658af8a57113e93e0645ee8bade |
| tests/bpay-next-command-rpc.test.js | 60ce0eecd5da3c5ad6be7cca3d65ac3e274b4fb0f489d3288cd8d57e397ae9a9 |
| tests/bpay-next-preparation.test.js | 5ceb752d39b24061a39d0d8c6e4c69db4e1acfd2c9f27836ba8884c46670d1a7 |
| tests/bpay-next-payment-history-caller.test.js | 8a09560333f72c485564777980d0c82af4451d0f4e73f9e02f55cc01f54fc98c |

No source/unit PASS, installed receipt or in-app tick is inferred from this inventory. Root should re-read any gate/test bytes immediately before writing because PF04 changes may be concurrent.
