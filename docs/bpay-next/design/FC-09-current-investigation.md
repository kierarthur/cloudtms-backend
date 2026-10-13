# FC-09 current investigation — sequence107

11 October 2026. Read-only source/design investigation. Product, checklist, SQL execution, tests, data, browser, render and hosted operations were not performed. The companion boundary is an author proposal for root's independent check, not implementation approval or a built-item receipt.

## Authority and remaining rows

Read the full approved card in the ordinary backend plan `BPAY_NEXT_BUILD_PLAN_20261009_codex_r3/03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:1196–1215`, all current checklist rows `docs/bpay-next/IMPLEMENTATION_CHECKLIST.md:3158–3186`, R3.json FC-09 section at21143, binding09, owner07/10, targeted R2 review references, sequence107 and the shared Case chain. Owning rows are D02 repeatable, D03 CASE_EDIT contract, D05 Worker maps, D06 Edit screen, D08 amendment-aware preparation and D10 FC16 call site; D01/D04/D07/D09 are retained source/context. Dependencies FC03/FC16/WB08/F16 are unchanged. Sequence106 FC06 precedes this item; sequence108 FC10 follows. No row is completed by these documents.

The immutable original CASE_CREATE is retained. A supported edit appends an immutable CASE_EDIT request/event and new rule with an exact current component projection. Both estimate and preparation must recognize that exact amendment; a latest-event/current-history fallback is prohibited. Accepted PREPARING Case intent is protected before an ACTIVE hold exists. READY release remains DR05 WORKER_RELEASE; confirmed Draft is never amended (OR05). No automatic release is added to Edit.

## Exact current owners, not old card literals

Current registry `docs/bpay-next/final-definer-closures.json` SHA-256 `674bef14fc610cc56d448f1588cfd456245b9f2379f047c7bad8410fb20f0b89` identifies:

| Signature | Current complete parent |
| --- | --- |
| private.bpay_next_case_allocation_plan_v1(jsonb) | 11102026_0204_banking_pay_next_case_allocation_plan_v1.sql |
| private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer) | 11102026_0904_banking_pay_next_early_candidate_financial_fence_v1.sql |
| private.bpay_next_case_change_assert_v1(uuid,bigint,text,numeric,text) and protected_bindings(uuid,jsonb,integer) | 11102026_1509_banking_pay_next_case_change_guard_v1.sql |
| public.bpay_next_financial_intake_v1(uuid,text,jsonb) | 11102026_1536_banking_pay_next_case_create_intake_v1.sql |
| public.bpay_next_preparation_page_v1(uuid,text,jsonb) | 11102026_1738_banking_pay_next_case_list_v1.sql |
| private.bpay_next_current_case_component_page_v1(uuid,uuid,bigint,integer) | 11102026_1910_banking_pay_next_manual_debt_reference_v1.sql |
| private.bpay_next_case_create_preview_v1(uuid,jsonb), 17-argument accept | same1910 |
| private.bpay_next_case_create_terms_v1(jsonb), context(uuid,uuid), apply(uuid,uuid,bigint), 14-argument accept | retained1536 |
| public.bpay_next_financial_job_v1(text,uuid,text,jsonb) | 11102026_0814_banking_pay_next_preparation_discard_v1.sql |
| private.bpay_next_enroll_one_v1() | 11102026_0847_banking_pay_next_discard_row_projection_v1.sql |
| private.bpay_next_case_rule_insert_guard_v1(), case_event_insert_guard_v1() | retained04102026_0310_banking_pay_next_case_guards_v1.sql |
| private.bpay_next_workbench_refresh_case_v1(uuid) | retained11102026_0559_banking_pay_next_worker_reprepare_v1.sql |

All SQL names above are under `supabase/repeatable/`. Case instruction guard remains0559; capacity/hold/result guards remain0310; the original row kernel0290 remains the only allocation formula. No obsolete0360 or gated0549 body is restored.

## Demonstrated preparation gap

0204:89 binds the exact original creator. At159–173 it requires the original principal and original primary/recovery component/source identity. Changing current principal or tax/stage with only a new rule fails this check. FC09 must resolve an amendment by an exact immutable key such as the captured/new rule ID and its Case/Candidate/original creator/component tuple; original creation still proves root identity. It must not infer approval from any current rule or most recent event.

0204:242–275 admits a different period rule only for genuine LOAN/ADVANCE funding evidence with unchanged schedule/tax/age facts. It does not admit an edit schedule successor. 0904:333–389 locks Case/component/period, calls the shared plan and writes/captures its explicit period operation. D08 therefore targets current0204 first and current0904 only where an additional exact plan operation genuinely needs a writer.

Original1536:344–359 already opens one or two periods at creation. A blanket “any period exists” refusal would make the just-created acceptance examples impossible. The schema0300 has one mutable period row per component/week and period_revision, not a second immutable row per revision. Eligible zero/unconsumed opening replacement needs an exact immutable old/new amendment basis and an explicit versioned successor; protected opening/instruction/result facts and funded age must remain unchanged. Root must close this small period operation before coding; generic schedule update is insufficient.

## No-activity and concurrency findings

Card1208 requires no economic event beyond APPROVED, no ACTIVE hold and no funded/recovered/paid amount. Include written-off and all active component/period counters, original typed result/economic use and actual payout commitment in the precise native test. Unknown or unsupported provenance refuses. FC16 pending/protected bindings refuse even when active_hold_amount is zero. Prior exact CASE_EDIT amendments must be distinguished from unrelated CORRECTED events if repeated pre-activity editing is supported; this distinction must be keyed to the immutable amendment request, never broadly waive event history.

FC16 assertion1509:238–293 is VOLATILE but takes no Case lock; it assumes the caller owns the native writer locks. Its WHOLE_CASE mode rejects any protected binding and produces the four exact summary keys; pending capacity/amount are NULL, not zero.

Original Create1358:219–225 acquires selected component SHARE locks before its command clock and membership insert. An edit writer must acquire its bounded exact component UPDATE locks before the FC16/no-activity reproof. Case-only locking does not serialize with the accepted cut. Preserve existing module/Candidate/worker_control ordering; do not acquire pay-run headers under Case locks. Recheck at queued apply as well as admission: a newer Create cut accepted while the edit waits must be blocked, never altered. Short command-clock acquisition remains after ordinary source locks; exact accepted replay returns before fresh state/protection refusal.

## Existing transport and screen seams

BE `broker/src/bpay-next-command.js:37,57,73,224–227` has no CASE_EDIT action/schema/receipt. `broker/src/bpay-next-job-step.js:29–75,227` and existing native queue maps recognize CASE_CREATE/CASE_WRITE_OFF but not CASE_EDIT. New ordered CASE_EDIT therefore needs its exact finite request/receipt, enrollment and financial CLAIM/STEP branch, using existing command/member/job tables and the existing ID/kind delivery. The core schema1324:795–991 has no closed command/job-kind CHECK requiring broad enum reconstruction.

Reuse current CASE_PREVIEW page with an exact optional edit operand variant, and the existing targeted CASE_COMPONENTS reader for immutable current prefill and lifecycle/protected facts. Current row1910 includes rule/schedule and saved original reference but not all current edit metadata, expected Case revision or exact protection references. Append only required fields/point facts. No new page kind, RPC, preflight store, agency inventory or Case-list UI is necessary.

FE Office `js/bpay-next-office.js:1936–1951` shared footer is currently CASE_CREATE-specific; its controller/journal/submit/ack variant must explicitly recognize CASE_EDIT and exact Case/expected revision. Reuse the actual Advance/Credit/Debt controls and native standard-window adapter. Candidate and kind remain locked. Late actor/owner/parent effects, pending replay bytes and targeted acknowledgement remain fenced. Existing forms currently support creation only; a Case ID must not be sent through a legacy edit route.

The no-caller `banking:finance:editBankDetails` handler exists at `js/main.js:102753–102789`; whole FE-js search found only that occurrence. Its helper is used by this branch. Remove only the dead branch/helper if still sole-use. Ordinary bank ownership and futureFC07 are not expanded.

## Supplied design and committed-credit decision

Read-only supplied B9 index/specs and both individual PNGs were inspected:
- SCR-122_edit_case_dark: Edit Payment Advance; locked worker; factual status/payout/repayment cards; amount/schedule/threshold/floor/Note; Preview and Save changes.
- SCR-122b_edit_credit_committed_dark: factual read-only amount/tax/comment and protected Draft reference; editable Note and Save changes.

No Note-only post-activity authority was found in card1200–1213, binding09, owner07/10 or targeted R2 review text. Root's narrow reading is therefore full committed/activity Case read-only; omit Note Save/Preview rather than add an unsupported post-activity mutation. Record this supplied-design difference under A2. Display amount/tax/comment and protected amount/reference truthfully, with no invented Draft official date and no guessed money for pending inputs.

## Owner directions and evidence limits

Preserve original Create cut and immutable captured RuleB inputs; balances remain live only at existing locked money owners. E1 original append skips and E3 absence of capture-time hold repricing remain; E2 native release mechanics and E5 exact sealed retry/economic-vs-execution identity remain untouched. Do not modify parked F25 resolver/producer, Source, public triggers, financial formulas, bank execution or later restructure/pause owners. A5 original descriptors/journals stay canonical. Native dark host, current actor/NEXT admission, continuous scrolling for touched lists, server money formatting, one primary action, owner one-sentence error copy,44px phone controls and documented visible differences apply. A4 later requires actual screenshots/all42 policy rows;25 and reader-additions26 are N/A per owner, not evidence supplied here. F18 hosted counts remain NOT_OBSERVED; no fictional absence or old fixtures are asserted.

## Source baseline hashes

| Source | SHA-256 |
| --- | --- |
| plan0204 | 2eeaafb8f14444715c30e13dc898f9b35614de1f53920344913f2986e09d6b0c |
| prepare0904 | fa3c73a83075f67246e62adea7ba1467c019bada601d3db880ec51b48cdd1a9e |
| FC16 guard1509 | 087c3c5d3ae9cfaa94b66ea58b338358d0236f2b4953be5ee6adf7e3dfc60fd4 |
| creator/intake1536 | 853ddce45455defed9f715a2593267794f3b224e469a9eaccde2aeca6e44a470 |
| reference1910 | 644995fb19e4a58d0854de8191980dc7e543015358d81d229785165377e2845c |
| guarded Create1358 | 6d13cd0a482be586e49aaeb2fb67bca82613aa092657cbcc208e6f012acbc0a6 |
| financial job0814 | 29f3099ebfb8a7e11d8c23a8996c5351eb9330316d8bdb6fd6d3a5136d3f379c |
| enrollOne0847 | 86343d45963a9335de8f3af0a3465cbbcdd5c4c2fd3eeef69c628060c8df1c47 |
| guards0310 | 7dd5085d3307a0194b3745487cc19a952ee3b8cb11d03f096d275d23632500fe |
| FE Office | 1cc98547542c99652c6b0e6c5139e64c9ac8a4dbb2e24c782ec0eda29b9b5b77 |
| FE shared Preview | 16914287a7f09824a4a9d9e8f99533209918970ae7a311660f9c17828a792cb2 |
| FE main | 74d30eb024f6ae5b0bde73d590a3af5ef2b93a37fb1d09bab1eb3560f889a07c |
| supplied SCR122 PNG | a786786880ec47050390fb706418455a4d065d91516b6d7f36901a6cf5c598c4 |
| supplied SCR122b PNG | 5ead1add79b9aebbb7d1fd61d49fe328bd9fb25c45042ba74a953b95856fd18d |

These are current saved source/design bytes, not deployed or runtime proof.
