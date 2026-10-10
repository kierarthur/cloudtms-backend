# WB-35 upstream route census — source only

**The named rate, TSFIN and finance-window routes are retained outside the NEXT legacy route fence; TSFIN cron remains independent. No confirmed upstream route/product gap was found. RECOVER_FINALISED_PAY retains its route and factual owners but correctly refuses retired finance under the binding SP-02 / OR-45 rule. WB-35's older claim of successful NEXT Recover parity needs documentation reconciliation, not removal of that fence.**

Read on 10 October 2026 from current BUILD-BE disk. Full R3 WB-35 card (`02_WORKBENCH.md:1263–1281`), all checklist rows D01–D16 and master sequence47/dependency F-12 were read. No route, drain, worker, cron, SQL, test, app, fixture, data, network or hosted operation was invoked; only this artifact was written.

All source paths/lines below are relative to `C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-build-backend-20261009`. Worker index line numbers use actual LF boundaries.

## Actual common fences

`broker/src/bpay-next-legacy-fence.js:162–181` is the one current `isBpayNextLegacyFinancialRoute(method,pathname)` declaration. It normalizes slash segments (164–166), allows only exact NEXT contracts (168–174), then matches `/api/banking/{pay|finance}`, exact old Timesheet advance/snooze/adjustment paths, remittance mutations, payment reauth and Candidate pay-method-change. No named rate/TSFIN/finance-window/shared Weekly Source path matches those branches. The actual index invokes this predicate before the ordinary HTTP router at `198589`; only a true match reaches `assertLegacyWriter` at `198599` and typed HTTP410 retired / HTTP503 disabled or owner-unavailable at `198601–198606`. This is a source branch inspection, not a test invocation.

Actual DB winner `private.bpay_next_legacy_writer_fence_v1()` remains `supabase/repeatable/04102026_0550_banking_pay_next_legacy_writer_fence_v1.sql:32–64`, as the final-definer registry confirms. Its exact 33 protected public relations are listed at 37–53 and 104–120; `timesheets_financials`, `ts_financials_outbox`, `settings_finance_windows`, rates/defaults/overrides and factual Source relations are absent. It delegates to `bpay_next_assert_legacy_writer_v1()` (12–30), which holds the singleton FOR SHARE and raises `55000 / BPAY_NEXT_LEGACY_WRITER_RETIRED` for non-LEGACY. Existing callback winner `04102026_0555_banking_pay_next_factual_legacy_projection_boundary_v1.sql:12–28` holds the same singleton and returns false for NEXT/DISABLED, keeping factual changes while suppressing retired projection callbacks. Do not weaken either shared fence for WB-35.

## Each retained route / cron

| Clause | Exact current dispatch / winning Worker definition and called authority | NEXT source result / limits |
| --- | --- | --- |
| LSV-126 GET **and** POST `/api/rates/resolve-preview` | `broker/src/index.js:199962–199963` → sole `handleResolveRate` at `161373`; admin auth 161374–161375; GET query / POST body 161377–161379; actual Candidate override and unified default resolution 161409–161434 | Both verbs are false through the legacy-route predicate. Existing override/default/charge/channel semantics and real missing-rate/notFound response remain; no Banking receipt/write or owner selection is inserted. |
| LSV-127 POST `/api/timesheets/finance-preview` | Index `200117` → sole `handleFinancePreviewTsfin` at `173276`; admin auth 173277–173278; bounded unique ≤200 identities 173280–173282; actual current TF read 173284–173297; real Timesheet/Contract frozen settings authority 173302–173332 | False predicate. Current snapshots/frozen source policy drive preview; not a new approval/publisher and not a financial writer fence target. |
| LSV-128 POST `/api/tsfin/recompute` | Index `200124` → sole `handleTsfinRecompute` `172865–172879`; admin auth, ≤200 actual input IDs, actual `enqueue_ts_financials_priority` with MANUAL at 172874–172877 | False predicate. Genuine TSFIN outbox enqueue remains. No new NEXT decision or direct pay publication is made by enqueue. |
| LSV-129 POST `/api/tsfin/queue/drain` | Index `200123` → sole `handleTsfinDrain` `172853–172859`; admin auth, existing 1–500 clamp, actual `runTsfinWorkerOnce` call 172858 | False predicate. Actual worker is the sole definition at `166612`; targeted/batch dequeue at 166820–166829 and actual snapshot completion call at 167724. It has no NEXT-owner retirement guard. This worker was read only, never invoked. |
| LSV-130 GET `/api/tsfin/financials` | Index `200125` → sole `handleTsfinFinancials` at `172886`; admin auth 172887–172888; actual `timesheets_financials?is_current=eq.true` source 172893–172906, then existing scoped display joins | False predicate; factual read remains. Existing displayed bank fields are not a NEXT payment decision or new history projection; no scope change made. |
| LSV-131 GET `/api/settings/finance-windows` | Index `199785` → sole `handleSettingsFinanceWindows` at `3414`; admin auth; actual `settings_finance_list()` RPC at 3443 | False predicate; settings read remains. |
| LSV-131 POST/PATCH/DELETE finance windows | Index POST exactbase 199786; PATCH `/.../:id` 199788–199790; DELETE `/.../:id` 199793–199795 → same handler. Existing validation at 3463 onward; real `settings_finance_windows` POST 3506–3510, PATCH 3582–3588, DELETE 3607–3612 | Every actual verb/path is false through the legacy predicate; settings are not one of 0550's financial relations. Actual NX-147 statement policy triggers retained below; no broad settings exemption needed. PATCH/DELETE use the actual `/:id` route, not a fabricated base mutation. |
| LSV-132 `POST /api/weekly-source/v1/commands`, action RECOVER_FINALISED_PAY | Actual `broker/src/weekly-source/routes.js:624–638` → dependency bound in index `198701–198711` → `recoverWeeklySourceFinalisationPayProjection` in `weekly-source/finalisation-pay-orchestrator.mjs:1061`. Original actor/request checks at 1062–1072, explicit DB admission at 1074–1077, typed refusal 1080–1089 before factual OPEN at 1092 onward | Shared HTTP path is **outside** the global legacy route predicate. The separate exact recovery financial entry asks the DB: `public.bpay_next_source_legacy_finance_admit_v1()` final0715:5–29 returns true LEGACY, PT410/BPAY_NEXT_LEGACY_WRITER_RETIRED NEXT, PT503 disabled. This is deliberate SP-02/OR-45 behavior, not an accidental global route match. Source facts/open/task routines are retained; successful old-engine NEXT recovery must not be claimed. |
| LSV-168 TSFIN scheduled worker | Index `scheduled` at `201270`; separate `broaderScheduledTask` at `201576`; its existing settings gate 201577–201581; regular-cron TSFIN loop 201647–201654 calls same `runTsfinWorkerOnce`, no NEXT owner check | F-12 Banking Pay retirement applies separately at 201482–201499, whose actual NEXT branch calls `runBpayNextRecoveryTick`. It does not return from or fence the TSFIN task. Existing invpdf-minute skip 201615–201618 is cron ownership, not NEXT retirement. Existing database/settings availability limits remain; no claim every cron expression runs TSFIN. |

## Exact DB source owners and policy closure

The NEXT registry authoritatively identifies 0550/0555/0715 as above and these current reuse owners: `public.bpay_next_enroll_service_v1()` → `04102026_0560_banking_pay_next_queue_recovery_service_v1.sql`; `public.bpay_next_financial_job_v1(text,uuid,text,jsonb)` → `04102026_0280_banking_pay_next_financial_job_service_v1.sql`. These are Banking Pay owners, separate from TSFIN. Their presence does not prove a hosted queue journey.

The ordinary publisher is now actual registry winner `10102026_1009_banking_pay_next_ordinary_approval_rate_freeze_v1.sql`, reached by retained 1530 authorised-TF trigger and staged1430. Exact source provenance is already saved in `design/WB-01-ordinary-authority-chain.md`. No preview/recompute is a substitute for effective authorisation.

Retained upstream DB source definitions found by exact function-name census (these generic TSFIN/settings routines are not separately entries in the NEXT-only final-definer registry):

| Actual signature | Saved definition/source |
| --- | --- |
| `public.enqueue_ts_financials_priority(uuid[],public.ts_fin_reason_enum)` | `supabase/repeatable/04012026_enqueue_ts_financials_priority.sql:1` (real bounded input / import-correction unit expansion; no NEXT fence) |
| `public.tsfin_dequeue_specific(uuid[],integer)` and `public.tsfin_dequeue_batch_ids(integer)` | `supabase/repeatable/05012026_tsfin_batch_rpcs_part1.sql:19,93`; actual Worker wrappers `index.js:165297–165325`, original leasing fact table |
| `public.tsfin_write_snapshots_and_complete(jsonb)` | `supabase/repeatable/21072026_1235_38_tsfin_write_snapshots_and_complete.sql:4`; actual Worker wrapper `index.js:165371` and call 167724 |
| `public.tsfin_write_current_snapshot_single_bounded(uuid,integer,jsonb,uuid,timestamp with time zone)` | `supabase/repeatable/21072026_1235_37_tsfin_write_current_snapshot_single_bounded.sql:4`; retained factual snapshot owner |
| `public.settings_finance_list()` | `supabase/repeatable/06012026_settings_finwindow_eligibletimesshets_plus_recompute_totals.sqls.sql:109` |
| `public.weekly_source_finalisation_pay_open_atomic_v1(jsonb)` / `...task_recover_atomic_v1(jsonb)` | `supabase/repeatable/15092026_2336_weekly_source_finalisation_pay_orchestration_v1.sql:296,752` retained shared Source run/task facts; SP-02 entry admission precedes explicit Recover usage |
| `private.bpay_next_finance_policy_change_v1()` / `...current_valuation_policy_id_v1()` | Exact core migration `supabase/migrations/03102026_1324_banking_pay_next_core.sql:76,62`. Policy trigger captures actual settings windows in immutable new policy rows and advances only current policy control 85–94. Actual INSERT/UPDATE/DELETE/TRUNCATE statement attachments 98–109. Existing revision policy FK/default at148–149 retains earlier authority. |

No same-name later replacement of the named TSFIN/settings functions was found in the saved repeatable/recursive-include source census; baseline same-name definitions are predecessors of these saved repeatables. This is source evidence, not a fresh installed-body/ledger claim. SQL comments such as “review artifact only, not installed” do not prove or disprove the current hosted installation.

## Checklist coverage and precise remaining boundary

D01 publisher is reused at current1009; D02/D03 service owner source remains0560/0280; D04 DB fence remains0550; D06 actual HTTP predicate is legacy-fence162; D07 index NEXT queue/minute source is imported at55, dependency binding40997–41006, exact delivery41090 and minute201494; D05 background handlers are current `broker/src/bpay-next-background.js:53,102`, distinct from retained TSFIN cron. D08 current preparation and D16 command remain real imports/adapters (`index.js:46,48,198843,198869`); no dead origin/test module is treated as active runtime. D09–D14 are individually mapped in the table above; D15 output-only row is addressed by this exact census, not a new framework. Future snooze/week projection lanes from related rows stay with their named later cards; no noop hook or early workflow was authored here.

**Only confirmed wording conflict:** WB-35:1266/1270/1275/1279 says successful NEXT RECOVER_FINALISED_PAY remains. Current full SP-02:138/145/151 explicitly binds the same LSV-132 to a deliberate typed retired-writer refusal, states no old-engine Recover journey is required and cites OR-45/B4.4. Existing BD041 support implements that exact ruling. Preserve the registered0715 admission and shared factual2336 owners; describe this one entry as retained address/permissions + exact deliberate NEXT refusal. Do not add a NEXT exemption that restores legacy C1/finance or remove admission merely to satisfy stale parity wording.

All in-app/Worker→RPC/queue/minute timing/hosted-install acceptance remains NOT_RUN here. No new product mismatch, SQL successor, broader producer work or testing programme is recommended by this source-only census.

## Snapshot hashes

| Path | Raw SHA-256 |
| --- | --- |
| `broker/src/index.js` | `eb507f1fa44ae63b75f3c86a86dc5217753ad6fea5fd9321dcce55d70fad2c14` |
| `broker/src/bpay-next-legacy-fence.js` | `afdbdda4ac440ada0a6267523a4a60398ae47feec85fcf8772f0fb2a2c6dfef8` |
| `broker/src/weekly-source/routes.js` | `3641498765ed87d9d2c450e07cfc3dca05a9601725976d2e82e315f40dde4b11` |
| `broker/src/weekly-source/finalisation-pay-orchestrator.mjs` | `549e63d14c160eb1ad932089dc4febced99854f843bec7d36edb81f649dcfd0b` |
| `supabase/repeatable/04102026_0550_banking_pay_next_legacy_writer_fence_v1.sql` | `de1314ab23759fe8b584e6757bdca49addb18267d060aab544d98fc015bb0145` |
| `supabase/repeatable/04102026_0555_banking_pay_next_factual_legacy_projection_boundary_v1.sql` | `eba6b3596434aa6eecf133594579fb7833a7e3acdbe7d6fbd3676fbfb913db86` |
| `supabase/repeatable/10102026_0715_banking_pay_next_source_retired_finance_entry_v1.sql` | `358abee947f0b03aeba9cbbbcadc17f6a7fe54bc56260951592b8a2279470044` |
| `supabase/migrations/03102026_1324_banking_pay_next_core.sql` | `f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c` |
| `docs/bpay-next/final-definer-closures.json` | `2dc51e7f79fb45b97905af6bcdc39bef57d172c57e53c111af2ce2ab7b0fd8e4` |
