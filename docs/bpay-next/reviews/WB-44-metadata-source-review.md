# WB-44 metadata SQL source review

10 October 2026. Independent read-only review of the uninstalled `1429` current-continuation migration/repeatable, `1438` origin-projection repeatable, original `1252` metadata/delta owners, full WB-44 card/all child rows, R2 section 5, W11/W12 and Rule-B owner scope. No SQL, tests, data, runtime, Worker invocation or product edits. Worker wiring is still pending and is outside this receipt.

Verdict: **changes required before installation**. The following are actual source paths, not hypothetical future producer requirements.

## MD01 — completed metadata pages leave Candidate summaries stale

`1429` repeatable:382–393 changes item/group name/reference/version, constructs before/after contributions and calls unchanged `1252` `apply_delta`. For a name or warning change these monetary/count contributions are identical. `1252`:109 filters every zero delta out of the loop. Its existing Candidate-summary path only copies metadata on INSERT:113–116; the UPDATE:119–120 changes measures and projection revision, not name/ref/warning/version.

Consequently the task can finish and adopt its metadata generation at `1429`:396 while Candidate-summary sort name, TMS reference, bank warning and metadata version are still old. Even a nonzero control-change delta does not refresh metadata on existing summary rows.

Required correction: bounded explicit Candidate-summary metadata propagation for the exact natural scopes represented by each visited group, including the real ALL scopes. Do not rely on a nonzero financial delta, and do not recalculate earnings. Complete the Candidate task only after those fields/versions are current as well as the item/group fields.

## MD02 — new forced-RLS policy excludes the mapped service owner

`1429` migration uses `for all to postgres` on `private.bpay_next_workbench_task`, then immediately inserts the SEED row. The owner mapper in `scripts/cloudtms-db-release-lib.mjs:112–144` rewrites `OWNER TO` and GRANT/REVOKE grantees, not a CREATE POLICY role. The actual non-BYPASSRLS mapped owner therefore has no policy on the forced-RLS table; the SEED INSERT and task access fail.

Use the existing owner policy spelling `to CURRENT_USER`, as in `1252` migration:290–305. Preserve public/anon/authenticated/service_role direct revocations and forced RLS.

## MD03 — unused personal-bank changes manufacture an Umbrella bank warning

`1429`:152–153 treats any Candidate `bank_details_hash` change as `p_bank_changed`, even when both old/new Candidate methods are UMBRELLA with the same Umbrella. `metadata_changed`:128–130 correctly selects the effective Umbrella hash but then unconditionally maps `p_bank_changed` to BANK_DETAILS_CHANGED. Updating that Candidate's unused personal sort/account therefore reports a changed effective bank identity although the Umbrella identity/hash stayed the same.

Use the actual old/new effective bank scope/identity. Retain genuine channel/Umbrella identity changes and actual personal-bank changes while that bank is applicable. Native bank-hash normalization remains the existing authority; do not add verification/provider state. A same effective name/ref/channel/Umbrella/hash assignment remains a no-op.

## MD04 — genuine pre-WB44 pending metadata can permanently block adoption

The retained `1252` name and bank-warning owners already set `continuation_pending=true` and increment `dirty_candidate_count` without a task, because WB-44 supplies their later consumer. `1429` migration creates only SEED. SEED_WORK/CASE/GROUP refreshes do not schedule or adopt these old pending metadata rows; `ensure_metadata` does not clear their pending flag. WAKE_PAGE discovers only task rows. SEED_DRAIN:419–420 then waits indefinitely for `dirty_candidate_count=0` although there is no runnable task able to clear it.

Add a finite pending-metadata seed/recovery lane using the real Candidate pending index; schedule exact tasks for current pending Candidates without one, including a Candidate with no current group. Complete through the normal metadata adoption/counter path. Do not blindly reset flags or dirty counts. This is recovery of existing projection state, not a business fixture or a history scan.

## MD05 — lease CHECK permits a partial lease tuple

The migration's `check((status='LEASED')=(lease_nonce is not null and lease_until_utc is not null))` allows a READY/DONE/ERROR row with exactly one lease field populated: both sides evaluate false. The operational functions currently set both fields together, but this is not the finite NULL-safe integrity constraint the declaration describes.

Require both non-NULL for LEASED and both NULL otherwise. This needs no new trigger/protocol. The existing exact natural task scope CHECK, typed Work/Candidate and revision FKs, revoked direct role access and mutable operational table are otherwise appropriate supporting shapes.

## Checked preserved boundaries

* Existing private Candidate name attachment is extended; the one Umbrella callback is the explicitly recorded WB-44 metadata supporting decision. Both return for inactive NEXT, compare actual OLD/NEW values, and do not invoke Source/query versions, new command kinds, F25 producers, finance windows or Contract rates. Retired generic `0555` callbacks are unchanged.
* Native Candidate/Umbrella hashes are reused. Warning vocabulary is NULL/MISSING/CHANGED; no VERIFIED or provider evidence is invented. MD03 concerns whether an actual effective identity changed, not the hash algorithm.
* Candidate-scoped scheduling owns Candidate control, retained metadata version and a task generation. Umbrella scheduling increments the global pending barrier before Candidate pages. Child metadata tasks keep the global barrier after the Umbrella task finishes. Whole page Candidate/Work/Case preholds precede the task lock and the page revalidates generation, epoch, nonce, step, phase and cursor.
* Origin tasks retain revision/applied revision/approval/FV/WAIT identity; a mismatch restarts the exact task. Their Case and Work phases are separate and bounded. Small wrapper sets prehold exact native scopes; large linked origin sets queue continued Work tasks, not first100 completion.
* Text comparison of the `1438` core against `1410` found only the function rename and removal of the two unbounded linked-Case lock/refresh loops. The classifier's monetary, accepted-selection, query, snooze, withdrawal and priority body is preserved. The new wrapper and refresh-many owner supply the bounded origin responsibility.
* Metadata/control refresh reads retained approved native facts through the current classifier/delta owners; it writes no native earnings, Case balances, position cursor, accepted Draft target or query snapshot. The Rule-B seal/CLAIM/PIN owners are unchanged.
* Seed has current native Work/OPEN-or-PAUSED Case and existing-group phases, stable fences and own-task exclusion on final drain. Failures retain the barrier and a token-free SQLSTATE. Those paths are source-reviewed only; MD04 is a remaining seed closure gap.

## Reviewed raw identities

| File | SHA-256 |
| --- | --- |
| `1429` migration | `c02f556274c18a0b2830e6c9631817ef27bbc001cd0370ccae2260c145c48034` |
| `1429` repeatable | `71c148636b745624f56f63b60927fb3070da5cea5b59e2d9704e55743b2dc60c` |
| `1438` origin projection | `4380deff20ea501ed2511f8392648c12550946f23aefc455c1fbd80028476b47` |

These hashes bind the reviewed saved source, not an installed/runtime receipt. Full WB-44, Worker wiring, application adoption/queue acceptance and joined journeys are not marked complete by this review.

## Fresh MD01–MD05 correction review

Re-read the current unlocked migration and all changed continuation functions. Raw SHA-256: migration `b3841d819ee8cc68ba7a69110687c2f82df22b659bcaa9266141042b9d8100a8`; repeatable `8a0f41f27fa17c6091651530a778fd675d42cf71fe76365faf665350ae4ca332`; `1438` origin projection remains `4380deff20ea501ed2511f8392648c12550946f23aefc455c1fbd80028476b47`.

MD01 is closed: METADATA_GROUP now advances to a separate METADATA_SUMMARY phase. It pages <=100 real Candidate natural keys under a matching C-collated expression index/fence, then copies name/ref/warning/version without touching measures. Summary key construction is injective over the actual UUID-or-ALL client scope and finite channel/section enums; its maximum size fits the existing cursor bound. Newly inserted scopes copy current metadata through the retained delta INSERT. Existing summary scopes present at scheduling are inside the retained fence. Adoption follows summary exhaustion, and a newer metadata generation resets/revalidates the whole task.

MD02 is closed by the CURRENT_USER policy. MD03 is closed by ignoring personal-bank hash changes while UMBRELLA remains the effective bank scope; actual channel/Umbrella changes and applicable personal-bank identity changes remain observed. MD05 is closed by the CASE CHECK requiring both lease fields together or both absent.

The retained pending counter is preserved: metadata mutation captures the old `continuation_pending`, sets the new pending flag and passes `p_previous_pending` to scheduling, so a previously clean Candidate increments dirty count once and an already pending Candidate does not increment again. `through_candidate_id` now carries the actual Candidate fence rather than overloading the Work UUID field. Wake discovery uses both wake and available-at timestamps.

**MD04 remains open for one finite genuine retained-row case.** The new SEED_METADATA phase correctly discovers pending Candidates by the existing pending index and schedules normal metadata tasks. However the original `1252` name callback:691 only performs `worker_control FOR UPDATE`; it neither inserts a control nor checks FOUND before creating metadata and incrementing dirty count. An actual name change on a Candidate with no NEXT Work/Case can therefore leave a pending metadata row with no worker_control. SEED_METADATA discovers it, but current `1429`:348–349 rejects CONTROL_MISSING before reaching `schedule_task`, which would insert the missing metadata-support control. The rejection is also before the inner page error handler.

Initialize the exact metadata-only Candidate control before its sorted SEED_METADATA prehold, consistently with the current metadata callback/scheduler. Preserve missing-control refusals for native Work/Case scope. A Candidate with no groups then completes empty GROUP/SUMMARY phases through normal adoption and decrements the existing dirty count; no synthetic Work, Case, earnings or finance row is needed.

Verdict on this fresh source: **MD01/MD02/MD03/MD05 closed; MD04 requires the retained no-control recovery correction.** No execution or broader protocol requirement is introduced.

## Final retained-row recovery closure

Fresh source reread: `1429` repeatable SHA-256 `eb403f3d76f15e0d6c6671ff69dd4d8467e816319b1daac9a2ae60628e3e3815`. Migration remains `b3841d819ee8cc68ba7a69110687c2f82df22b659bcaa9266141042b9d8100a8`; `1438` origin projection remains `4380deff20ea501ed2511f8392648c12550946f23aefc455c1fbd80028476b47`.

MD04 is closed. Only the SEED_METADATA branch now inserts the exact Candidate control with ON CONFLICT DO NOTHING before each sorted Candidate prehold (`1429`:350–354); native Work/Case missing-control checks remain. Scheduling retains an already counted pending flag, and empty group/summary scope still reaches normal task adoption. The bootstrap Candidate fence is drawn from the actual pending index; later new metadata events already have their own tasks. There is no blanket reset or synthetic native Work/Case.

Retained mapping initialization conservatively sets MISSING from the actual applicable native NULL hash, keeps an existing nonmissing warning otherwise and is limited to the uninitialized retained mapping. It does not invent CHANGED or verification. SEED_DRAIN now locks the singleton revision before rechecking exact pending/dirty counters; its own task finish, count decrement and CURRENT adoption share that transaction. Task generation and metadata-version revalidation still precede summary adoption.

**Final source verdict: MD01–MD05 closed; PASS for this reviewed metadata/schema/origin-continuation subset.** No remaining concrete defect was found in the corrected paths reviewed here. Worker wiring, release compilation, actual data/queue recovery, first-use seed adoption, later event hooks and whole WB-44 completion remain outside this source-only receipt.

## Scoped snooze/week due support and full cached-metadata seed supplement

Read the fresh migration/repeatable after the D04 support additions, the retained native `1100` Snooze schema/action/guard, the new due and seed paths, and the current `1438` origin closure. Raw SHA-256: migration `c89b160ebb66275b9f60a6e969b7ab15a961750d56b57d95447ed4d5c53d5978`; repeatable `dc539089fae0cd1a8baf4765a89cea9ff1d1e8039200ecda5d80371dda2335dd`; `1438` `b8d445c4a4cfc02978e8ac77a159350b0e1978aaff9a1172da8e7e157a6414f5`.

One fresh seed issue was identified and corrected before this receipt: **MD06**. The initial typed name/control comparator omitted the genuinely observable case where an effective native bank hash became NULL while NEXT was inactive but the cached mapping remained known and its warning stayed NULL. Current code:419–427 also compares actual effective-hash missingness against the cached MISSING warning, then invokes normal metadata mutation with `p_bank_changed=false`. Thus it establishes actual MISSING without inventing historical CHANGED or verification. MD06 is closed.

Checked new support:

* The separate private Snooze-due record holds the real native Snooze UUID, version, exclusive Work/Case, date and mutable pending watermark. Its exclusive-scope CHECK is NULL-safe; actual native FK types match. FORCE RLS/CURRENT_USER owner policy and direct browser/service revocations are preserved. The new operational watermark is not stored in native Snooze, native event snapshots or an accepted financial receipt.
* The private AFTER callback copies only actual saved date/version/active operands. The retained native guard makes Snooze identity/Candidate/Work/Case scope immutable (`1100`:353–355), so the copied origin scope cannot silently migrate. Equal version/date/active UPDATEs are no-ops. CLEAR removes expiry pending, and SET/CLEAR also put the real Work into the existing Processing/ORIGIN barrier or refresh the real Case in the same transaction. A later failure of the native action/event assertions rolls all these metadata writes back together.
* The native setter already inserts/locks Candidate control and preholds Work or its actual Case component before receiving its existing command (`1100`:208–215). Precisely, the SET prehold is a Case **component**, not a finance-case header; the projection Case helper acquires the header under the already held Candidate control. No second command, command kind, native balance, event mutation, snooze expiration/clear, Source query producer or F25 protocol is introduced by the callback.
* Fenced SEED_SNOOZE pages walk <=100 current active native UUIDs, derive and prehold their complete Candidate/Work/Case scope before task/metadata writes and initialize only their private due records. Native Snooze SET/CLEAR owns the same Candidate serialization, so a seeded receipt cannot overwrite a concurrently newer version behind that lock. Indefinite records are retained with pending=false and are not fabricated as an expiry.
* DUE_PAGE discovers at most100 distinct real current Work/Case keys from bounded due-receipt and FUTURE_WEEK prepared-row inputs. It preholds the whole sorted Candidate/Work/Case set before due receipt locks, queues a durable Work origin barrier or performs the Case projection, then consumes only <=100 matching receipt UUID/version pairs. Native snooze date semantics are inclusive, so `until_date < London today` is the correct expiry predicate; the existing future-week predicate uses `eligible_from_date <= today`. No unvisited receipt is acknowledged. Lost replies retain task/current-projection evidence and consumed watermarks; no external effect is performed here.
* Seed metadata now walks the complete **existing current metadata cache**, not only old pending flags. It compares actual name/ref/channel/Umbrella/VAT/enabled and the factual MISSING condition only after complete Candidate preholds. It schedules normal GROUP/SUMMARY adoption even for zero deltas. The UUID upper fence and <=100 pages avoid a public Candidate or historical sweep; new concurrent metadata events still supply their own generation/barrier. A retained no-Work Candidate gains only its metadata-support control, as previously reviewed.
* The separate bounded C-collated summary phase remains mandatory before metadata task finish. Singleton counters are locked and rechecked before seed CURRENT adoption. The changed `1438` core still text-compares to `1410` with only the rename and removal of the two unbounded linked-Case loops; the retained financial classifier is unchanged.

**Updated source verdict: PASS for the reviewed metadata, cached seed and scoped Snooze/week-due SQL subset; MD01–MD06 closed.** No further concrete defect was found in these saved additions. This does not mark the Worker due/continuation lane, installed native trigger behavior, complete queue delivery, application adoption or whole WB-44 acceptance as tested or complete. No SQL/tests/runtime/data execution or product edit was performed by the reviewer.
