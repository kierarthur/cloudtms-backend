# F-25 Rule B final checkpoint review

10 October 2026. **PASS for saved source/checkpoint consistency and the bounded parking/retention/registry/contract delta. No actionable source checkpoint defect found.** Fresh local PostgreSQL17 compilation/sealing is parent-reported evidence; this reader did not execute SQL, database/runtime/tests or change product/checklists. This does not independently review the four Rule B consumer bodies or claim whole application validation.

BUILD-BE `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-backend-20261009`; HEAD `8eadb2e204ae33ca43f0cbb95bf2a734d727e130` with pending forward source changes. Current saved bytes, parent parking manifest, current registry/contract/checkpoint, HEAD Git objects and pre-F25 contract at10b001ab inspected. Only this review artifact was written.

## Parking, retained inputs and exact source binding

- Parent manifest has37 unique exact former repeatable paths; all37 are absent. Thirty committed removals retain HEAD history; seven formerly untracked sources retain manifest hashes/design-review evidence. New0151 Rule B migration/repeatable are present and explicitly outside parking.
- All nine committed F25 migration files and all nine committed lock rows remain unchanged. LF-canonical file SHA equals HEAD and current lock for every retained migration.0128 SQL and only its uncommitted lock append remain absent. The current lock adds only the new0151 migration against HEAD.
- Both2153 and2256 repeatables match HEAD exactly. The release library matches HEAD exactly, including the path/hash-bound2153 executable logical-owner mapping and existing immutable migration/protected-boundary checks. No owner/grantee exception was broadened.
- The fresh checkpoint inventories308 migrations and808 repeatable closures. Independently recomputed saved-source hashes match **all308 migration hashes and all808 recursive path+NUL+LF-source+NUL closure hashes**; no parked path appears. This is source reading/hashing, not execution of a release gate.
- The726 registry chains have no parked finalRoot/finalPhysical/allowedPhysical/predecessor/finalClosures reference, and every declared final root/physical file exists. Four Rule B chains point to0151: seal_selection,claim_prepare_job,pin_selected_work,prepare_case_page. Other original authority chains survive; unused2352 migration-defined producer guard remains available through its locked schema.

## Saved contract delta

Canonical JSON SHA, using the exact `JSON.stringify`-equivalent field/order representation used by the release library, is **`a0177a8200237bc983e8eb8cc303b97243299903ef18829a296746fc1af2753a`**, matching the parent’s reported fresh seal. Raw file SHA is separately recorded below.

Compared with the committed HEAD contract:

| Section | Before → current | Added | Removed | Changed |
|---|---|---|---|---|
| Routines |2174 →2117|0|57|26|
| Triggers |721 →705|2|18|0|
| Relations |520 →521|1|0|3|
| Policies |293 →293|0|0|0|
| Schemas/enums/extensions/default ACLs |unchanged|0|0|0|

The18 removed triggers are the former settings entry/stage/deferred flush, Query owner/certificate assertions and canonical manual-review/incident/comparison capture hooks. Their deferred-trigger constraint entries also disappear from input_transaction/query_producer. No evidence table is removed. New private run_work_query_snapshot adds exactly its two immutable/no-truncate triggers; selection_candidate adds four nullable captured fields (channel,Umbrella,VAT,floor) and its Umbrella FK. These changes follow the new0151 schema.

All57 removed routines are private parked protocol helpers. The26 changed existing definitions comprise original owner restoration plus the bounded Rule B consumers. Eleven restored Source/Query/Candidate owners below match the pre-F25 contract at10b001ab **as complete routine records**, including definition hash, owner, config, security-definer mode and ACL—not only name. This confirms the saved fresh contract no longer carries the former producer hooks in these owners.

| Restored exact routine identity | Current definition SHA-256 |
|---|---|
| `private.bpay_next_source_book_guard_v1()` | `966644847728a8df24b03f860b5a26f756b1e0963ebf32433cab836606b10c0b` |
| `private.weekly_source_candidate_submission_compare_sync_v1(p_candidate_generation_id uuid, p_projection_publication_id uuid, p_membership_id uuid, p_timesheet_id uuid, p_timesheet_hash bytea, p_now_utc timestamp with time zone)` | `5b770cd46179f483f967e597430fe405983cdb327a5e6e274976259e3b4b570b` |
| `private.weekly_source_manual_reviews_finalised_v1(p_final_revision_id uuid, p_actor_user_id uuid)` | `f663fd9077ca4ff7204a429939c640ccccd1f801fce5c4e0ac7d838537f6b8a4` |
| `public.weekly_source_candidate_app_submit_atomic_v1(p_session_id uuid, p_environment text, p_request_id uuid, p_body jsonb, p_now_utc timestamp with time zone)` | `5720d54afec37190f2a9f607813c901f85662c6d105246e8f32806c2e505e804` |
| `public.weekly_source_candidate_response_submit_atomic_v1(p_request jsonb)` | `14712e8468bce969c72270506203ad1c876063fb5f195fc05c064ca17fa5b73e` |
| `public.weekly_source_manager_review_respond_atomic_v1(p_request jsonb)` | `b33c954970367494fe6ac310726481aca8c0dba03e41459c33a750cc4b7035ec` |
| `public.weekly_source_manual_review_open_v1(p_request jsonb)` | `0342417b83a6adf40523ed9280c2def0d81d424476583c36abc5b9e2179706fb` |
| `public.weekly_source_manual_review_resolve_v1(p_request jsonb)` | `321e69bf8f8e0c3f392e05ada081c28c1b22423a5ce7937872592c75d53f45d4` |
| `public.weekly_source_office_bulk_query_action_atomic_v1(p_request jsonb)` | `87de49e4416b193a865dc9d5e62cb352ddc82a9412e4c9d51f196e847c7e8365` |
| `public.weekly_source_query_accept_system_hours_atomic_v1(p_request jsonb)` | `cdc706df8dfdb46d51b12a872bea3be660faca6e2d09d70ff0324edca2147f78` |
| `public.weekly_source_query_sync_atomic_v1(p_request jsonb)` | `77de73a8bcbc177172e8eb15c5d6d6e24f0ff364c92829680bfe528463a3620e` |

Schemas, policies, enums, extensions and default ACLs are unchanged. Retained private producer/event/evidence tables and new private snapshot table remain postgres-owned, RLS enabled and owner-only ACL; no browser/service table grant is introduced. Runtime roles or an installed hosted contract were not inspected.

## Checkpoint interpretation

LOCAL_SCHEMA_CHECKPOINT is a NEW local compilation receipt whose selected/inventory source entries include exact0151 and omit all37 parked roots. Its context.expectedHash remains the former pre-seal input hash `b05d6b9c15b5ed01d32260557bb6ee582e3ccb7097b6458d5ae1eaface157a5a`, and fullProtectedVerification remains PENDING. Treat those fields as the compilation input/context, not a current full-security verification or final seal receipt. The current saved contract’s canonical hash above is independently verified. SourceCommit is the prior HEAD while working-tree input hashes are exact; that is expected before the owner-authorised forward commit.

The parent reports the sole empty task-local PG17 was rebuilt, compiled and sealed. This review reads its source/contract receipt only and makes no business-call, fixture, timing, hosted, payment/provider/bank/email/drain or application claim. Checklist writes remain with the builder.

## Current hashes

Raw SHA-256 hashes exact saved bytes. Migration/closure canonical hashes use the release’s LF convention; both are distinguished where needed.

| Exact path | Current raw SHA-256 |
|---|---|
| `docs/bpay-next/F-25-PARKED-FILES.json` | `653d85743e850f639506d24740adeea52686ce8cf4ad4519b75ac35cfdc1cca0` |
| `docs/bpay-next/final-definer-closures.json` | `875f3b481cdd9e617a408ff49ff66556f14714ae7b5d1de421351858d149c28a` |
| `supabase/release/current-contract.json` | `b78d9737b4724d8d809a3f4c3ee90742985205193d2c04e7ab44724b9d9c6f07` |
| `docs/bpay-next/LOCAL_SCHEMA_CHECKPOINT.json` | `d4c3770b0c085f29925450d2907d832d33f47d9ebe34c89f2d337f7af118081d` |
| `supabase/release/migration-lock.json` | `f0a8dfc8d131b97e56530f9c2ed92727e2943b2e6786550f2aa6a4d346d3ee33` |
| `scripts/cloudtms-db-release-lib.mjs` | `a5c79ae9d95b5e835e0c84fd876cad45bd8f5b31f8c1bafa95c76d6578afb4bf` |
| `supabase/repeatable/09102026_2153_banking_pay_next_accepted_input_v1.sql` | `4462d4e9a45909ba5e428b1916f74a7eeacf99b73add469b64eae8f185e28810` |
| `supabase/repeatable/09102026_2256_banking_pay_next_accepted_case_week_guards_v1.sql` | `5226a8c39e53f7900a1dfd6a466b8991b51cdff2c8095c52c300825d759dfd6f` |
| `supabase/migrations/10102026_0151_banking_pay_next_rule_b_selection_snapshot_v1.sql` | `7e270d8bdeed7f2c5f52432b13273a107d2b91ca3f3d8ee6dbc2e1adcd0c9645` |
| `supabase/repeatable/10102026_0151_banking_pay_next_rule_b_selection_consumers_v1.sql` | `2698331479177a246d2ee581f71aa1514a1479f05bf6258c1ceb1f4f490104cf` |

Nine retained committed migration LF/lock hashes:

| Exact path | Unchanged canonical SHA-256 |
|---|---|
| `supabase/migrations/09102026_2153_banking_pay_next_accepted_input_v1.sql` | `0289b53ad745d5ff5c3a3130ce596e0b5a155e3645a84f1c57b154f8774a5182` |
| `supabase/migrations/09102026_2225_banking_pay_next_accepted_input_provenance_v1.sql` | `c7d79426059729a7f85c787120fdc371ef312f9b832ba6576a13d33139a086c3` |
| `supabase/migrations/09102026_2244_banking_pay_next_input_transaction_v1.sql` | `4fe014fe836d3474c18d77670788814754384a6fab7b889b557a6af55e0e2191` |
| `supabase/migrations/09102026_2300_banking_pay_next_accepted_case_week_terms_v1.sql` | `ad70ce3e0a7531d52209f0566412628f0c5c59a03fa63a534fab2535b3b950ef` |
| `supabase/migrations/09102026_2352_banking_pay_next_query_producer_protocol_v1.sql` | `d6dfda1236deeddf6f7f6f1f720ff95ff4521d605dd93260c574307cf1842be1` |
| `supabase/migrations/10102026_0020_banking_pay_next_query_root_completion_v1.sql` | `bf36da5429faebb2d02995ab24f1dc8538e83e8587a7273ab8acb62aaa6c60bf` |
| `supabase/migrations/10102026_0036_banking_pay_next_query_sync_branch_evidence_v1.sql` | `9acb438e59f38cd2cca753f03a8dd10b8ef1ba0f63c0e4d435a2badd9a440a36` |
| `supabase/migrations/10102026_0057_banking_pay_next_query_witness_envelope_v1.sql` | `784c3cb5b26b9fe447e4d5e2983c3fe9b6d4c3e1e833a950d46d96c8bc1e1cbc` |
| `supabase/migrations/10102026_0111_banking_pay_next_candidate_query_completion_v1.sql` | `76d1e2909770819fe2b8e872f9fb958a8144404bbc98a07b3dc60db7010fc1b3` |

No protocol investigation was resumed. No product edits, SQL/runtime/tests, checklist changes, network/hosted operations or commit/push occurred.
