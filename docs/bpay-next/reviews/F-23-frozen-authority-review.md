# F-23 frozen authority: exact bounded source review

10 October 2026. Read-only follow-up D12 + D08 review. No product changes, gate/test/module execution, SQL/runtime/fixtures/hosted operations. Fixed F-01 source authority is `a6f7423eb71e6e7cd173e705c2290e5408a1d077`; all seven frozen files still match its raw Git objects.

## Exact eighteen trigger bindings

Trigger identity is `(owning qualified relation, trigger name)`, not name alone. Preserve timing, events, ROW/STATEMENT and actual callback signature. Four are in locked1324 and fourteen in frozen1342; none is declared by the other five frozen files.

| Trigger | Owning relation | Exact callback | Timing / events / level | Source |
| --- | --- | --- | --- | --- |
| `bpay_next_finance_policy_insert_v1` | `public.settings_finance_windows` | `private.bpay_next_finance_policy_change_v1()` | AFTER INSERT / STATEMENT | `supabase/migrations/03102026_1324_banking_pay_next_core.sql:98` |
| `bpay_next_finance_policy_update_v1` | `public.settings_finance_windows` | `private.bpay_next_finance_policy_change_v1()` | AFTER UPDATE / STATEMENT | `supabase/migrations/03102026_1324_banking_pay_next_core.sql:101` |
| `bpay_next_finance_policy_delete_v1` | `public.settings_finance_windows` | `private.bpay_next_finance_policy_change_v1()` | AFTER DELETE / STATEMENT | `supabase/migrations/03102026_1324_banking_pay_next_core.sql:104` |
| `bpay_next_finance_policy_truncate_v1` | `public.settings_finance_windows` | `private.bpay_next_finance_policy_change_v1()` | AFTER TRUNCATE / STATEMENT | `supabase/migrations/03102026_1324_banking_pay_next_core.sql:107` |
| `bpay_next_revision_guard_v1` | `private.bpay_next_work_revision` | `private.bpay_next_revision_guard_v1()` | BEFORE INSERT OR UPDATE OR DELETE / ROW | `supabase/repeatable/03102026_1342_banking_pay_next_guards_v1.sql:224` |
| `bpay_next_line_guard_v1` | `private.bpay_next_approved_line` | `private.bpay_next_revision_child_guard_v1()` | BEFORE INSERT OR UPDATE OR DELETE / ROW | `supabase/repeatable/03102026_1342_banking_pay_next_guards_v1.sql:228` |
| `bpay_next_detail_guard_v1` | `private.bpay_next_shift_detail` | `private.bpay_next_revision_child_guard_v1()` | BEFORE INSERT OR UPDATE OR DELETE / ROW | `supabase/repeatable/03102026_1342_banking_pay_next_guards_v1.sql:232` |
| `bpay_next_rate_detail_guard_v1` | `private.bpay_next_rate_detail` | `private.bpay_next_revision_child_guard_v1()` | BEFORE INSERT OR UPDATE OR DELETE / ROW | `supabase/repeatable/03102026_1342_banking_pay_next_guards_v1.sql:236` |
| `bpay_next_rate_schedule_guard_v1` | `private.bpay_next_rate_schedule` | `private.bpay_next_revision_child_guard_v1()` | BEFORE INSERT OR UPDATE OR DELETE / ROW | `supabase/repeatable/03102026_1342_banking_pay_next_guards_v1.sql:240` |
| `bpay_next_break_guard_v1` | `private.bpay_next_break_detail` | `private.bpay_next_revision_child_guard_v1()` | BEFORE INSERT OR UPDATE OR DELETE / ROW | `supabase/repeatable/03102026_1342_banking_pay_next_guards_v1.sql:244` |
| `bpay_next_work_pointer_guard_v1` | `private.bpay_next_work` | `private.bpay_next_work_pointer_guard_v1()` | BEFORE UPDATE OR DELETE / ROW | `supabase/repeatable/03102026_1342_banking_pay_next_guards_v1.sql:248` |
| `bpay_next_work_insert_guard_v1` | `private.bpay_next_work` | `private.bpay_next_work_insert_guard_v1()` | BEFORE INSERT / ROW | `supabase/repeatable/03102026_1342_banking_pay_next_guards_v1.sql:252` |
| `bpay_next_financial_effect_immutable_v1` | `private.bpay_next_financial_effect` | `private.bpay_next_effect_immutable_v1()` | BEFORE UPDATE OR DELETE / ROW | `supabase/repeatable/03102026_1342_banking_pay_next_guards_v1.sql:256` |
| `bpay_next_case_event_immutable_v1` | `private.bpay_next_case_event` | `private.bpay_next_effect_immutable_v1()` | BEFORE UPDATE OR DELETE / ROW | `supabase/repeatable/03102026_1342_banking_pay_next_guards_v1.sql:260` |
| `bpay_next_policy_immutable_v1` | `private.bpay_next_valuation_policy` | `private.bpay_next_policy_immutable_v1()` | BEFORE UPDATE OR DELETE / ROW | `supabase/repeatable/03102026_1342_banking_pay_next_guards_v1.sql:264` |
| `bpay_next_policy_no_truncate_v1` | `private.bpay_next_valuation_policy` | `private.bpay_next_policy_immutable_v1()` | BEFORE TRUNCATE / STATEMENT | `supabase/repeatable/03102026_1342_banking_pay_next_guards_v1.sql:268` |
| `bpay_next_policy_window_immutable_v1` | `private.bpay_next_valuation_policy_window` | `private.bpay_next_policy_immutable_v1()` | BEFORE INSERT OR UPDATE OR DELETE / ROW | `supabase/repeatable/03102026_1342_banking_pay_next_guards_v1.sql:273` |
| `bpay_next_policy_window_no_truncate_v1` | `private.bpay_next_valuation_policy_window` | `private.bpay_next_policy_immutable_v1()` | BEFORE TRUNCATE / STATEMENT | `supabase/repeatable/03102026_1342_banking_pay_next_guards_v1.sql:278` |

## Finite competing-authority checks

Reuse actual source spans, quoted-identifier normalization and canonical recursive execution order. Check **new/changed executable statements against the fixed F-01 source**, preserving exact existing baseline statements/includes and multiplicity. The frozen1342 DROP-then-CREATE trigger refresh is legitimate existing authority; do not reject it by a generic DROP-trigger regex. Comments, strings and archived/imported evidence are not executable changes. Dynamic construction that cannot be classified should be a finite source-review refusal.

| Actual new authority statement | Narrow refusal target | Reason / preservation |
| --- | --- | --- |
| CREATE [OR REPLACE] TRIGGER / CREATE CONSTRAINT TRIGGER | Any of the eighteen actual relation/name pairs, including a changed name/event/callback replacement after DROP. Also new use of a frozen callback outside its original fixed-base bindings. | A new sibling definer can replace/rebind frozen authority without touching the original file or its routine body. Preserve exact baseline occurrences and actual historical bindings; do not ban unrelated feature-owned triggers or all triggers on a shared relation. |
| DROP TRIGGER / ALTER TRIGGER RENAME | Actual frozen pair; trigger rename also loses the frozen identity. | Recognize optional IF EXISTS and exact ON relation; same name on unrelated relation alone is not the original object. |
| ALTER TABLE ... DISABLE TRIGGER / ENABLE [ALWAYS/REPLICA] TRIGGER | Named frozen trigger on its original table, or ALL/USER on a table bearing a frozen trigger. | Changes firing authority even with unchanged definition bytes. Preserve original baseline firing mode; ordinary ENABLE does not excuse a preceding new DISABLE. Parse each ALTER TABLE subcommand rather than only the first. |
| ALTER FUNCTION / DROP FUNCTION / ALTER ROUTINE / DROP ROUTINE | Exact frozen routine signatures already collected from all seven files. | A new SECURITY INVOKER, RESET/SET search_path, OWNER/grant authority, rename/schema relocation, DROP...CASCADE or overload-target change can bypass CREATE-only definer checks. Existing exact base ALTER OWNER/REVOKE/GRANT statements remain allowed. A broad new browser grant to a protected callback also requires existing ACL-source refusal. |
| CREATE TABLE / DROP TABLE / relation rename / SET SCHEMA | Exact forty-five core table identities below; DROP of a table bearing a frozen trigger (including shared public.settings_finance_windows) destroys the original binding. | A new same-object CREATE, replacement following DROP, rename or relocation is competing structural authority. Protect relation identity; do not ban all ALTER TABLE statements or new feature tables. Include DROP SCHEMA private/public CASCADE if ever proposed; that destroys named frozen objects too. |
| ALTER TABLE ... OWNER / DISABLE ROW LEVEL SECURITY / NO FORCE ROW LEVEL SECURITY | Actual frozen core relation authority, when differing from its exact fixed-base installed owner/RLS contract; shared public table authority remains its existing owner/security contract. | Distinguish structural/security weakening from feature additions. Logical postgres mapping remains the existing audited release adapter’s responsibility; do not replace it with a new owner convention. |

Do **not** turn this into a blanket ban on ALTER TABLE of every bpay_next relation. ADD COLUMN, ADD INDEX, feature-owned new relations/constraints and approved later targeted schema deliverables are governed by their exact cards. EX-16 explicitly replaces the original six-kind outcome check with one combined SCHEDULED/CANCELLED check and adds three nullable origin columns; the plan therefore does not authorize declaring every original CHECK immutable against all future cards. A newly destructive/rebinding statement is refused; feature-owned additive/replacement DDL needs its real named scope and existing review/contract gate. F-23 does not grant generic permission to weaken old constraints or alter original money fields. Avoid a new manifest/proof framework or broad Candidate/invoice schema census.

### Frozen core relation identities

These are the actual forty-five CREATE TABLE identities in1324. They identify competing CREATE/DROP/rename/schema authority; they are not an automatic prohibition on every feature-owned additive alteration.

- `private.bpay_next_module_control`
- `private.bpay_next_valuation_policy`
- `private.bpay_next_valuation_policy_window`
- `private.bpay_next_valuation_policy_control`
- `private.bpay_next_work`
- `private.bpay_next_work_revision`
- `private.bpay_next_approved_line`
- `private.bpay_next_shift_detail`
- `private.bpay_next_rate_detail`
- `private.bpay_next_rate_schedule`
- `private.bpay_next_break_detail`
- `private.bpay_next_source_chosen_detail`
- `private.bpay_next_position`
- `private.bpay_next_finance_case`
- `private.bpay_next_case_event`
- `private.bpay_next_financial_effect`
- `private.bpay_next_pay_run`
- `private.bpay_next_selection_page`
- `private.bpay_next_run_selection`
- `private.bpay_next_selection_candidate`
- `private.bpay_next_run_worker`
- `private.bpay_next_run_work`
- `private.bpay_next_run_position`
- `private.bpay_next_run_line`
- `private.bpay_next_hold`
- `private.bpay_next_case_hold`
- `private.bpay_next_net_projection`
- `private.bpay_next_transfer`
- `private.bpay_next_return_cash`
- `private.bpay_next_transfer_member`
- `private.bpay_next_transfer_outcome`
- `private.bpay_next_csv_instruction`
- `private.bpay_next_worker_control`
- `private.bpay_next_command_clock`
- `private.bpay_next_command`
- `private.bpay_next_run_command`
- `private.bpay_next_command_member`
- `private.bpay_next_paye_net_request`
- `private.bpay_next_outcome_request`
- `private.bpay_next_return_request`
- `private.bpay_next_reissue_request`
- `private.bpay_next_cancel_request`
- `private.bpay_next_publication`
- `private.bpay_next_enrollment_clock`
- `private.bpay_next_job`

## D08 actual provider tuple and off-state review

Fresh code has corrected its target to `private.bpay_next_rail_capability` and the module-owner exception to exact `public.bpay_next_owner_transition_v1(jsonb)`. Both exact-name corrections are right. F-17 PREVIEW remains read-only; only later owner-authorized APPLY owns material module writes.

**Current D08 still needs correction:** its second file-wide regex matches bare `enabled`, including the column name. Approved `INSERT ... (rail,enabled) VALUES ('PROVIDER',false)` and `UPDATE ... SET enabled=false` are consequently rejected. A true value in an unrelated statement can also be conflated with a rail-capability write. Classify the actual statement and tuple, not file-wide words.

EX-47’s exact contract is rail CSV/EXTERNAL/SIMULATED/PROVIDER, `enabled`, `enabled_by_authority_ref`, `changed_at_utc`. PROVIDER is disabled; CSV/EXTERNAL enabled are approved, SIMULATED is available only with the real TEST marker. No function/route/Office setting/Worker environment enables PROVIDER; separate owner AUTH-PROVIDER database change is outside shipped migration/repeatable/seed authority.

| Actual seed/default/writer shape | Gate result |
| --- | --- |
| Exact PROVIDER row enabled=false, including explicit disabled seed or disable UPDATE | Permit; absence of a true literal is not sufficient if DEFAULT/SELECT supplies the value. |
| CSV/EXTERNAL row enabled=true, with actual row identity closed | Permit approved availability; do not forbid all table writes simply because enabled occurs. |
| PROVIDER enabled=true / truthy known expression / ON CONFLICT setting true | Refuse. |
| CREATE/ALTER default enabled=true that can supply PROVIDER, INSERT omitting enabled when actual default is enabled, unscoped UPDATE enabled=true, MERGE branch enabling PROVIDER | Refuse or bounded unclassified-source refusal until exact row/value proof. |
| Finite mixed VALUES containing CSV=true and PROVIDER=false | Permit by actual column/value pairing; retain native row identity. |
| Dynamic SQL, INSERT SELECT or unknown expression whose PROVIDER enabled result cannot be proven off | Refuse unclassified; do not infer off from AUTH-PROVIDER string, credentials, environment or unrelated false literal. |
| Pure capability SELECT, disabled-provider guard, environment factual comparison | Permit; no network/provider execution. |

The TEST marker relation is independently created empty and never written by shipped source; its `marker boolean ... default true check(marker)` declaration itself is required EX-47 schema and is not a marker INSERT. Keep that distinction when scanning defaults.

## Source receipts

- `supabase/migrations/03102026_1324_banking_pay_next_core.sql`: `f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c`; raw equals F-01: True
- `supabase/repeatable/03102026_1342_banking_pay_next_guards_v1.sql`: `d29decd59c88c2620fe658b5d96f0deace19063cd0eaddf150ea7cf09c7b60af`; raw equals F-01: True
- `supabase/repeatable/03102026_1404_banking_pay_next_command_intake_v1.sql`: `be3c7dd17ecea6cbff631ed0b39c9b09947e6284b93db04ecdf517e320fed3e1`; raw equals F-01: True
- `supabase/repeatable/04102026_0555_banking_pay_next_factual_legacy_projection_boundary_v1.sql`: `eba6b3596434aa6eecf133594579fb7833a7e3acdbe7d6fbd3676fbfb913db86`; raw equals F-01: True
- `supabase/repeatable/04102026_0556_banking_pay_next_source_factual_legacy_boundary_v1.sql`: `64ef7e6d04e1b652af722cb9dd050310fcd8e1cd249f59b9359428a310b70933`; raw equals F-01: True
- `supabase/migrations/04102026_0504_banking_pay_next_factual_email_claim_index.sql`: `dc5fac7e18364a698ff35acbbc1656b76fc5077e532946d5a920b27d4a7dd308`; raw equals F-01: True
- `supabase/repeatable/04102026_0558_banking_pay_next_legacy_email_claim_boundary_v1.sql`: `fd5bc5ed576d81d02aade82b0111bb560e3ce51707259b76871b469f29e1fb9c`; raw equals F-01: True
- `scripts/bpay-next-source-gates.mjs`: `5d0fc832740c7af6d502d2b777d6944a71a9fd8468c45fedb650a9fc0bc7560d`
- `tests/bpay-next-source-gates.test.js`: `b844e0f30169eff8e3d97152256f5c47d2f3f1ecd4c5c16b405591f8cc2e1c82`
