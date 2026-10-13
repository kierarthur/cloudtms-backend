# F-06 foundation import dependencies

Read-only investigation, 9 October 2026. Re-read F-06 and its twelve checklist deliverables, the desktop override, relevant release-bible ordering/owner rules, all twelve staged input inventories and their dependency-bearing sections, and current BUILD-BE definitions. No product files were changed and no SQL, database, runtime, tests or fixtures were executed.

## Scope and exact inputs

Staged source root: `C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/banking-pay-reset-implementation-20261003/supabase`.

Build root: `C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-build-backend-20261009`.

The F-06 card is `01_BUILDER_RULES_AND_FOUNDATION.md:550–590`; checklist F-06-D01–D12 describes four new canonical migrations and eight carried/split repeatable inputs. It requires one release unit with F-05, not isolated first-use of a subset. Import the saved RESET inputs, not UMM comparison overlay or correction after bodies.

| Card row | Staged path beneath source root | Lines | Raw SHA-256 | Dependency / handling |
| --- | --- | ---: | --- | --- |
| 1 | `migrations/06102026_1245_banking_pay_next_source_book_binding.sql` | 250 | `8F8138B346AE119D6B890C30BA275F3A8A9D2A927F712E36D1225C998C7E5E13` | Re-scaffold first; installed core/Source receipt/head/publication tables. Creates binding tables and source-book/capture provenance. |
| 2 | `migrations/07102026_0018_banking_pay_next_initial_tsfin_umbrella_work_shape_v1.sql` | 64 | `0AEC5B7A88BAD3E7393F80A5348CC18C4420C6B564E10695B687A81B62ED5E99` | Re-scaffold second, after row1; origin columns on binding version and typed Umbrella WORK cash constraint. |
| 3 | `migrations/06102026_0236_banking_pay_next_channel_page_index_v1.sql` | 6 | `C32B271ABCDD6A649F923438209F4D07EE5DC71BD75EB065499451294E7964FE` | Re-scaffold third; core run-worker table, index `(run_id,target_pay_channel,candidate_id,id)`. |
| 4 | `migrations/06102026_1206_banking_pay_payment_history_scope_indexes_v1.sql` | 15 | `638646FB2C93C095C4EFAD139603C624A44356A650F50BD4BD61D40F7388370B` | Re-scaffold fourth; case event `(case_id,occurred_at_utc,id)` and command member `(candidate_id,command_id)`. |
| 5 | `repeatable/06102026_1246_banking_pay_next_source_book_binding_v1.sql` | 708 | `2E9A297FF362E41DBFBADC869B211B37176CF76D06EDD3E78808A2E4C2A57340` | Nine private functions, source-book/binding triggers; both migrations1/2 must already exist because snapshot uses initial-origin fields. |
| 6 | `repeatable/06102026_1301_banking_pay_next_source_receipt_binding_v1.sql` | 103 | `24CF44354735D4A452EB3334847294E486EE17905BDC085F402E77B7ED1D75D9` | Complete successor of existing1570 receipt trigger; needs1246 and existing stage/pair publisher. |
| 7 | `repeatable/07102026_0018_banking_pay_next_initial_tsfin_daily_evidence_v1.sql` | 365 | `FA256BFA14B93321E1749C7CBEE2505BACA4F4DA2EEF5D11EA6457BDFAE120E2` | Four private evidence/binding helpers; needs both binding migrations,1246 and existing Source I-1/context/rate helpers. |
| 8 | `repeatable/06102026_0236_banking_pay_next_channel_draft_readers_v1.sql` | 105 | `0A0FC2FB257A7E6FA6DC418ECA762E451A8B087EBCEC67CF9500174C92B88F93` | Three private readers; new channel-page index, existing worker-status reader; later current caller successors belong to same F05/F06 unit. |
| 9 | `repeatable/06102026_1206_banking_pay_payment_history_page_v1.sql` | 416 | `D963C739F743E1526647EF101DB7B9460DAD88BE3E2F56C2775C199AEDDC2C73` | New public history reader,52 closed sections; scope indexes and existing NEXT tables. Explicit unavailable labels for not-yet-built comparison material. |
| 10 | `repeatable/06102026_1157_banking_pay_shared_selected_quote_v1.sql` | 998 | `2870041DA540F108671F9DF9C0952DB05430E8CCBF8EB076428FDB03920B8343` | **Three definitions**, including two existing successors; see below. |
| 11 | `repeatable/08102026_0718_banking_pay_next_frozen_run_read_safety_v1.sql` | 67 | `0A884C8CB2568CA568AC51069FCF8FAC945159A7F3F4D688D57B1731C295FEAE` | Four exact immutable/no-truncate attachments; existing effect guard; idempotent cancel-history index with conflict preflight. |
| 12 | `repeatable/08102026_0549_banking_pay_next_overpayment_week_applicability_v1.sql` | 864 | `9E2488857DC7AD0823B172E2C4EFBC9EEF6687C0300392ED812E7A99E88F027E` | Raw comparison-dependent reference only. The current F07 ordinary successor is `supabase/repeatable/09102026_1958_banking_pay_next_ordinary_case_week_applicability_v1.sql`; do not copy raw0549 into release inventory. |

All hashes above are raw saved-file hashes. Release migration/closure hashes canonicalise CRLF to LF, so they need not equal raw hash values.

## Canonical order and creation-time dependencies

The release library `sqlFiles()`/`inventory()` includes saved tracked and untracked SQL and sorts each kind using `sqlDateKey()` (DDMMYYYY is normalised to YYYYMMDD), then filename tie-breaker. Migrations execute before repeatables. Ordinary lexicographic filename sorting is incorrect.

Re-scaffold the four migrations in **card row order**1245→0018→0236→1206 at current UK minute prefixes which strictly increase. The generator has minute resolution; same-minute files would sort by suffix/name and are not proof of this dependency order. Add original staged-path provenance and append migration locks normally; never change earlier migration bytes or re-lock an existing hash. All new source-book columns/defaults are schema-only; do not run a seed, backfill or receipt publication as part of import.

Keep existing repeatable names under the card's explicit B2.6 exception unless F04 requires a later successor date. Their actual canonical order is:

`06102026_0236 readers → 06102026_1157 shared quote → 06102026_1206 history → 06102026_1246 binding → 06102026_1301 receipt → 07102026_0018 initial/Daily → 08102026_0718 frozen safety → 09102026_1958 F07 ordinary case-page`.

The card's table is a logical dependency table, not the repeatable execution order. Most calls are PL/pgSQL and late-bound:1246 snapshot may call the later0018 initial assertion; current caller successors0575/2359 may call0236 readers;0557 may call initial binder. The complete F05/F06 unit must close those runtime edges before first use. Current BUILD-BE0575 still calls its existing worker-status reader and has no channel helper call; wiring the exact caller successors is F05's work, not a reason to invent readers or change F06 APIs.

One exception to the card's blanket “all repeatables LANGUAGE plpgsql” wording:1246 `_source_binding_components_v1(uuid)` is **LANGUAGE SQL** and therefore has a creation-time reference to `_source_binding_members_v1(uuid)`. Members is defined earlier in the same1246 file, and its table types exist after migrations1/2, so the existing order is safe. Preserve those two definitions' order; do not split SQL-components before members. Other trigger attachments resolve their target functions at apply time, so guards must precede attachments within their files.

## Existing-function successor handling

The following functions already exist in BUILD-BE and must remain complete final successors, not duplicate stubs or edits to old migrations:

| Signature | Current BUILD-BE authority | F06 final physical/root authority |
| --- | --- | --- |
| `private.bpay_next_source_receipt_publish_v1()` | `03102026_1570_banking_pay_next_source_receipt_hook_v1.sql` | `06102026_1301_banking_pay_next_source_receipt_binding_v1.sql` |
| `private.bpay_next_value_source_for_target_v1(numeric,text,text,boolean,uuid,date)` | `03102026_1500_banking_pay_next_target_valuation_v1.sql` | `06102026_1157_banking_pay_shared_selected_quote_v1.sql` |
| `public.pay_finance_component_resolutions_apply(uuid,jsonb,uuid,uuid,text)` | `04092026_0910_banking_pay_finance_saved_resolution_evidence_v1.sql`; old include/root26052026 also defines a predecessor | `06102026_1157_banking_pay_shared_selected_quote_v1.sql` |
| `private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer)` | F07 ordinary final1958, predecessor0360 | **Keep F07 ordinary final1958**, not raw0549 or its comparison capacity helper. |

The current F04 registry still chooses1570,1500 and04092026_0910 for the first three at this read. When importing, update each registry record's allowed physical sources, predecessor set and canonical final root/physical/closure. In particular, public resolution-apply is already a gated chain and must also move to1157. Ensure changed predecessor closures force the final1301/1157 closure in changed-only apply; a full NEW-only ordering success does not close that risk.

All other F06 function signatures were absent in the current BUILD-BE source census. The frozen effect guard itself remains the existing1342 authority;0718 adds attachments, not a new guard function.

## Source binding, receipt order and permissions

Migration1245 creates private `bpay_next_source_component_binding` and `_binding_version`, original component identity and direct version FK links. Both tables have ENABLE/FORCE RLS, owner-only policies built for `current_user`, and all table rights revoked from PUBLIC/anon/authenticated/service_role. Circular current-version links are deferrable initially deferred; initial bind intentionally inserts version then binding in one transaction. Preserve those deferred constraints and actual canonical Source component identity.

Migration0018 widens receipt columns to nullable **only** under the exact `INITIAL_AUTHORISED_TSFIN_V1` origin shape. It requires real root authorisation/financial snapshot/version/generation/signature and six32-byte evidence digests, matching approved/classification line and single publication. Common HEAD receipts retain all original nonnull requirements. Its Umbrella WORK net-projection constraint requires ex+VAT=inc, NULL PAYE NET, no Case additions/deductions/recoveries and cash=inc; do not relax it incidentally.

1246 provides these exact nine owner helpers: `source_binding_members`, `source_binding_components`, `source_binding_lockset`, `source_component_bind`, `source_book_seed_proof`, `source_disposition_seed`, `source_book_guard`, `source_binding_guard`, `bound_source_snapshot` (all `bpay_next_*_v1`). All are SECURITY DEFINER with `search_path=pg_catalog,private,public`; components alone is SQL. All owner-only signatures are revoked from all four public/browser/service roles.

The seed helpers are installed definitions only. They require an exact applied SOURCE revision and qualified typed original paid/held witness or actual zero basis, never a history SUM. Installation performs no seed call. The book guard derives native bucket changes once from actual scalar updates, refuses caller cache edits/revision changes and remains separate from any comparison certification. The bound reader returns explicit pending/stale/posting-required states, `selection_state=UNBOUND` even when its identity/book is READY, and does not authorise AUTO/manual comparison selection.

1301 is a metadata trigger successor, dormant when active owner is LEGACY. It sorts exact one/two receipt members by family booking/root before stage locks, stages all members, checks Candidate identity, obtains direct original+member binding lockset **before** publisher/agency clock, publishes the pair, and binds each actual current/prior component. Each bind must return REGISTERED with exact canonical component identity. REGISTERED is not APPLIED or financial acceptance. It reasserts the existing AFTER INSERT receipt-table attachment in the same changed closure. No comparison register call is present in this F06 version.

0018 evidence helpers preserve the existing Source I-1 chain (`weekly_source_effective_inventory_v1`, `weekly_source_inventory_approval_basis_v2`, `weekly_source_sha256_jsonb_v1`, source command ID). Certificate proves actual initial TSFIN and sealed Banking revision; metadata bind/replay does not fabricate enrollment/job/position. Initial bound assertion rechecks that exact certificate. Daily evidence uses existing `tsfin_load_context_batch`, `tsfin_resolve_rates_batch` and `_candidate_daily_rate_contract_v1`; it freezes both available channel schedules using actual authorised Daily TF/context/rates and calculates no money. These private helpers are SECURITY DEFINER, owner-only; Daily search path is `pg_catalog,public,private`.

Raw SQL retains repository logical owner `postgres`. Use the existing audited provider compiler to map that owner to CURRENT_USER when installing on local/Miget; do not substitute arbitrary source owners, SET ROLE postgres, grant browser roles private access or relax FORCE RLS.

## Shared selected quote and target valuer

1157's new `private.pay_finance_selected_quote_v1(public.pay_finance_component_resolution_mode_enum,numeric,text,text,numeric,numeric,numeric,numeric,numeric,boolean) returns jsonb` is IMMUTABLE SECURITY INVOKER, `search_path=pg_catalog,private`. It delegates to existing `public._pay_convert_paye_to_umbrella`, `_pay_convert_umbrella_to_paye_ex`, `_pay_umbrella_vat_calc`; those exist in the canonical26052026 root. Preserve explicit NULL optional inputs; caller owns classification/currentness/selection guards. It is not a second calculator or evidence authority.

The full1157 file also replaces existing `public.pay_finance_component_resolutions_apply`: selection/context/evidence branches remain the saved-resolution authority and six quote assignments delegate to the shared helper. Preserve its SECURITY DEFINER/search_path public, existing complete signature/defaults and service_role-only execute ACL. Do not import only1157's first/last function and leave the public caller doing separate arithmetic.

The final NEXT target valuer is STABLE SECURITY DEFINER with `search_path=pg_catalog,private`; it preserves strict penny/channel/VAT/policy/pay-date checks, selects exactly one latest applicable immutable policy window, rejects missing/ambiguous windows and calls shared quote with explicit SUGGESTED_EQUIVALENT_BASIS and NULL units/manual values. It returns the existing window/ex/VAT/inc table signature. Private quote/valuer remain revoked from PUBLIC/anon/authenticated/service_role; owner-definer callers need no new service grant. Preserve old1500 as a superseded predecessor under the F04 registry, rather than retaining it as final authority.

## Reader contracts and PostgREST reload

0236 readers are STABLE SECURITY INVOKER with `search_path=pg_catalog,private`, private and revoked from all four public/browser/service roles. Draft reader requires confirmed SEALED run and frozen channel membership (zero/cancelled members count). Worker reader uses run/channel equality and Candidate keyset+limit before row/byte cap; status reader verifies exact run/channel/worker then delegates to existing status reader. No new lifecycle or public grant belongs in this file. Add the required schema-reload notification before its transaction ends as prescribed by F06/B2.14; this does not make private helpers public RPCs.

1206 history reader is STABLE SECURITY DEFINER with `search_path=pg_catalog,private,public`, timezone UTC. It requires a service JWT and trusted broker-supplied active admin actor, closed request keys and exactly one Candidate/Timesheet scope. It is intentionally readable after owner rollback and has no NEXT active-owner gate. Keep all52 typed parent/child registry sections, string-or-null scalars, frozen/current labels, bound cursors/upper keys and explicit unavailable moved/comparison data. Preserve PUBLIC/anon/authenticated revoke and service_role execute; it already emits `notify pgrst,'reload schema'`. Do not infer browser permission from the history-modal requirement.

## Frozen attachments, index extraction and source census

0718 installs exactly these four existing-guard attachments:

- `bpay_next_run_work_frozen_immutable_v1`: BEFORE UPDATE OR DELETE, each row, guard1342.
- `bpay_next_run_work_frozen_no_truncate_v1`: BEFORE TRUNCATE, each statement, same guard.
- `bpay_next_run_line_frozen_immutable_v1`: BEFORE UPDATE OR DELETE, each row, same guard.
- `bpay_next_run_line_frozen_no_truncate_v1`: BEFORE TRUNCATE, each statement, same guard.

Current1342 `bpay_next_effect_immutable_v1()` raises23514 `BPAY_NEXT_FINANCIAL_EFFECT_IMMUTABLE` whenever TG_OP is not INSERT. No whitelist, depth exception or workflow-progress bypass is needed.0718 exact-name preflight rejects wrong guard/type/enabled state/arguments/columns/predicate/constraint/deferrability before dropping/reasserting attachments.

The cancel-history index already uses `CREATE INDEX IF NOT EXISTS bpay_next_cancel_request_worker_history_idx ON private.bpay_next_cancel_request(run_worker_id,command_id)` and checks existing definition/validity/readiness/nonunique/no predicate/no expression with `BPAY_NEXT_CANCEL_HISTORY_INDEX_CONFLICT`. **Keep it in the repeatable; no extraction migration is required.** It does not replace the unique pending-worker admission index.

Read-only source census at this investigation found zero UPDATE writers using the card's git-grep expression across tracked NEXT repeatables (exit1/no matches). A separate rg census including untracked `*next*.sql` found zero direct UPDATE/DELETE/TRUNCATE on `bpay_next_run_work`/`bpay_next_run_line`. This is a source finding, not runtime proof. Root must re-run and log the required census immediately before installing0718 after all F05/F06 merges; report any writer as a deviation, never whitelist it.

## Permitted implementation verification

Owner start sheet narrows the repository's generic test/NEW/first-use wording. Root can load changed schema/function SQL on task-local PG17 and run `db:check`, relevant final-closure/source gates, and allowed seal/coupling checks. Do not invoke the newly installed seed/bind/receipt/history/payment functions with fixtures or execute frozen-row mutation tests now. In-app acceptance and effect-trigger mutation behavior remain NOT_RUN until separately authorised. This note stops at F06 dependencies; it does not redesign later cards.
