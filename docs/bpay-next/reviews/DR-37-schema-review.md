# DR-37 replacement migration source review

12 October 2026. Read-only independent review of root-authored, uncommitted/uninstalled `supabase/migrations/12102026_1215_banking_pay_next_case_net_replacement_v1.sql`, full DR-37 card/checklist, R2 §2 and `design/DR-37.md`. Product/checklist untouched. No SQL, business rows/RPC, tests, fixtures or hosted actions. Lifecycle guards/repeatable are still being authored; this receipt is not whole-item PASS.

Initial reviewed raw SHA-256: `13167B6A222FF39732A750FDF31AC5A12BD701A6802051ED0AAC334F10147AA8`.

## Initial findings (historical)

1. **Preserve native BIGINT component ordinals.** Migration:51,53,154 declares staging/cleanup cursor component ordinals INTEGER, while native component/instruction/allocation-state cursors are BIGINT (`case0300:73,246,329`). Use BIGINT with positive checks for all three; the new table must not narrow an otherwise valid native ordered membership.
2. **Retain the native state/pass pairing in the staged FK.** `LIKE ... INCLUDING CONSTRAINTS` copies CHECKs, not foreign keys. New staged state FK at114–115 has only five operands, whereas the original result's FK includes `pass_kind` (`0300:392–393`) and its matching six-column allocation-state unique exists (`0300:336`). Restore that original six-operand FK. Otherwise a DRAFT/PREVIEW staged row can reference a NET target state structurally; the new staged row should preserve the native typed result closure.

Both findings were reported to root before any lock/apply. They are closed by the fresh re-review below.

## Source checks that are already sound

- Request shape widens only the Case branch from expected+1 to request>expected, preserving the ordinary legacy-null branch and native input-kind/input-shape constraints (`migration:10–15`; `0380:11–29`). Unique immutable `(worker,request_no)` remains; gaps become legal rather than reused.
- Nullable original job/target-state identity supports accepted PRECHECK before enrollment. The existing job composite owner FK exists (`0300:303`), worker `(id,run_id,candidate_id)` unique exists (`0300:217`) and period `(component,Case,Candidate,week)` unique exists (`0300:148`). The pending worker pointer has the exact replacement/worker/Candidate composite FK. Multiple NULL pre-job bindings are intentional, not an active-lease adoption.
- One nonterminal replacement per worker is enforced by the partial unique index; Candidate-wide financial exclusion remains the existing active-fence unique index rather than a second lock protocol. Monetary carry uses native penny domains, original instruction/count fields are BIGINT, high-water is independent of accepted projection, and no table is a live money ledger.
- Staged result keeps native result columns/defaults/CHECKs and deterministic replacement/instruction and replacement/result-number uniques. Per-Case staged capacity carry is the design's required disjoint aggregate for multiple instructions sharing one Case, not a new calculator.
- Completed handoff stores exact completed instruction/hold counts, source total, original job, cancellation command and prior/target native identities. Native immutable result/hold tuples can qualify target/VAT operands individually; there is no requirement to invent a duplicate money aggregate for every unit.
- New tables have a finite literal owner loop, owner-only FORCE RLS and revoked public/anon/authenticated/service_role table privileges. No public-table trigger, public command, browser/service table grant, business backfill or agency-owner switch is added.

## Guard-dependent closure to review with the repeatable

The following are explicit implementation checks, not reports that unfinished code is absent: bind replacement request_no/prior projection/request worker and current module; bind nullable job/target only once from the exact enrolled owner; require both bindings before STAGING/commit/cancel; bind every staged row to this replacement's target NET state, original immutable instruction/DRAFT result and exact prior ACTIVE hold; bind carry Case/Candidate identity; bind refusal request_no exactly; bind every handoff job/cancel command/state/projection/cleanup cursor to its replacement, including prior NULL pair and exact final native cursor. Single-ID FKs alone do not prove those cross-links.

Lifecycle/insert guards must make staged/refusal/handoff rows append-only, forbid terminal replacement rewrites/deletes/TRUNCATE, prevent late REFUSED after hold mutation, enforce monotone counts/cursors and exact immutable completed handoff replay, and retain original 0300/0310 hold/result/capacity amount/lease rules. High-water default0 is intentionally not a business backfill; intake must allocate from `max(stored high-water, latest immutable native request_no)+1` with overflow refusal and maintain it through refusal/cancel. This is stated by root design:13 and needs verification in the new accept owner.

**Initial checkpoint verdict (historical): OPEN for the two concrete native-type/FK findings and pending authored guard review. Compile/catalogue and all business acceptance NOT_RUN by this reviewer.**

## Fresh schema re-review

Reviewed full saved migration again, raw SHA-256 `4EE1267C582F5B204292D968A721CA77C4337CD3087F4E22E90AD6AEC089ED9C`. The original findings are closed: staging, cleanup, inventory and certificate component ordinals are now positive nullable BIGINT (`59,61,64,179`), and staged rows retain the exact native six-operand state/pass FK (`132–133`; original0300:392–393).

**One additional native relationship requires closure before locking.** Original accepted results also have `(state_id,allocation_channel)` → `bpay_next_case_allocation_channel(state_id,allocation_channel)` (`0300:396`). `LIKE ... INCLUDING CONSTRAINTS` does not copy that FK. Current staged rows restore state/pass and instruction FKs but omit this channel pair (`121,132–137`). Retain that exact existing FK, or prove an equivalent mandatory insert-guard check against the exact target state's native channel row. This was reported to root immediately; it does not require a new financial owner or channel policy.

The new cancellation inventory is a narrow R2 §2 exact-membership support record, not an alternate cancellation authority: `(replacement_id,case_hold_id)` is its PK and `(replacement_id,instruction_id)` its unique (`184–192`); native `0300:440–441` permits only one ACTIVE typed hold per instruction, so this preserves the genuine surviving DRAFT/prior-NET/target-NET membership. Expected and released source totals are now separately stored and equal in the immutable handoff (`177–178`), with bounded inventory and cleanup cursors/counts retained on the replacement (`55–65,91–94`). The inventory scans frozen instructions before cleanup rather than discovering released rows from hold history. The finite RLS owner loop covers all six new private tables (`194–205`); no public membership or browser/service table privilege is introduced.

The authored repeatable must still establish exact inventory-member hold/instruction/allocation-state/replacement identity and source amount, constrain states to this replacement's DRAFT/prior/target state, admit inventory only under the exact original NET fence/lease, freeze membership before RELEASE, and compare complete maintained inventory/release counts and totals before publishing handoff. Inventory and handoff cursor tuples must be all NULL or all populated, with the final tuple bound to the exact exhausted native membership; this is not fully expressed by the migration's CHECKs. Append-only member/refusal/handoff and lifecycle guards, and the genuine later SIMPLE_CANCEL RELEASE delegate, remain separate source-review obligations. Existing `0380:37–40` NET-state shape still requires a real captured week revision and preserves COMPLETE projection qualification; new PRECHECK/STAGING creation must satisfy it rather than invent a placeholder.

**Current verdict: OPEN only for the additional exact target-channel relationship and pending repeatable guard review. The two initial findings are closed. This is a schema source receipt, not whole DR-37 completion. No SQL, catalogue, business acceptance or timing was run by this reviewer.**

## Target-channel correction verified

Fresh migration SHA-256 `131005DE5124F0B44FCA77D4D5203C63F9484202F4A2798634C7C8CEB12C2BC9` includes the exact native `(state_id,allocation_channel)` FK at136 and the audited single predecessor stage-CHECK lookup at16–25. The third schema finding is CLOSED. All three native schema corrections are now source-verified. The current partial lifecycle/intake review is saved separately in `DR-37-intake-lifecycle-review.md`; its concrete guard/replay findings control that unfinished repeatable. This does not pass the unbuilt staged/commit/consumer owners or whole DR-37. No SQL or business acceptance was performed.
