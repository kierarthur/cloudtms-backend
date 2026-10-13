# DR-15 independent reader source review

12 October2026. Read-only saved-source review of installed `12102026_0252_banking_pay_next_preparation_issues_v1.sql`, supporting0259 read indexes and the PREPARE_ISSUES manifest against BD132/card/current owners. No SQL/database/business execution, tests, product edits or fixtures. **SOURCE verdict: findings open; no delivery/runtime/visual PASS.** Installed0252 is immutable; corrections require a successor.

## Essential findings

### R01 — Serialized request/cursor/reply bounds are absent

0252:23–55 validates shapes/counts with character lengths, then151–167 builds up to101 complete rows and returns up to100 plus a raw label cursor without any serialized byte check. The100-row bound is not the120000-byte response bound. Legal8KiB names/references across many rows can exceed it; an8KiB label containing JSON-escaped characters can also produce an issued cursor exceeding16384 bytes. UTF8 character length is not byte length. This can reject the entire valid read at transport instead of offering a usable bounded continuation.

Smallest fix: a successor enforces the ordinary request/cursor byte envelopes, accumulates only fitting output rows, uses the last included native row for continuation, and checks the assembled issued cursor and reply. An individually unreturnable row receives the existing finite size refusal; never silently truncate a label or return an unusable cursor. Preserve full native sort keys. No protocol/table is needed.

### R02 — Unbound historical Work labels are replaced by current Candidate data

0252:111–114 unconditionally coalesces missing immutable Work label to public Candidate name/reference and emits CURRENT_CANDIDATE. An original ID-only legacy page skip legitimately has `work_id` but no exact expected revision (0454:100–101); in that case the manifest explicitly requires original Work labels to stay null (`DR-15-PREPARE_ISSUES.json:36`). Current Case-only Candidate labels are permissible, but this fallback also covers unbound Work facts and makes the historical record appear labelled by a later identity.

Smallest fix: only genuinely Case-only/no-original-Work facts may use the qualified current Candidate label. If an original Work identity is present but its exact revision cannot be established, retain unknown nullable identity/label provenance. Never use a current revision/history-latest fallback.

### R03 — Legacy repeated skips are not deduplicated

0252:62 gives every legacy receipt page its own `PAGE:<page_no>:<work_id>` row.67 excludes a Rule B duplicate, but does not exclude the same legacy skipped Work appearing on another page. Original explicit append0454:197–199 skips before the run_selection/work_choice inserts at201–204; therefore the native unique accepted-Work constraint does not prohibit that omitted Work from being skipped again in a later legacy page. The read counts distinct Work IDs at134 but lists/counts scope rows per page, making the same omission appear twice and disagree with its own skipped-Timesheets count.

Smallest fix: choose a deterministic original retained page fact per skipped Work, while preserving its own exact expected revision/reason; do not collapse unrelated Candidate/Work omissions or infer a current revision. Completed RELEASE native admission currently prevents a second release after the worker is CANCELLED (2259:2765), so no general new historical revision framework is required for that branch.

### R04 — Unknown Reason sort differs from the displayed manifest

0252:127 uses `Contact support` for an unknown nonpending issue, but the closed manifest39–40 displays `Could not prepare this Candidate; contact support.`. Whole-list Reason sorting must use the actual displayed phrase, including the unknown fallback, or rows appear in a different alphabetical order from their visible Reason. All33 known labels in each SQL review/omitted map match their corresponding manifest values by static JSON comparison; only this fallback differs.

Smallest fix: use that same one-sentence fallback in the native Reason operand. Preserve unknown typed codes and the pending-state copy; no new enum or command is needed.

## Retained source checks

- Static normalized complete-statement comparison: removing only three DR15 declaration lines and the early PREPARE_ISSUES branch reconstructs the complete current2259 public definition exactly. All unrelated Case/audit/retry/guard/status branches/configuration remain intact. This was source comparison only, not execution.
- New branch18–22 uses service-role, current active admin and NEXT plain reads; it precedes the general locking preparation authority. It contains no mutation, financial call, wake or new command. Original epoch35–37 is qualified by the actual run command or accepted descriptor. There is no invented agency_id.
- Original selected WORK skips at58–60 are positively selected with non-null native skip. Explicit page revision63–66 is read from that same retained choice receipt, without pointer/history fallback. Native Work/revision labels117 and release/expiry/discard retained prefix joins use exact Work/Candidate bindings.
- Completed SYSTEM/OFFICE release77 requires COMPLETE/stage COMPLETE/finished time. Expiry85 and discard94 also require native terminal facts; blocked discard is not reported as successfully removed. Original selected Candidate membership101–107 keeps unenrolled Candidates visible with nullable worker identity and native failure/pending state.
- Tail132–137 is point-revalidated against its exact row key/value;163 refuses an obsolete or nonunique tail. Null flag/numeric gross ordering140–150 matches packed ordering156–162, with an explicit native source tie. All statuses and counts are read-only factual outputs, not financial authority.
- Four0259 indexes are confined to existing native choice/release/worker sources. Choice/run/Candidate/member index supports first frozen-label lookup; completed-release partial index supports the release run predicate. Gross/state indexes support native worker legs. The joined full source union still requires residual whole-scope sorting; these indexes do not prove an entirely indexed global path or constant timing.

## Remaining coherence qualification

The existing1429 Candidate metadata trigger/scheduler advances Workbench revision in the same transaction as current name/reference edits, as established in the saved design review.0252 reads that context at38–43 before its large CTE at57–162. The containing PL/pgSQL function is VOLATILE; separate reads can see a newer committed native state after the captured context. A final fresh comparison of the same native module/run/Workbench context before return is the narrow safe closure, or an equivalent demonstrably single-snapshot read. No new clock/digest/vector is authorised. Exact tail validation protects later continuations but cannot by itself certify the first reply's context against a concurrent commit.

## Reviewed bytes

| Source | SHA-256 |
|---|---|
| installed0252 repeatable | `183a17cb1800e4980945e0e348d249bc07d0829945f6a30c95414ba82c478955` |
|0259 read-index migration| `73b350c5823dcbdeb125e19dec83d1f6372527620a6c7d7651a41deee3334d34` |
| PREPARE_ISSUES manifest | `e8b5de853d9ba6f148f5e03f325c35dcd021bb327911d02f11480b299e56e279` |
| BD132 build note | `90369d57ed6dae84fd1d6bd3e4bcafa42099fdf5766af87eb3d8b893a32a984f` |
| current2259 parent | `1a57552c42d63d0dc885a4e7294472c03319537da9e913a96cbbadc10c2ab879` |

Required source gates/local compile/catalogue/contract checks are root-owned and are not independently rerun by this review. Worker/Office closed schemas, unknown journal recovery and A4 acceptance require their own current-source/pixel receipts.

## Fresh0307 successor review — R01–R04 closed, R05 open

Read the complete uninstalled `12102026_0307_banking_pay_next_preparation_issues_bounds_v1.sql` current bytes, SHA-256 `8281f1b72742fde2e8db4074c0ce641b4155147269b426f9fba1886f854b720c`. **SOURCE-only status: the four original findings are closed; the finite OPEN-run context edge below remains.** Historical installed0252/0259 were not edited.0259 and the manifest retain the hashes above.

- **R01 closed:**23 caps serialized arguments at32768 bytes;46 caps incoming cursor at8192 bytes;167–174 checks native label byte lengths and incrementally admits only rows fitting the assembled120000-byte reply. An oversized first row refuses with the exact size error; a later boundary preserves a nonempty included prefix, sets incomplete and uses its last included native key. Final185–189 checks the complete reply and issued cursor. A cursor that cannot fit receives the honest named refusal; no empty fake-complete result or truncated identity is emitted. The CTE's intermediate source101-row pack remains count-bounded, not described as a120000-byte intermediate allocation.
- **R02 closed:**113–115 permits current Candidate name/reference only when original Work ID is null. Missing original Work revision emits UNKNOWN_ORIGINAL with null labels. An actual frozen revision with null Candidate reference remains null, including the original-label fallback; no coalesce converts its null into current reference.123 restricts the original captured-label fallback to no-original-Work facts. Exact physical fields still come only from the actual bound Work revision.
- **R03 closed:**62–68 uses `distinct on(w.id)` ordered by exact Work ID/original page number, retaining the earliest page's original revision/reason. Accepted Rule B skip exclusion remains exact; distinct native Work omissions remain separate.
- **R04 closed:**128 uses the manifest's exact unknown displayed phrase. Static JSON comparison again found all33 review and all33 omitted known labels identical to their manifest values.
- **First-read context check added:**177–184 freshly compares the same module owner/run status/revision/buckets/Workbench revision/adoption tuple immediately before return, without locks, writes or a new authority. This closes a concurrent commit when that commit changes an included operand. Exact native tail/value validation164 remains in place.
- Complete normalized function comparison again proves that removing only the three DR15 declaration lines and early issue branch reconstructs the entire current2259 public definition exactly. Native point read33 is now qualified `r.*`; signature/security/search_path/owner/revoke/service-only execute tail is preserved.

### R05 — Mutable legacy OPEN membership is outside that context

0307's context39–43 and final177–183 omit the native selection-page/selected-membership counters. The branch admits OPEN runs. Original legacy explicit append0454:213–215 adds page/skipped facts and updates `selection_page_count,selection_count,selected_candidate_count,work_choice_count,sealed_work_choice_count`, without advancing review_revision or the Workbench revision. In the particularly small case of another all-skipped legacy page, only selection_page_count advances; status/selection-state/preparing/ready/review/removed and Workbench operands stay unchanged. A new omitted row can therefore arrive ahead of a valid tail while both native-context comparisons still pass. The original accepted Rule B cut is immutable; this edge is specific to the intentionally supported live legacy OPEN receipt path.

Smallest closure: add existing `selection_page_count` and `selected_candidate_count` to the same ordinary reader context/final comparison and its two closed transport mirrors. The first catches even an all-skipped legacy append, while the second qualifies original Candidate membership. Alternatively constrain mutable legacy OPEN issue reads explicitly if that is the chosen supported contract. Do not add a clock, witness, digest, vector, table or financial operation.

This amendment is source inspection only. Runtime, joined, sort timing and A4 remain NOT_RUN/pending, and no whole-item completion is asserted.

## Final fresh0307 override — reader SQL SOURCE PASS

Fresh saved0307 SHA-256 `ed7ba4a93d22f0cba36dba5621f2ddd1ecaa44ed27f7ec837fd4d437233a6c96` supersedes the earlier0307 hash/status. **All R01–R05 and the first-read native-context finding are closed in this reader. SQL SOURCE PASS only.**0252/0259 remain immutable historical sources;0259/manifest hashes above remain current.

R05 is closed by existing `selection_page_count` and `selected_candidate_count`, exposed as `selection_pages/selected_candidates` in both initial39–44 and final178–186 context constructors. A legacy all-skipped append now necessarily changes selection_pages and refuses stale continuation/first-read coherence; native Candidate membership changes are also qualified. No new revision/proof is introduced. Existing key/value tail validation165 and fresh module/run/Workbench comparison186 remain enforced.

Independently repeated the complete normalized inverse comparison on these final bytes: removing only three issue declaration lines and the early PREPARE_ISSUES branch exactly reconstructs the current2259 public function. The33+33 manifest labels, exact unknown fallback, unknown-original Work provenance, deterministic legacy skip deduplication and finite byte-prefix/issued-cursor checks remain as reviewed above. Signature/configuration/security/owner/revokes/service-only execute are retained, and the new branch is read-only.

BE's closed PREPARE_ISSUE_CONTEXT at preparation.js:15 now includes both native counters. At this snapshot FE Office:206 was still being authored and lacked those two fields; root was notified to update that exact mirror. That is a transport/UI integration task, not an open defect in this reader SQL. No Worker/Office SOURCE PASS, runtime/joined/timing/A4 or whole-item completion is inferred from this SQL verdict. Source gates and installation/catalogue checks remain root-owned and were not run by this reviewer.

## Final0332 reader amendment — SOURCE PASS retained

Independently reviewed `12102026_0332_banking_pay_next_preparation_issue_payment_sort_v1.sql`, SHA-256 `abc93d5dacbc1ab745356ddb1344ef414ef0c5253242ba028a4098ac567cc943`, against the actual installed0307 parent, whose final pre-installation bytes are SHA-256 `7c83ed8ebedc42f63f97320db565904fdd33e4f26e298ed96592076d0eaa9274`. The earlier0307 review hashes are superseded by that installed hash; the historical file was not edited by this reviewer.

The complete0332 function differs from this0307 parent in exactly one native PAYMENT sort operand at128: an actual Timesheet reference sorts by `Timesheet <reference>`, missing-reference original Work by `Timesheet`, and no-original-Work payment by `Payments`. Those are exactly the displayed labels in the real Office list at1771. Native full value, source ties, null/unknown facts, keyset comparisons and byte limits remain intact; no client sort or guessed payment kind is introduced.

Fresh static inverse comparison again proves that removing only the DR15 declaration lines and early branch reconstructs the complete2259 public definition exactly. Fresh reread retains all R01–R05 closures: native32KiB request/8KiB cursor/120000-byte reply envelopes; nonempty bounded included prefix; explicit oversized-row/cursor refusal; frozen nullable reference; UNKNOWN_ORIGINAL missing Work labels; deterministic legacy Work dedup;33+33 known reason labels/unknown fallback; original selected native sources; initial/final selection-page/selected-Candidate/module/run/Workbench context and exact tail qualification. No SQL/business/test call was made.

Current manifest SHA-256 is `eab3266bc7e92b353028853dfe2718af1aeae40ca24f339612e331d393c0b23c`. Its two plain-language policy phrases now say `cannot be paid this way` for UMBRELLA_WORK_EXECUTION_UNSUPPORTED and CASE_TARGET_UNSUPPORTED. Both native review/omitted SQL maps match all33 corresponding manifest entries exactly.0332 and its actual0307 parent retain the same owner/revoke/service-only grant plus `notify pgrst, 'reload schema'` installation tail; the runtime branch remains read-only.0259 index migration stays SHA-256 `73b350c5823dcbdeb125e19dec83d1f6372527620a6c7d7651a41deee3334d34`.

**Final reader verdict: SQL SOURCE PASS at0332/current manifest.** Root reports normal source/catalogue/contract checks separately; they are not independently executed or represented as runtime acceptance here. Transport/UI, A4, joined/business and timing acceptance retain their separate status, and this receipt does not mark the whole item built.

## Final0341 semantic State order — SOURCE PASS retained

Fresh source-only review of uninstalled `12102026_0341_banking_pay_next_preparation_issue_review_order_v1.sql`, SHA-256 `3526263068aefa3c351e68b6eca3c6ee3cc30c40474027c14c97dd6d13007ca5`, against the actual installed0332 parent SHA-256 `d424565e4301d8f7b8ffbfc64c82cf40f85f20d4d7c046c627fdd084f828e751`. The final0332 bytes include the pre-installation truthful PENDING phrase `Waiting to prepare` in State and unknown/pending Reason operands. Actual worker PREPARING retains `Preparing`; the native facts/enums are unchanged. These current hashes supersede the earlier0332 hash in this receipt.

The complete0341 function differs from this installed0332 in exactly one STATE operand: a finite semantic rank followed by its existing displayed State phrase. REVIEW/FAILED sort first (`0:To review`), then actual PREPARING (`1`), unenrolled PENDING (`2`), READY (`3`), DRAFT (`4`), ISSUED (`5`), COMPLETE (`6`), CANCELLING (`7`), then the remaining native states (`8`). ASC therefore matches the actual default `To review first`; DESC is its exact reverse. The existing native source tie remains unchanged, and the same ranked value is packed, sought and tail-revalidated. Worker/FE accept it through their existing bounded text-key type; no field, enum, cursor protocol, clock, table or command was added. NOT_IN_DRAFT still defaults to Candidate A–Z. Current native Office state/reason phrases and sort selectors at1720,1732,1997,2011 match this scope.

Static normalized inverse comparison again reproduces the entire2259 public definition exactly after removing only the DR15 declarations/early branch. The one-operand parent diff proves R01–R05/context/provenance/dedup/byte closures remain unchanged. The earlier FE-context integration warning is now closed: both observed BE:15 and FE:206 closed context definitions contain selection_pages/selected_candidates. This is not a broader independent Worker/UI acceptance claim.

Current final registry SHA-256 `35b57b2e8edbd3a1196281dd743f817a19753f313a2d71df931d42d15728692c` names0341 as both finalPhysical/finalRoot for the sole existing public signature, retaining the0252→0307→0332→0341 chain and next DR13 owner. Current source-gate SHA-256 `3a12537091e8056ba6d29f551ac158f159cafe83ade5c0698a5ad43f97f884ef` retains exact named public constructors at704–712 and exact signature-bound finite builtins/call edges; installed0252's old unqualified ROWTYPE statement has only a whole-file-SHA-locked, one-exact-statement historical allowance at2196–2199. The final qualified successor is checked normally. No broad authority or wildcard constructor exemption was added. This is inspection of gate coverage, not execution of the gate suite.

**Final reader verdict: SQL SOURCE PASS at0341; no open reader findings.** Manifest remains `eab3266bc7e92b353028853dfe2718af1aeae40ca24f339612e331d393c0b23c`;0259 remains unchanged. Installed predecessor immutability, public service/admin/NEXT gates, fixed security configuration and original unrelated branches remain intact. Runtime, business/joined/timing, A4 and whole-item delivery retain their separate root-owned acceptance status.
