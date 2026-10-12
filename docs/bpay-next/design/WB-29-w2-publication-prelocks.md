# WB-29 W2 complete publication census and prelocks

12 October 2026; seq140. **Read-only supporting design findings, ready for root implementation review.** This is not W2 SOURCE PASS, installed SQL or business/runtime acceptance. Current2135 complete clone/publication,1438 targeted refresh,1309 seed and the final1215 Candidate fence are the authorities. BD161 inclusive carry supersedes the older compatible-siblings-only wording: every mapped ACTIVE sibling is retained, including an incompatible target/date/policy variant; explicit CLEAR removes only its exact owner.

## Immediate requirements

1. Complete the full input census and all business/public-FK prerequisites for the entire explicit Work set before the first Source seed. Seed1309 calls refresh_work, which can take the global Workbench revision row; waiting until just before POSITION_APPLY's agency clock is too late.
2. Pre-mark the whole exact physical Timesheet set through the existing public.timesheet_financial_retention_mark_v1(uuid[]) before publisher1. Publisher2135 calls it inside each publication. Otherwise Work2 can take a new public Timesheet/retention-insert lock after Work1 has already acquired the agency clock.
3. Bound both each current predecessor and its qualified original approval root. The stage/payload assertion reads complete child multisets from the root as well as the clone. A submitted100-row decision limit does not bound those physical trees.
4. Keep native refresh_many's >100 queued fanout. Its101 related-Case sentinel is not a complete Case inventory; do not prelock the first100 and then call that set complete.

## Concrete finite envelope

Existing limits are: one Candidate, <=100 explicit Works and <=100 submitted decision rows total; full W2 request<=32768 encoded bytes and action receipt result<=65536 bytes; refresh_many's initial Work+explicitCase arrays<=100 and combined pg_column_size<=16384; native retention marker<=1000 distinct Timesheets. There is **no existing2135 full-clone row/byte limit**.

Recommend an explicit W2 implementation envelope of **<=10000 physical input rows and <=8388608 bytes** in aggregate across the distinct current/root revision trees, plus **<=100 related Case IDs** when W2 needs an immediate complete Case prelock set. These are proposed execution safeguards, not claimed installed constraints or card constants. They allow normal multi-Work inputs rather than assuming100 decisions means100 physical rows. Root must record its chosen envelope and atomic typed overflow refusal in the contract; no silent truncation, partial publication or invented completion. An8-second transport ceiling remains independent and unproved by these source-only budgets.

Count/byte checks apply before seed, stage, refresh or publication. Include the revision row and full child-row serialized payload, including large retained JSON/text/evidence fields. Measure logical encoded stored facts with octet_length(to_jsonb(row)::text), not pg_column_size: TOAST/compression is not an encoded-byte bound. Check each row against remaining bytes before adding it; never construct an unbounded jsonb_agg and measure afterwards. The small fixed request/receipt/decision-reference bounds remain separate.

For one original root used by its current treatment, count that distinct revision tree once in the input-read inventory, but separately account for the complete clone output and seal-derived rows. Do not count it twice as two clones. Source tree limits also bound the otherwise unbounded EXCEPT ALL assertions. The byte/row ceiling is a refusal envelope, never permission to omit facts or normalize original money.

## Exact full physical census

For each locked Work w, let r=w.current_revision_id=w.applied_revision_id and a=private.bpay_next_treatment_approval_revision_v1(r). Require sealed/approved r and a, same Work, native revision_no/currentness and original root proof. Materialize distinct r/a IDs and each tree's bounded native identities once.

| Physical relation | Exact scope predicate / native index | Completeness |
|---|---|---|
| bpay_next_work_revision | id IN distinct current/root IDs; PK, same work_id | One actual revision per ID; retain full JSON, physical/version/policy/Source fields. |
| bpay_next_approved_line | revision_id=r; bpay_next_line_revision_idx(revision_id,line_no,id), unique(revision_id,component_key) | Count exactly revision.expected_line_count; component/line identities unique. Full tree, including untouched expenses/adjustments. |
| bpay_next_rate_detail | approved_line_id in that tree's complete bounded line set; (approved_line_id,bucket) | Per-line count exactly expected_rate_detail_count (0..5); retain actual hours/rate, including NULL. |
| bpay_next_rate_schedule | revision_id=r; (revision_id,rate_family,rate_code) | Count exactly expected_rate_schedule_count; preserve all frozen available/approved schedule facts, not only requested buckets. |
| bpay_next_shift_detail | approved_line_id in complete line set; (approved_line_id,detail_no,id) | Full shifts, clocks, hours, segment keys and retained evidence. No invented expected-shift counter exists in core1324. |
| bpay_next_break_detail | shift_detail_id in complete bounded shift set; unique(shift_detail_id,break_no) | Full break children. No invented expected-break counter. |
| bpay_next_approved_segment | revision_id=r; (revision_id,component_key,segment_key) plus unique(revision_id,segment_key) | Full sealed segment facts used by post-seal equality. Derivation count is exactly shifts whose segment_key IS NOT NULL. |
| bpay_next_revision_worker_basis | revision_id=r; PK | Existing whole/expense32-byte identities, one per sealed revision. New seal derives its own row; never copy it as another money owner. |

Use bounded physical prefixes before aggregates or qualification filters. For example: SELECT x.* FROM private.bpay_next_approved_line x WHERE x.revision_id=r ORDER BY x.line_no,x.id LIMIT remaining_rows+1. Reject the overflow sentinel before count/sum/equality. Read child rows by each bounded parent PK/index with the same cumulative sentinel; a join-wide LIMIT after an unbounded sort/filter does not establish a physical visit bound. Apply byte checks while consuming that prefix. For unordered pure census a LIMIT sentinel without a sort is sufficient; cloning/payload identity retains native keys.

After successful bounded census, complete immutable-tree comparisons and the native2135 stage can traverse the full qualified scope without a LIMIT. Do not put a truncating LIMIT in its INSERT SELECT bodies. Source/current roots are sealed immutable, and Work locks prevent a different predecessor from becoming current during this transaction.

2135 stages revision, line, rate detail, schedule, shifts and breaks. It deliberately does **not** clone approved_segment or revision_worker_basis: existing1236 seal derives both. Verify derived segment membership exactly against the original root and derive its count/byte upper budget from all segment-key shifts plus the existing root segment facts. No shifted quantities, remainders, rounding, pro-rata or financial history join is introduced.

## Required lock sequence and scope

Exact immutable original receipt replay/readback comes first after native service/admin/schema admission and original-request comparison; it must not acquire a fresh mutation scope or fail on a later target/epoch. For a fresh operation:

1. Native module row id=1 SHARE and current admin actor qualification remain unchanged. Resolve all exact prerequisite IDs by bounded point/index reads; caller Work IDs are explicit, distinct and one-Candidate. No whole-Candidate/Contract discovery. Public rows needed for qualification/FKs are listed below; use sorted IDs per relation and fail closed on missing identity.
2. Hold the one Candidate worker_control UPDATE and invoke private.bpay_next_assert_candidate_financial_fence_v1(candidate_id,NULL,NULL,NULL) before any seed or financial publication. Its current1215 body also takes module SHARE NOWAIT, Candidate UPDATE NOWAIT and active Candidate-week fence UPDATE NOWAIT. An unrelated STAGING/COMMITTING/CANCEL_REQUESTED/CANCELLING fence returns the native55P03 busy refusal. W2 has no NET job/nonce/epoch and cannot pretend to own that fence. Do not pass invented job arguments, remove the guard or reset a fence.
3. Acquire **all** explicit Works UPDATE in ascending UUID order; validate Candidate and expected/current/applied revision equality under those locks. Take current/root WorkRevision SHARE ordered by work_id,id, then complete required Position rows UPDATE by work_id,component_key COLLATE "C". Position PK is(work_id,component_key); current-revision scope also has(work_id,applied_revision_id,component_key). Lock the full finite component set needed by requested rows and all mapped siblings. Missing Position is POSITION_PENDING/refusal, not a fabricated zero row.
4. Discover related collection Case IDs under those Work/Candidate locks using work_collection(work_id,case_id) WHERE case_id IS NOT NULL, with an aggregate101 sentinel and deduplicated UUID order. Require every Case.candidate_id equals the same Candidate. If complete immediate set<=100, prelock finance_case UPDATE ordered id before any seed. Its parent lock is the native serialization boundary; do not add Case period/hold/history locks for read-only Workbench projection. Existing1822 refresh_case itself locks Candidate then Case and reads native components/rules. Work_collection unique(work_id,component_key) and related-origin partial index are current authorities.
5. Prelock existing ORIGIN Workbench task rows for this complete Work set by task_key COLLATE "C", and obtain the existing Candidate metadata through ensure_metadata before first refresh/generation.1438 refresh_work may encounter an existing origin task and queue instead of refreshing. Task-before-metadata/revision is native1429 order. No new task kind or absent-task placeholder is needed; Candidate/Work locks serialize the existing writers for this scope.
6. Obtain all required public scope/FK locks, immutable policies and bounded mapped ACTIVE decision rows before seed/clock. Native decision boundary/published ACTIVE indexes enumerate all variants on exact current predecessor; order decision IDs and retain target/VAT/policy/date/reason/original basis. Current target compatibility does not filter the carry set. Native2303 keys permit independent audit identities; they do not permit rebasing them.
7. Recheck original receipt under Candidate serialization, then validate the exact supplied native context and every raw intent/pair. Finish all full-tree census, seed-witness qualification, quotes, mappings and response-bound planning before changing anything. Stage **all** Works before publisher1. All their public-retention pre-marking is also complete before seed1. Only after these prerequisites may original Source seed/refresh and2135 publishers run.

Public non-key metadata/lifecycle writers can hold a public row while their native trigger waits for Candidate/Work/global projection authority. Therefore do **not** introduce a blocking SHARE upgrade after taking Candidate/Work locks. When stronger qualification locks are required under that order, use **FOR SHARE NOWAIT**, map native55P03 safely and roll back the original transaction. FOR KEY SHARE protects FK identity but does not freeze pay_method, VAT, physical is_current/version or availability fields; distinguish these purposes. Do not alter native writers merely to make W2 wait.

## Exact public FK and live-qualification set

- public.candidates(id): one actual Candidate, SHARE NOWAIT where current target settings are being frozen; Candidate.control remains the financial serialization owner.
- public.tms_users(id): actual current admin actor, native SHARE qualification; actor may appear on decision/events/receipt. Carried historical metadata is not falsely re-authored as the original chooser.
- public.timesheets(timesheet_id): distinct Work.original_timesheet_id and each current/root revision.physical_timesheet_id. For physical current/version/booking/archived/revoked qualification use SHARE NOWAIT. The existing retention marker then takes already-covered sorted KEY SHARE locks and inserts its exact idempotent retention rows once. Never enumerate the Timesheet version family.
- public.timesheets_financials(id): only non-NULL current/root financial_snapshot_id, SHARE NOWAIT when current immutable snapshot qualification depends on its flags/identity; retain exact(id,timesheet_id,timesheet_version). No payroll/invoice history scan.
- public.contracts(id): exact non-NULL Work.contract_id; KEY SHARE covers existing FK identity, SHARE NOWAIT only if the actual W1/W2 current-rate/context qualification reads its live fields. Do not replace approved used rates with live Contract rates. Genuine contractless Daily NULL remains NULL.
- public.umbrellas(id): finite union of current target, current/root source_umbrella_id and every carried sibling target_umbrella_id. Existing FKs use KEY SHARE; actual current-target enabled/VAT qualification uses SHARE NOWAIT. Incompatible carried historical metadata is preserved, not forced to today's enabled/VAT values.
- private.bpay_next_valuation_policy(id): exact current/root and every carried variant policy IDs; immutable PK/FK identities. No new policy, finance-window mutation or global policy scan.
- Current revision.source_head_id, source_rate_override_id/source_rate_default_id are retained native facts, not blanket public FK claims: core1324/1009 do not declare those UUIDs as public FKs. Lock a Source proof row only when its exact existing owner requires it; never invent a public head/override lookup or traverse descendants.

Fresh cloned child FKs point to newly inserted revision/line/shift IDs owned by this transaction. Do not prelock nonexistent future rows or make a generic lock framework. Actual POSITION_APPLY command/member FKs are created by the unchanged publisher. Actor/Candidate/physical/Umbrella/policy prerequisites above prevent late external FK waits after its first clock.

## Source proof and refresh semantics

Seed only genuine UNBOUND with native1246 proof on exact effect/hold IDs, current applied revision and complete frozen/settlement identity. A single effect/hold must cover the whole Position R/H scalar; absence/multiple/unproved scope refuses, never sums history. READY book is left intact; do not pass treatment revision as a new replay seed tuple. Parent Source book/canonical root differences retain current captured/observed/publication/job/Position guards. No new accepted offer survives E2 revision change.

For <=100 total Works+related Cases,1438 refresh_many prelocks them and refreshes exact projections. If total exceeds100, native code intentionally replaces related Case array with explicit Cases and queues exact Work ORIGIN tasks for bounded later fanout. Do not override that behavior, label first100 complete or insist on immediate Ready. If W2's seed path could itself require an oversized complete related-Case lock set before its first global generation, refuse that single fresh operation before seed; otherwise leave oversized post-publication related-origin processing to existing queue. Such refusal must be explicit in the W2 contract, not an asserted whole-card impossibility. No unrelated Case/new Candidate lock is acquired after a global clock.

Existing Workbench group/item/summary/generation updates are native projection serialization, not new financial parents. They remain within the established Candidate/Work/Case→task→metadata→revision ordering. Do not prelock every Candidate summary or global group inventory. Agency receive_command1404 then locks command_clock(id=1) UPDATE; all business scope above is already fixed. Subsequent publishers reacquire their preheld business rows only. Original atomic receipt/event/decision order and inclusive immutable carry remain BD161; any failure rolls back every seed, clone, marker, clock and publication.

## Source anchors and evidence limit

Read current saved bodies:2135 native publication/stager:5–128, payload assertion:31–75;2135 publication boundary:25–119;2135 revision guard;1438 refresh_work:317–351 and refresh_many:354–417;1346 mark_processing:6 onward;1309 seed:5–44;1822 refresh_case:1401 onward;1215 fence:1525–1630;1429 ensure_metadata:31 and schedule_task:66 onward;1252 delta:87 onward;core1324 schema:111–346/indexes:995–1052;1100 segment schema;1236 seal:44–103;2044 decision/event FKs;2303 independent audit keys. public retention marker is the current saved flat native26052026_2100HRS_NEW_FUNCTIONS.sql:183391–183459, with exact<=1000 sorted Timesheet KEY SHARE and idempotent retention INSERT.

| Saved source | SHA-256 |
|---|---|
|2135 treatment_native_publication|56D91E37EDE376018C72245EE02FD1D30237F987C656754B2FD0EC668582DDC1|
|2135 treatment_publication_boundary|89A98FC7688803AABE93011EDFCF588C7BAA602DC46C903AA6E3000B1E0751E8|
|1438 workbench_origin_projection|B8D445C4A4CFC02978E8AC77A159350B0E1978AAFF9A1172DA8E7E157A6414F5|
|1215 case_net_replacement/fence|23AC3D72354EAC877D66E61E249DE0DB94F1F820BB7F96A90373C5D5425D72BB|
|1309 workbench_source_seed_refresh|E1330272DFAA2978E537C49E074669BB9273410620F016BDCD1D4F05BF567241|
|1822 classified_work_collection|B9650E9DA89A3DCA3B59BC33F73EFC8ACF132038DAE163DF826D1F18730990FD|

Reviewer performed source reads and this Markdown write only. Exact authored W2 source must be independently reviewed against this contract before approval. No catalogue probe, installation, data read, compilation, timing or joined/runtime acceptance occurred.

