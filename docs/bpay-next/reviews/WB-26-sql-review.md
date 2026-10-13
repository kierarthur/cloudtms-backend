# WB-26 SQL and Worker source review

11 October 2026. Independent final source review for sequence 71. Reviewed saved disk contents in the active backend worktree. Notes only: the root builder remains the sole product writer. No SQL execution, database connection, tests, fixtures, browser, hosted call, secret, payment/provider action, commit, push or deployment was performed by this reviewer. All actual application journeys remain NOT_RUN.

## Verdict

PASS for the current narrow SQL/Worker logic, source-gate delta and supporting index migration. No essential finding remains unresolved. This verdict covers the three saved successors, the single private index migration and changed Worker closed schemas; it does not claim installation, runtime parity, screen acceptance or app testing.

The full approved WB26 card (`02_WORKBENCH.md:981–1012`), all seventeen checklist rows (`IMPLEMENTATION_CHECKLIST.md:2068–2099`), `design/WB-26-build-scope.md`, `design/WB-26-design-review.md`, and the earlier investigation were re-read. D08/D10/D15 and their indispensable existing-owner support are the review scope. The other rows remain shared-owner evidence. Rule B, Policy X, the original money locks and immutable allocation row kernel remain the authority. Reference0549, parked2153, producers, WB24 aggregate controls and WB27 estimates/extraction remain outside this source change.

## Finding resolved during review

The first source read added the new Case JSON keys to every LEAF, including ordinary Work with null values. The existing reducer treats any LEAF basis difference as a changed unit and defaults it selected (`10102026_2306:90–99`), so unchanged Work exclusions/NONE could have been reset during upgrade. This was reported immediately to root.

Root corrected `11102026_0102:29–34`: the new fields are now appended only when `cc.id is not null`; Work retains the original JSON keys and values. The re-read confirms the fix. The rule ID/revision pair also now comes from one exact immutable rule: `i.rule_id` is joined to `cr.id=i.rule_id`, constrained to the same native Case/Candidate (`23–26`, `39–41`). The earlier design findings about positive numeric evidence and retained-rule taxonomy are also resolved (`327–339`).

## Accepted Case boundary and live money

`supabase/repeatable/11102026_0102_banking_pay_next_case_choice_boundary_v1.sql` contains only four named existing-owner successors:

- `private.bpay_next_selection_memory_leaves_v1(jsonb)` extends the existing immutable Case LEAF with native Case/component identity, Case status/due, taxonomy, component ordinal, revisions and original snooze control sequence. Existing capture and immutable LEAF guards remain the writers/fence; no new table or protocol exists.
- `private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer)` uses that exact run/Candidate/component LEAF only on the memory descriptor route (`310–339`). Missing older evidence takes existing `CASE_INPUT_UNBOUND`; the ordinary non-memory path retains current-pointer behavior. The captured rule UUID and revision bind the same immutable Case/Candidate/rule taxonomy. Positive Case/component/rule revisions and nonnegative control evidence are explicitly required. Full current Case/component revision equality is deliberately absent because money/resolution owners legitimately advance those revisions.
- `private.bpay_next_complete_no_case_worker_v1(uuid,uuid,bigint)` retains the original completion owner and narrows its accepted-empty exception.
- `private.bpay_next_publish_no_case_week_fact_v1(uuid,uuid)` reads the original captured Candidate floor for accepted descriptor runs (`851–856`), with the old ordinary fallback. Missing captured Candidate authority cannot silently fetch a later floor.

Preparation still locks run → Candidate control → job → run worker → allocation state, then genuine Case → component → period, in its predecessor order. The accepted status/due/rule are non-money inputs. Outstanding, funding, recovered/written-off balances, realised and active reserved period capacity remain live under those original locks (`305–308`, `386–418`, `436–439`, original capacity section). Captured monetary values in memory do not become capacity. Genuine funding-event evidence and the original exact period transition remain mandatory (`399–402`, `441–459`). The original `bpay_next_case_allocation_row_v1` call remains the only recovery formula (`594`); the predecessor-body comparison found no second kernel or unrelated monetary rewrite.

Original Source comparison refusals remain before allocation writes and selected instruction capture (`160–188`, `344–351`). A later rule pointer cannot reprice the accepted memory route. Old malformed/incomplete retained evidence is refused, and no new permissive fallback substitutes later Case inputs. Original snooze/event cutoff handling stays with its original owners.

## Accepted-empty Case completion

The prepare gate requires no allocation state, no Case header, an accepted descriptor, the exact retained selected Candidate and zero retained CASE_COMPONENT membership before delegating to the existing no-Case helper (`148–155`). Paused/closed factual Cases alone therefore do not require fabricated empty CASE_PAGE/CASE_SEAL commands. Legacy non-descriptor behavior stays conservative.

Completion proves positive captured positions and a positive captured Work line on this accepted-empty route, exact `captured_position_count = captured_line_count + captured_managed_position_count`, expected/captured managed count equality, and absence of incomplete managed capture (`787–799`). It preserves the existing PREPARE lease, phase/module/owner/status/review/financial-resolution checks and original Work holds; it introduces no collection, revaluation or release. Nonempty all-false SEALED Case manifests keep the predecessor's own zero-instruction allocation path, so selected Work survives. Case selection revision remains zero only on the genuine no-Case path; the original week/contribution/net projection checks remain intact.

## Retained Case read, recovery and Worker contract

`supabase/repeatable/11102026_0113_banking_pay_next_case_choice_readers_v1.sql` adds only a four-argument overload of the existing accepted-input helper and replaces its existing public preparation/common Workbench reader callers. Optional `member_kind` is closed to WORK/CASE_COMPONENT; absent/default WORK delegates to the original three-argument helper and preserves the Work response (`6–16`, `54`, `72–76`). There is no new public reader kind, route or mutation action.

CASE_COMPONENT pages use immutable global `member_no`, select one native Candidate at a time and limit rows to 100 (`17–30`). Current CREATE numbers Case members by Candidate then component (`11102026_0005:277–279`), so the Worker contiguous member-number check matches the actual source. Expected membership count, exact manifest counters/state and per-member uploaded evidence are server-owned. Uploaded is proved by the native child item's run/Candidate/revision/component and retained selected boolean (`26–28`); no uploaded-count-as-cursor assumption is introduced. Arbitrary original upload order can therefore recover by scanning retained membership and skipping exactly accepted children.

The helper holds run SHARE (`15`); existing CASE_PAGE/CASE_SEAL hold that same run UPDATE (`10102026_1832:1071`, `1150`). This keeps the sequential manifest and uploaded-child reads coherent against those original writers. Terminal global/candidate flags use retained membership after the actual last returned key (`31–39`), never current Case facts. Empty terminal membership returns a truthful null Candidate/zero header rather than a fake successful seal.

`broker/src/bpay-next-preparation.js:257,348–362` closes request/member-kind/header/row schemas and validates native Candidate, revision1, exact item/selected/excluded arithmetic, expected/sealed counts, uploaded status for SEALED membership, terminal flags, bounds and contiguous cursor advancement. Native CASE_PAGE/CASE_SEAL requests and their replay receipts remain unchanged. The server support is sufficient for the original journal's bounded recovery; this review does not claim the frontend journal or actual lost-response journey has run.

The common Workbench reader differs from current2340 only by factual `case_state` and the exact Case/Candidate PK join (`0113:513`, `899`). The Worker schema adds nullable Case state matching the native four-state Case enum (`broker/src/bpay-next-workbench.js:352,424`; core migration `03102026_1324:360`). Original PAUSED classification remains final1946 WAITING/CASE_PAUSED and nonselectable. No background recoverable amount or browser headroom is added here.

## Closure and review limits

The saved `final-definer-closures.json` names all seven exact functions, including the private four-argument helper. Prepare/completion/week fact remain final0102; preparation/common reader/new overload remain final0113; LEAF is now final0121 with 0102 retained as the permitted immutable predecessor. `current-contract.json` adds that private-only invoker signature and changes only the six named predecessor definitions. The new helper has owner-only EXECUTE; the existing public preparation RPC retains service-only execution. All three files issue schema reload after definitions/grants. No browser role, new public table, trigger, financial owner or producer is introduced.

Saved-source closure hashes match the current `LOCAL_SCHEMA_CHECKPOINT.json` inventory using the repository's actual closure algorithm (`scripts/cloudtms-db-release-lib.mjs:461–492`, path + NUL + LF source + NUL):

- 0102: `18a8b7da604ad7cb90c20d7f32417ac04bd5bbea3a011b30782089bd75142bdd`
- 0113: `43e29356ab9b3046a82fc28ecf1303ef72b11ec726dbbccf034019b64822dc98`
- 0121: `8673e3ff80aa9f3bdb0fd77cb517763505d3ddbfe550fe6b720b80a4b593a5d8`

A preliminary raw-file-hash concern was withdrawn after checking this closure algorithm. The reviewer did not run the local compiler, contract gates, DB checks or JavaScript syntax checks; the root owns those normal checks and their receipts. This source review supplies no installed/app tick and makes no claim that the authored acceptance journeys passed.

## Final native control point and source-gate correction

The saved `11102026_0121_banking_pay_next_case_choice_control_point_v1.sql` is a single LEAF successor. An in-memory body comparison with preserved0102 found exactly the two control lookup lines changed: `MAX(sn.control_sequence)` becomes the exact Case-component query `SELECT sn.control_sequence ... WHERE sn.case_component_id=cc.id ORDER BY sn.control_sequence DESC LIMIT 1`. The native field is positive, non-null BIGINT (`10102026_1100_banking_pay_next_snooze.sql:22`); both expressions return the latest value, and absence still maps to zero. All Case-only enrichment, original Work basis, immutable rule pairing, current scope/eligibility and capture owners remain unchanged. No money operation or extra command is introduced.

The `scripts/bpay-next-source-gates.mjs:1036–1046` delta adds only two exact-signature native type proofs: schema-declared `SN.CONTROL_SEQUENCE` for the Case LEAF query and schema-declared BIGINT `MEMBER_NO` for the original <=100 retained Case page projection. It requires the actual literal relation/query shape, including the bounded page order/limit for the latter. It does not change forbidden-construct rules, relax hazard classification, replace baselines, add blanket aggregate allowances or introduce a new test/tool. Installed0102 is preserved instead of being edited to satisfy the gate. This reviewer inspected the source; no gate was run by the reviewer.

The existing snooze indices (`10102026_1100:45–50`) have active scope, active Work, Work-scope history and due-date keys. None provides the Case-component/latest-control key needed by0121. The smallest necessary private support is one non-unique B-tree index:

```sql
CREATE INDEX bpay_next_snooze_case_control_point_idx
ON private.bpay_next_snooze(case_component_id,control_sequence DESC)
WHERE case_component_id IS NOT NULL;
```

Case-component identity is already a genuine exact native key/FK, so Candidate/Case prefix columns, INCLUDE columns and a new history owner are unnecessary. The predicate must include inactive rows: the latest cleared/amended control remains the accepted version evidence. An active-only index would not support the exact query and could encourage a semantic change. Equal control values need no extra tie-breaker because the query returns only that value. A single dated migration containing this index is indispensable point-seek support, with no public table, new protocol or broad optimization scope.

The root's saved `supabase/migrations/11102026_0125_banking_pay_next_case_choice_control_point_index_v1.sql:9–11` was subsequently read in full. It creates exactly that non-unique private B-tree index with the non-null Case-component predicate in one normal migration transaction. It contains no business-row write, grants, owner rewrite, trigger, public object or extra optimization. Its key order supports equality on the exact component and the native descending LIMIT1 control query. The pending index source finding is closed. The reviewer performed no migration/DB operation; root's required checks are running separately.

Root reported the normal local PG17 load running. The saved checkpoint subsequently records `LOCAL_SCHEMA_COMPILED` at `2026-10-11T00:24:42.852Z` and its0121 closure matches the saved source hash above. This reviewer did not observe root's final compiler logs or connect to a database; that saved receipt is compile-only evidence and is not independent runtime/installation proof.
