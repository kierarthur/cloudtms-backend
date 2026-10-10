# WB-22 retained memory and PF04 cleanup compatibility

10 October2026. Read-only source census for the two named selection-memory tables. Current PF04 final cleanup0406, WB23 receipt guard2240, current1832 pay-run/accepted-choice guards and CREATE, core1324 and sealed current contract read. No product edits, SQL, tests, modules, gates, runtime or business calls. This note adds no cleanup owner or public monetary seam.

## Actual existing deletion scope

`private.bpay_next_terminal_cleanup_v1(integer)` is still the registered final in `10102026_0406_banking_pay_next_terminal_cleanup_v1.sql`. Its SELECTION lane visits at most100 selection_page PK rows, locks the parent pay_run with SKIP LOCKED, and deletes only the temporary page when parent status is COMPLETE/CANCELLED. Its JOB lane visits at most100 jobs and deletes only eligible DONE/no-lease supported kinds after original COMPLETE/full-enrollment command checks and all retained job-reference exclusions. It never deletes pay_run, run_command, command, run_selection, selection_candidate, work_choice, selected components, captured RuleB settings/query snapshot/pin, frozen money rows or their native origins. It never visits either new WB22 memory table.

Final2240 selection_receipt_guard allows DELETE only after the same terminal parent row lock; it does not authorize memory deletion. Current1832:1839–1846 preparation_expiry_guard refuses every DELETE, including its attached pay_run table, with BPAY_NEXT_EXPIRY_EVIDENCE_DELETE_FORBIDDEN. Core1324:418–434 pay_run has UUID PK and retained actor/pay_date; run_selection/selection_candidate/run_worker/run_command already reference it with RESTRICT. Saved source search found no direct DELETE/TRUNCATE pay_run writer. These observations are source evidence, not execution proof.

**No PF04 cleanup successor or additional retained-job exclusion is needed** when WB22 memory references durable pay_run/command/native origins only. Do not put a job_id, lease_nonce, temporary page or selection-page FK on memory. A new job FK would create a deletion blocker PF04's current finite exclusions do not know about, potentially aborting an otherwise eligible cleanup page; a page FK would defeat the intended temporary-page lifetime.

## Minimal compatible references

| Memory fact | Durable target and closure |
| --- | --- |
| Original captured run | `run_id REFERENCES private.bpay_next_pay_run(id) ON DELETE RESTRICT`; do not cascade memory from a terminal status or parent deletion. Insert guard also binds native actor and pay_date to the exact memory namespace. |
| Actual positive PREPARE command | The sealed contract has `run_command UNIQUE(run_id,command_id)` and `command UNIQUE(id,agency_sequence,module_epoch)`. Nullable genuine tuples can bind these existing keys without adding a pay_run index/column or changing another owner. Require the actual native relation and all-null/all-nonnull tuple shape; requested UUID alone is not command evidence. |
| Previous acknowledged baseline / inherited exclusion basis | Retain a durable HEADER reference and exact namespace. Validate that the target is its acknowledged HEADER, not a same-run LEAF or arbitrary other actor/date/scope row. Keep the original referenced basis immutable; no SET NULL/CASCADE that turns an exclusion into unqualified current data. |
| Captured LEAF / explicit intent row parent | Restrict to its exact captured HEADER/run and native Candidate/Work/revision/approved-line or Case component parent. Use existing native keys and an insert guard for the exact parent tuple; do not create competing financial identity/unique constraints merely to ease a memory FK. |
| Native financial basis | Reference durable native approved/origin IDs where appropriate. Retain fixed typed equality facts. Do not FK a maintained workbench_item/group/contribution row: refresh deletes/replaces those rows, and a memory FK would obstruct the current classifier. Names/bank warning/metadata revision are not memory authority. |

If HEADER and LEAF share one mode-memory table, a FK merely to run_id is not proof that the target is the HEADER. Use the schema's genuine unique header key, or the finite exact row-kind/header-presence guard in addition to a valid FK. A partial unique index cannot be assumed to be a referenced unique constraint. This is a shape requirement for the two named tables, not a third table or generic reference framework.

## Requested command UUID is not always a real command

Actual1832:495–498 invokes receive_command(PREPARE) and inserts run_command **only when selected_candidate_count>0**. Lines499–500 store accepted_choice_command_id=p_command_id even when the result is EMPTY. Thus an unconditional FK from that requested command UUID to command/run_command would reject the legitimate original empty CREATE or manufacture a command to satisfy the FK.

For positive captures, insert a HEADER carrying genuine command/sequence/epoch only after the actual original receive and run_command insertion exist, using already captured/held facts and no new business lock. The existing current contract's exact command tuples above are suitable. For empty captures, genuine command tuple remains wholly null, and no acknowledged Draft is invented. The separate original request UUID can remain retained as request/replay identity if needed, but must not be mislabeled or FK-qualified as an executed command. Do not rewrite an immutable HEADER's command facts later simply because seal now has a command; choose the original capture placement/shape correctly.

All-null/all-nonnull CHECKs must be explicitly NULL-safe. A MATCH SIMPLE composite FK alone permits a partial-null tuple and does not prove that sequence/epoch belongs to a known original command. The insert guard must also bind the actual original run_command and exact namespace, not just the globally unique command UUID.

## Small private guards and lifetime

Both named tables remain private, FORCE RLS with only the audited effective owner policy, ordinary-role ACL revoked, and no direct browser/service table write. Add the repeatable final private row guard(s) on these new tables only. Captured namespace/native identity/basis/raw intent/context/capture clock are immutable after insert. The only initial lifetime mutation is the design's one genuine null-to-nonnull acknowledgement marker, qualified by the actual original seal path; its clocks and evidence cannot be cleared or rewritten. Exact replay never acknowledges a different boundary.

DELETE and statement TRUNCATE of these captured memory rows must refuse under the initial WB22 owner. Terminal run COMPLETE/CANCELLED, job DONE, finished PREPARE, lease expiry or72-hour preparation expiry is **not** permission to erase actor/date memory. Do not reuse the selection-page delete exemption, trigger disabling, CASCADE or arbitrary current-user bypass. If one multi-table guard is used, branch by table/row kind before accessing row-specific fields; do not rely on CASE short-circuit planning to hide nonexistent NEW fields.

The named future WB28 reset must detach/retire the relevant active persisted baseline and local state so old exclusions are not re-applied, while preserving original accepted/replay evidence. It must not delete original pay_run/command or broaden PF04. No reset action, TTL, new financial command, archive table or date-retirement worker is authorized by this census. A different pay_date naturally has a separate namespace and cannot inherit the earlier baseline; that is default isolation, not a claim that historical rows were deleted. Actual physical retirement, if later required, needs its named owner and must preserve any original HEADER/intent/basis referenced by another retained generation.

This conservative initial retention is compatible with current cleanup and satisfies same-date exact-basis survival until the genuine reset/date-retirement owner is built. PF04 can continue deleting only its proven temporary rows. Selection memory never depends on a temporary page or job to reopen an unchanged untick; it depends on its own captured native facts and durable original run boundary.

Source SHA256:0406 cleanup `434704fd74342fc461eb30ce09d812ff51eea2a05e83be42af9a14a2203fe4c8`;2240 receipt guard `36c7a3aac98fc13431e03f4534ba3ca1242cef2fc735775ebe890e412bb1fb45`;1832 current CREATE/expiry guards `b629fcc003b76ae61ec58c0d35ad2256a4cf9f736e307f5dc80e0a8fb0e20438`; inspected sealed current contract `f8f738e20929f7c92581eda97f7b9faed42eacd8ad62f5184aedda410067dd96`.
