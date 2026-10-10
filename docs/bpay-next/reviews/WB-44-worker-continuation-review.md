# WB-44 Worker continuation source review

Read-only independent review, 10 October 2026. Read the full WB-44 card and all D01–D13 rows, the saved continuation design, both changed Worker modules in full, the1429 task schema and service/CLAIM/STEP implementation, actual index factory/queue/minute/scheduled lifetime, and the existing continuation classifier. No tests, SQL, runtime, fixtures, network, business calls or product edits were performed.

## Initial verdict

The actual queue and minute wiring reaches the new private metadata lane without changing financial kinds,0280, the financial adapter, enrollment protocol or index dispatch. One finite reply-validation finding remains open below. The announced due-snooze/week action was not yet in the source snapshot reviewed here and remains pending fresh review; this is not a completed D04 verdict or whole WB44 acceptance.

### H01 — bind non-stale page success to the actual request and page bound

`bpay-next-runtime-rpc.js::workbenchResult` currently accepts any canonical positive generation/owner epoch/nonnegative step number and `rows_visited<=100` for STEP. A non-stale READY/DONE reply for a different generation or unchanged step number is consequently treated as a successful completed page, and a reply with more rows than a smaller actual requested limit is accepted. The background consumer then publishes the next CLAIM and acknowledges the original message.

The actual1429 SQL has narrower finite tuples: CLAIM always returns zero rows and no progressed page; every stale result also returns zero rows/nonprogressed; a fresh successful STEP returns READY or DONE, the same generation/local owner epoch, incremented step number, `progressed=true` and rows bounded by its actual input limit. Its inner rolled-back page failure returns ERROR/zero/nonprogressed. Add these exact checks without rejecting legitimate stale replies that show a newer stored generation or an already completed task. Use BigInt for the step increment/counters, retaining the existing canonical bounded string contract. No next-phase computation, current-lease lookup or new protocol is needed.

## Source evidence that passes this bounded review

- Fixed transport adds only WORKBENCH_WAKE_PAGE/CLAIM/STEP, mapped to the one existing new SQL service wrapper. Its exact p_action/task/args construction carries the original queued STEP nonce, local owner epoch, generation, step number, phase and cursor. Limits remain1024 request bytes,32768 streamed response bytes, eight-second attempt and100 page rows. Existing HTTPS/loopback/service-role/header/redirect controls remain in the actual factory.
- Successful replies use duplicate-key-aware JSON parsing, strict UTF-8, depth/stream/deadline bounds and exact own-data fields. Wake pages accept at most the requested number of distinct native task IDs and finite adoption state/canonical revisions/counts. Task results are restricted to the actual type/phase/status set, exact task ID, canonical bounded bigint strings, finite SQLSTATE error or null, and fresh CLAIM-only nonce disclosure. H01 concerns the additional cross-field request correlation, not a missing response byte bound.
-1429 service independently requires JWT service_role and current NEXT/module epoch. Its browser roles and direct private function/table access are revoked. CLAIM locks Candidate control then task, refuses a live lease/early availability/old module epoch, and creates a new local epoch/nonce only for an eligible task. A lost CLAIM reply cannot recover or adopt its secret: re-delivery sees stale LEASED with null nonce and retries until the actual lease expires.
- STEP retains the exact original request. SQL compares status/module/local epoch/nonce/generation/step/phase/cursor and lease expiry before page mutation, including the plain-discovery snapshot. A completed or superseded delivery yields a stale token-free result. READY from a stale STEP only causes a new CLAIM wake; no active lease or current nonce is adopted. An ERROR remains retryable/pending, and all thrown/unknown/malformed results remain retryable with no successful ACK.
- Fresh CLAIM publishes exactly one STEP through awaited `sendBatch`; a finished STEP publishes exactly one CLAIM before ACK. A durable DONE task is the positive terminal evidence for ACK. A failed send leaves the current message retryable; SQL task state remains discoverable by recovery. There is one adapter call per delivered message, no recursive page drain and no inline financial processing.
- Minute adds an independent WORKBENCH recovery lane after the existing independent financial/expiry/cleanup lanes. Each lane catches its own failure, so one refused/sent/unknown page does not prevent the others from receiving their turn. WAKE only publishes stored task IDs in CLAIM envelopes. SQL rotates due wake time while retaining READY/ERROR or expired-lease authority; queue loss never completes the task.
- Actual `index.js:40997` dependency factory supplies the changed runtime RPC and the existing queue binding. Queue41065 checks fresh owner, retains all legacy disposition behavior, then41090 calls this background module. The continuation classifier treats explicit runtime_action as a NEXT candidate and delegates exact schema validation to the actual background adapter. The exported queue handler201263 awaits this owner-gated entry. Only the actual minute cron (`* * * * *`,1314–1318) calls recovery at201494, and its task is retained in the scheduled lifetime aggregate. No additional index rewiring is necessary to reach the new discriminator.
- The saved diff contains only these two Worker modules. Original ENROLL/WAKE/expiry/cleanup parsing, financial kind list, financial consumer fallback and actual index remain unchanged in this batch. Fixed logging contains no queue body, nonce, SQL message, bank detail or Candidate display payload.

## Initial source hashes

| Source | SHA256 |
|---|---|
| runtime RPC | `8378db76f063d38aaf6e29861bf31756a7e4ab4ba92c49f3f9b78f9762faa24b` |
| background | `e7037e62639d38c9311db907f8cbd7d5d3de9366564addfc416f79a353617572` |
| actual index wiring | `eb507f1fa44ae63b75f3c86a86dc5217753ad6fea5fd9321dcce55d70fad2c14` |
|1429 service/task repeatable | `870833b253eb3aee66dc2146069cef8cbfb0424274f02aa5ff2f01fc6a3adc53` |
|1429 task migration | `03e28413e6f1c692ffe8aadcf7de8fe1b893b8718db0412dcdd7dcf9806726fb` |

These bind the initial source read, not a later edited batch. H01 closure and due-action review need fresh bytes/hashes. No installation, active queue, stored projection or application acceptance was inferred. SQL projection/seed/conservation/fanout correctness remains its separate implementation review; validated `rows_visited` alone does not prove cumulative native writes.

## Fresh H01 closure and due-lane review

Re-read both changed Worker modules in full after the saved fix and DUE_PAGE addition. **H01 is closed.** The adapter now enforces CLAIM/stale/ERROR zero rows/nonprogress, STEP's actual input row limit, and fresh READY/DONE success with progressed=true, unchanged generation/local owner epoch and BigInt step+1. Stale replies can still describe the genuinely newer stored generation and are not incorrectly bound to the old request. The queue's safe stale/pending/send/ACK behavior remains as reviewed above.

The added fixed WORKBENCH_DUE_PAGE action uses the same service wrapper, null task ID and exact `{limit}` input; its sole reply field is canonical bounded `rows_visited`, checked against the actual input limit. Actual `runBpayNextRecoveryTick` invokes an independent WORKBENCH_DUE lane before WORKBENCH WAKE. It publishes no synthetic work receipt or task, and a due failure does not prevent existing financial lanes or the genuine task WAKE lane. The following two SQL support defects prevent a completed D04 source verdict despite correct Worker transport/wiring.

### H02 — the due watermark cannot mutate the guarded business snooze row

1429 DUE_PAGE currently updates `private.bpay_next_snooze.workbench_expiry_version` after queuing its Work tasks. Final1100 `bpay_next_snooze_metadata_guard_v1` is still attached BEFORE every snooze UPDATE and requires the original business command status RECEIVED (343–347), a version increment and larger control sequence (350–356). A retained completed SET command cannot qualify, so this update refuses and rolls back the entire due transaction.

A bypass in that guard alone would also be insufficient: final1100 event guard387–391 binds `after_state` to actual `to_jsonb(snooze)` and the next genuine event's `before_state` to the previous immutable `after_state`. An intervening metadata column update alters that snapshot and breaks a later genuine SET/CLEAR chain. Preserve these business owners and immutable events with a tiny separate private snooze-id/version watermark receipt; do not manufacture a financial command, event or digest to record metadata completion. Registry confirms both existing snooze guards remain final1100. Its deferred receipt owner is not a substitute for the failing BEFORE guard.

### H03 — CASE snoozes cannot enter the Work-only due path

The existing1100 schema explicitly makes `scope_kind='CASE'` equivalent to `work_id IS NULL`.1429 due snooze discovery currently selects every active expired snooze without a non-null Work predicate. Its UNION can retain a null Work member; the loop then invokes `queue_origin_v1(NULL)`, whose strict native Work lookup fails. Such a row can repeatedly poison the current due page.

Keep the Work-only lane explicit and handle the actual supported Case due identity through a genuine bounded Case projection path, or explicitly record that named Case recovery remains incomplete. Silently feeding null Work IDs, marking Case snoozes visited without reclassification, or claiming all snooze recovery complete is not valid. This is a real nullable native scope, not hypothetical malformed business data.

Fresh read hashes: runtime RPC `2defb3c7211bd66c8aca229c9ee97a6e43700259b3c3114e1abb80df5e332766`; background `e7037e62639d38c9311db907f8cbd7d5d3de9366564addfc416f79a353617572`; index `eb507f1fa44ae63b75f3c86a86dc5217753ad6fea5fd9321dcce55d70fad2c14`;1429 service repeatable `870833b253eb3aee66dc2146069cef8cbfb0424274f02aa5ff2f01fc6a3adc53`;1429 migration `03e28413e6f1c692ffe8aadcf7de8fe1b893b8718db0412dcdd7dcf9806726fb`. These source reads occurred during the root's additive batch work; later fixes need fresh binding. No execution or full-card PASS is claimed.

## Corrected due bridge: H02 and H03 closed

Fresh-read the corrected1429 migration, private snooze callback, seed discovery/step, full public service, Worker phase schemas and actual background diff. **H02 and H03 are closed. Source PASS for this Worker continuation/transport/wiring review and its corrected due interface support; no open finding in that bounded scope.** This supersedes the initial refusal verdict, not the independent broader SQL projection/fanout/seed review or future application acceptance.

- The guarded native snooze table has no new watermark column and is not updated by due recovery. The separate private `bpay_next_workbench_snooze_due` row records a genuine native snooze ID/version, exactly one Work or Case identity, actual until_date and pending metadata. Its due index, private RLS/owner policy and browser/service table revokes are explicit. The narrow AFTER callback on the existing private native snooze copies only real INSERT/version/date/active changes while NEXT is active. Existing business command/version/receipt owners and immutable full-row snooze event snapshots remain unchanged.
- Existing active snoozes are initialized through genuine `SEED_SNOOZE`: a fixed native UUID upper fence at WAKE bootstrap, indexed active-ID keyset and at most the requested100 actual IDs per step. Their Candidate/Work/Case identities are discovered and preheld before the seed task/metadata work; the row insert reads the real current version/date/active value. Concurrent genuine native changes enter the same private callback rather than depending on a historical sweep or first UI open. The source task phase transition includes SEED_GROUP → SEED_SNOOZE → SEED_METADATA, with persisted cursor/step progress.
- Due scope is now a distinct bounded set of actual `WORK:<uuid>` or `CASE:<uuid>` keys from that ledger and prepared FUTURE_WEEK rows. Neither scope creates a null Work member. It preholds sorted Candidate controls, exact Works, exact Cases and the bounded private due receipts. Work queues its actual ORIGIN task and Processing barrier; Case invokes the genuine existing Case projection owner in the same transaction. Only the exact visited private receipt IDs with unchanged recorded version are marked nonpending. No native snooze is cleared/expired, no hold is expired, and no financial command/event is manufactured.
- Source error/unknown outcome rolls back or leaves the private receipt pending; a successful durable handoff is recoverable through the same subsequent WAKE page. The existing1252 migration180 supplies the actual prepared FUTURE_WEEK `(eligible_from_date,id)` partial index. The SQL returns exactly the visited scope count, which the Worker checks against its request limit. No financial amount or snapshot crosses the due transport.
- Runtime input and type-specific result phase lists both now include SEED_SNOOZE only for SEED. H01 checks remain intact. The background's independent WORKBENCH_DUE → WORKBENCH WAKE order, awaited queue sends and owner-gated index factory/queue/minute entry remain unchanged. Existing financial kinds/0280/enrollment and business HTTP routes are unchanged.

Final source hashes for this closure:

| Source | SHA256 |
|---|---|
| runtime RPC | `a6b18ee9667831bf26fd6195250f703594c912ae14172a7aff4ee9871f8771d5` |
| background | `e7037e62639d38c9311db907f8cbd7d5d3de9366564addfc416f79a353617572` |
| actual index wiring | `eb507f1fa44ae63b75f3c86a86dc5217753ad6fea5fd9321dcce55d70fad2c14` |
|1429 service/task repeatable | `6a813b2ebfa789cadf23a8570941e6ec03f1b33fd0fd5e18397a52ccda3149f5` |
|1429 task/due migration | `c89b160ebb66275b9f60a6e969b7ab15a961750d56b57d95447ed4d5c53d5978` |

No SQL, business call, test or runtime execution was performed. This is source evidence only; it does not certify installed/active consumer behavior, native cumulative page economics or a completed whole WB44 card.
