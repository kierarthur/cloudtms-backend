# Candidate Daily Phase 3 diagnostics and rollback

## Diagnostic sources

### Safe configuration check

Run:

```text
Availability: ctmsP3_configurationStatus()
Master Rota:  ctmsP3_masterConfigurationStatus()
```

The result contains property names/presence booleans only. Never copy property values into evidence.

### Apps Script Executions

When enabled, filter execution logs for:

```text
system = CANDIDATE_DAILY_PHASE3
```

Correlate by `correlation_id` or `operation_id`. A Master item rejection may additionally contain only `rejection_items` entries with numeric indexes and approved safe error codes. The intended log contains no candidate identity or payload.

### CloudTMS

Use the same correlation ID to inspect the TEST Worker and database receipt. Do not print authorization headers, request bodies, source HMACs or candidate rows in a handover.

### Persisted recovery state

Availability uncertain-operation keys begin `CTMS_P3_OP_`. Master R13 uses `CTMS_P3_ROTA_PENDING_INDEX`, `CTMS_P3_ROTA_MANIFEST_*` and numbered `CTMS_P3_ROTA_BODY_*` chunks. Inspect only key names and safe manifest metadata in a controlled diagnostic. Never publish stored factual bodies. Master records do not expire into replacement identities; they are removed only after complete success or an exact approved terminal rejection.

## Failure interpretation

| Symptom | Safe interpretation/action |
| --- | --- |
| Flag false and legacy fails | Not caused by an executing Phase 3 bridge; compare the exact pasted Code with rollback and inspect the legacy system. |
| Flag true; CloudTMS read fails | Legacy tile envelope should still return. Disable and inspect HMAC/source-link/Worker correlation. |
| Availability execute uncertain | Do not create a new key. Let the same factual request status-probe; only exact status `404 / NOT_FOUND / DO_NOT_RETRY` permits the first and only exact retry. |
| Availability `409 / STATUS_CHECK` | Retain the frozen operation and status-probe. HTTP 409 alone is never terminal authority. |
| Malformed/unrecognised 4xx | Preserve as uncertain; do not clear the operation or invent a replacement key. |
| Deferred/busy legacy response | Expect no CloudTMS operation until the queue is revalidated and successfully written during flush. |
| Mixed legacy result | The signed body must contain only `applied:true`, non-deferred rows that were actually written. |
| Master publish uncertain | Preserve the index/manifests/body chunks and replay the same ordered batch/key/body/correlation before building any new event. |
| Master `409 / BATCH_IN_PROGRESS / STATUS_CHECK` | Non-terminal. Retain and replay the exact frozen batch; never log overall completion. |
| Master HTTP 200 with every item `COMMITTED`/`REPLAYED` | The receipt UUID, exact outcome count and complete unique index set must validate before the batch advances. |
| Master HTTP 200 with an approved item `REJECTED` | Terminal rejection, never completion. Confirm the bounded log contains only item index/safe code and the frozen event is cleared through the explicit terminal path. |
| Master HTTP 200 with malformed, incomplete, duplicate-index or unknown outcome | Preserve the exact frozen event. Do not clear state or invent another batch. |
| Master stored body missing/hash mismatch | Disable and inspect the bounded key/manifest metadata. Do not delete the index or construct a replacement event. |
| Master property capacity preflight fails | No POST should occur. Keep legacy operation unchanged; reduce the separately reviewed TEST scope or storage pressure before retrying. |
| Booked/system-blocked projection | Expect `DEFERRED_OVERLAY`; do not force a Sheet overwrite. |
| First-generation identity missing/ambiguous | Disable. Correct the admin-entered CID1 value so it resolves to exactly one active existing Candidate, then allow a new factual Master generation to retry automatic binding. Do not insert a source link manually or nominate a Candidate UUID in the browser. |
| First-generation identity conflict | Disable. Inspect only bounded ownership facts. A source HMAC or active PRIMARY source already belongs to a different authority; never overwrite it automatically. |
| Historical source-HMAC conflict | Disable. Any prior owner in PRIMARY, OVERLAP, RETIRED, REJECTED, expired or future-valid history remains authoritative. Do not delete/retire history to make automatic rebinding succeed. |
| Same-Candidate historical HMAC is presented again | Treat as `IDENTITY_LINK_CONFLICT`. First generation must not silently reactivate retired/rejected/non-current history; use only a separately reviewed rotation or repair authority. |
| Normalized CID1 uniqueness preflight fails | Stop before index creation. Identify the duplicate group through controlled Office identity review without printing full CID1 values; do not let generation choose a winner. |
| Top-level `409 / IDENTITY_LINK_CONFLICT / DO_NOT_RETRY` | Terminal rejection. Confirm bounded safe logging, exact frozen-operation clear and no mirror-complete log. |
| Candidate read works only after direct fixture updates | Invalid evidence. Repeat through reconciliation/readiness and `candidate_daily_authority_transition_atomic_v1` with independent actor/approver and exact cursors. |
| Safe migration starts while 17.6/18.1 matrix is pending/failed | Cancel/stop the migration before SQL mutation. The migration job must depend on the exact-commit matrix. |
| Authority-transition call aborts with `IDENTITY_LINK_CONFLICT` instead of returning an indexed outcome | Wrong/pre-R17 authority is installed. Keep bridges false; do not retry under a new key; compare the exact installed function definition with the reviewed R17 repeatable. |
| A mixed transition batch loses a valid sibling after another item conflicts | Stop. The expected R17 item-subtransaction containment is absent or regressed. Preserve the receipt and do not manually recreate a transition. |
| `40P01` between generation and Office transition | Stop all Candidate Daily writers. Verify both writers acquire the identical SOURCE advisory namespace before Candidate scope; do not reorder locks ad hoc in only one caller. |
| Malformed source-link data aborts before an indexed result | Stop. R17 prelock filtering must perform no integer cast; the item validator must own `VALIDATION_FAILED`. |
| Opposite-order multi-source transition batches block or deadlock | Stop. Confirm deduplicated lexical source-lock order and sorted Candidate-scope row locks in the installed definition. |
| Legacy enabled tiles fail after a committed generation | Correlate the source-HMAC read to the one automatic source link and active generation. The unchanged legacy envelope must still return while the bridge is disabled for investigation. |
| New app cannot see the committed generation | Verify the existing separate Candidate authority transition, entitlement and global feature gate. R15 deliberately creates none of those activation permissions. |
| HMAC/reader retired | Disable. Restore the approved retained reader; never hard-code or log the secret. |
| Quota/lock pressure | Disable, capture bounded timings, and reschedule only after review. Do not add an unreviewed trigger. |

## Immediate rollback

1. Set `CLOUDTMS_CANDIDATE_BRIDGE_ENABLED` to `false` in both projects. This stops new bridge network/log/state/Sheet work without removing legacy functionality.
2. Point each web deployment back to the recorded pre-install version if the web source must be reverted immediately.
3. If Head-trigger code must also be reverted, replace each revised `Code.gs` with its package `rollback/Code.gs` and remove only the new `CloudTMSCandidateBridge` file.
4. Do not change any existing manifest, HTML file, trigger or property unrelated to Phase 3.
5. Re-run the legacy false-path and trigger inventory comparison.
6. Preserve CloudTMS receipts and logs for audit. Do not delete Supabase rows as rollback.

## Rollback authorities

```text
Availability rollback Code.gs SHA-256
eacd187564ea9b0f00c1830f9240c6afcfe1a0d0611162c1bdf9b9fd6bbb3b3f

Master Rota rollback Code.gs SHA-256
c3ae9c480a97ad2771312f5f453adbe7049c07219f89624f75df543d319fa0a8
```

Recompute the SHA-256 after copying. If it differs, stop rather than deploying an uncertain rollback.
