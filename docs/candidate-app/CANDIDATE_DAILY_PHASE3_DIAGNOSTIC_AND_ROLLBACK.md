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

Correlate by `correlation_id` or `operation_id`. The intended log contains no candidate identity or payload.

### CloudTMS

Use the same correlation ID to inspect the TEST Worker and database receipt. Do not print authorization headers, request bodies, source HMACs or candidate rows in a handover.

### Persisted recovery state

Availability uncertain-operation keys begin `CTMS_P3_OP_`. Master uncertain-generation keys begin `CTMS_P3_ROTA_`. Inspect only key names and safe state metadata in a controlled diagnostic. Never publish stored factual bodies. The records expire after seven days and are deleted after authoritative completion/stable failure.

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
| Master publish uncertain | Preserve the existing `CTMS_P3_ROTA_` operation and replay the same batch/key/body. |
| Booked/system-blocked projection | Expect `DEFERRED_OVERLAY`; do not force a Sheet overwrite. |
| Source link missing/ambiguous | Disable. Correct Phase 4 bootstrap/mapping authority; do not nominate a different candidate in the browser. |
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
