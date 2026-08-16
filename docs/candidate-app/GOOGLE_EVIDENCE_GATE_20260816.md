# Candidate Daily Google Evidence Gate — 16 August 2026

## Disposition

The authorised read-only Google Evidence Gate is complete for the two Google systems that are in scope for Candidate Daily coexistence:

1. the **Availability API** Google Sheet and bound Apps Script project; and
2. the **NEW MASTER ROTA System** Google Sheet and bound Apps Script project.

The gate was evidence-only. No Sheet cell, tab, named range, Script Property, trigger, manifest, Apps Script file, deployment, execution, OAuth grant or Google configuration was changed. No Apps Script function was executed. No property value was read or exported.

This evidence does not authorise Phase 3 Google editing or deployment. It removes the earlier uncertainty about which effective projects, files, versions and trigger owners must be protected when that later work is separately authorised.

## Scope boundary

The inspected Google projects contain considerably more functionality than Candidate Daily. Only the following bounded seam was classified:

- Availability data and its legacy browser/API compatibility path;
- Master Rota availability consumption and publication dependencies;
- Daily and Emergency functions required to keep the new Candidate App and temporary legacy app compatible;
- deployment/file/trigger/property-name evidence needed to avoid editing the wrong authority;
- the current state of the historical `ai_startDailyPings` reference.

Unrelated rota, credentialing, compliance, contact, messaging, document and administrative functions remain outside this implementation. Their presence in either Google project does not make them Candidate Daily scope.

## Evidence method and safety

Evidence was captured through the user's already authenticated Chrome session for `arthurrai2006@gmail.com`, using read-only Apps Script and Google Sheets views. The review recorded file order, version identity, manifests, scopes, triggers, property-name inventory and relevant Sheet topology.

The following were deliberately not copied into this document or handover:

- Script Property values;
- OAuth tokens, browser state or cookies;
- personal data rows;
- live availability, rota or emergency payloads;
- complete public deployment URLs;
- unredacted Apps Script source.

The two user-certified unredacted attachments remain the static source authority. Their bytes were compared locally against the effective current `Code.gs` source. Sanitised structural copies from R5 remain suitable for independent review, but are not deployment payloads.

## Availability API authority

| Evidence item | Effective fact |
| --- | --- |
| Spreadsheet identity | `1BSomZL0jRse5SGfTgADwswVmIjY4mCMfvDAfQxxIUA8` |
| Bound project | Availability API |
| Current Head `Code.gs` | 638,203 bytes/characters in the effective editor export |
| Current Head SHA-256 | `eacd187564ea9b0f00c1830f9240c6afcfe1a0d0611162c1bdf9b9fd6bbb3b3f` |
| Certified-source comparison | Exact byte match to the user-certified Availability Apps Script attachment |
| Active web deployment | Version 215 |
| Deployment/source comparison | Active version 215 matches the current Head hash exactly |
| Web execution owner/access | Executes as deployment owner; legacy anonymous access remains present |
| Effective files | `appsscript.json`, `Code.gs`, `MessageEditor.html.html`, `EmergencyContactsModal.html`, `ContactDetailsModal.html` |
| Installed triggers | 0 |
| Manifest | V8; `Europe/London`; Stackdriver; Drive advanced service v3 |
| OAuth scopes | 7 effective scopes, including Sheets/Drive, external request, mail and container UI authorities |
| Script Property names | 50 names inventoried; values neither read nor exported |
| Relevant tabs | `Availability API Links`, `Logs` |

The existing legacy browser, login behaviour and internal `msisdn`-based lookup remain unchanged. The later compatibility seam is server-side Apps Script to CloudTMS only. The old browser must never receive CloudTMS HMAC material, canonical Candidate UUIDs, Supabase authority or Candidate access credentials.

## NEW MASTER ROTA System authority

| Evidence item | Effective fact |
| --- | --- |
| Spreadsheet identity | `1eEnrLMhLX_FzuO7sdUfAzEnvuXAwmR79fBntKe5Gp04` |
| Bound project | NEW MASTER ROTA System |
| Current Head `Code.gs` | 1,188,256 bytes/characters in the effective editor export |
| Current Head SHA-256 | `c3ae9c480a97ad2771312f5f453adbe7049c07219f89624f75df543d319fa0a8` |
| Certified-source comparison | Exact byte match to the user-certified Master Rota Apps Script attachment |
| Active web deployment | Version 100 |
| Deployed version SHA-256 | `f41dad2e09df43a48ca29acdb2272b3fd441ee3d371865c3a49411caf682099d` |
| Deployment/source comparison | Historical read-only gate found active web version 100 differed from current Head. The user then deployed that certified current Head as active web version 101 on 16 August 2026. Installed triggers continue to execute current Head. Phase 3 must still re-read and hash-check immediately before any Google edit. |
| Effective files | `appsscript.json`, `Code.gs` |
| Installed triggers | 12 inventory entries: 8 enabled and 4 disabled |
| Enabled trigger owners | `ai_refreshWatchdog`, `updateCredentiallyDocs`, `hourlyBackup`, `ai_dailyRefresh`, `refreshAllPatientSheetColors`, `refreshComplianceCache`, `dailyWhatsappReminders`, `runDelayedUpdates` |
| Disabled trigger entries | Four disabled `updateCredentiallyDocs` entries retained as observed evidence |
| Manifest | V8; `Europe/London`; Stackdriver; Sheets v4, Drive v3 and Docs v1 advanced services |
| OAuth scopes | 10 effective scopes |
| Script Property names | 28 names inventoried; values neither read nor exported |
| Sheet topology | 34 tabs; Candidate Daily-relevant tabs include `Availability`, `EmergencyAlerts`, `Candidate List`, `Healthroster` |
| Named ranges | None |

The original Head/deployment difference was a binding R6 safety fact and has been operationally superseded by the user's deployment of the certified current Head as active web version 101 on 16 August 2026. This is user-provided deployment evidence, not permission to edit Google. Trigger-based changes and deployed web-app changes must still never be assumed to use the same source version. Phase 3 must re-read current Head, verify its hash, identify whether a patch targets Head-trigger execution, deployed web execution or both, and prove the deployed version after publication.

## Effective function order and duplicate ownership

The effective file order is frozen above. Both certified `Code.gs` sources contain repeated function names accumulated over the life of the projects. Apps Script's effective winning definition follows loaded source order, so a later patch must be based on the effective current Head and placed deliberately at the intended winning seam.

The R5 sanitisation/function-index evidence remains the review-safe duplicate inventory. The unredacted certified source remains the byte authority. Phase 3 must re-read the effective Head immediately before editing, compare its hash to this gate and regenerate the function inventory if the hash has changed.

No duplicate function was renamed, removed or “cleaned up” during this gate. Unrelated duplicate/legacy defects are explicitly outside Candidate Daily scope.

## `ai_startDailyPings`

`ai_startDailyPings` has no declaration in either effective current Head. One historical Master Rota menu/reference remains, and there is no installed trigger for that name.

Disposition:

- it is an orphaned/unimplemented legacy reference;
- it is not required by the accepted Candidate Daily architecture;
- it is not revived, added, repaired or deleted by Phase 1A;
- any later cleanup is a separate, explicitly authorised legacy-maintenance decision.

## Master Rota consumer/writer boundary

The coexistence contract remains:

```text
temporary legacy browser
  -> existing Availability Apps Script behaviour
  -> smallest trusted server-side compatibility adapter
  -> signed CloudTMS system request
  -> one CloudTMS Daily authority
  -> bounded projection back to Google Availability/Master consumers

new Candidate App
  -> Candidate public broker
  -> same CloudTMS Daily authority
```

Master Rota and Emergency consumers must remain usable while both clients coexist. CloudTMS becomes the one business/effect authority only after the later Phase 2, Phase 1B, Phase 3 and controlled cutover gates. Before cutover, this evidence does not alter existing Google ownership.

## Quota and concurrency implications

No quota setting was changed. The accepted R5 bounds remain the implementation authority:

- Candidate reads: 60 per minute per Candidate, at most 6 in flight;
- Candidate commands: 12 per minute per Candidate, at most 1 in flight;
- external-effect commands: 6 per minute per Candidate and 1 per effect key;
- signed Google-system calls: 120 per minute per key ID and at most 8 in flight;
- Candidate bodies: 32 KiB maximum;
- signed system bodies: 256 KiB maximum;
- DB deadline: 10 seconds;
- retained Google read/preview deadline: 12 seconds;
- external-effect call deadline: 20 seconds followed by status reconciliation.

Apps Script's own execution and UrlFetch quotas remain operational constraints for Phase 3 soak testing. This gate does not increase quotas or add triggers.

## Gate conclusion

The two effective Google authorities are identified and their current source/deployment relationship is known, including the later user-confirmed Master Rota web version 101 deployment. Phase 1A may be reviewed without further Google evidence. Phase 3 remains separately gated and must use the smallest possible server-side compatibility changes, preserve the existing legacy browser, protect Master/Emergency behaviour, and revalidate the effective hashes before any edit or deployment. Decommissioning the temporary legacy browser and `LEGACY_COMPAT` facade must not decommission the Availability Sheet/Apps Script service, Emergency functions, Master Rota publication, signed CloudTMS synchronisation, projections/freshness or specialist services; each retained owner needs a separate later migration and acceptance decision.
