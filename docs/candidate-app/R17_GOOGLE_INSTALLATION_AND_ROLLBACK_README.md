# CloudTMS Candidate Daily Phase 3 R17 — Google installation and rollback bundle

Date: 18 August 2026
Environment: TEST only
Google source authority: inherited R16 source, unchanged by R17
Bridge state required throughout this first qualification: `false`

## Purpose

This small bundle contains the complete, unredacted, copy/paste-ready source for the existing Availability API and NEW MASTER ROTA Apps Script projects. It also contains the exact pre-change `Code.gs` rollback source for each project.

This bundle does not contain secret values. Existing Script Properties remain in the Google projects and must not be copied into source files.

## Project 1 — Availability API

Install both files from `01_AVAILABILITY_API/INSTALL/` into the existing Availability API project:

1. Replace the existing `Code.gs` with `Code.gs`.
2. Add or replace the separate file named `CloudTMSCandidateBridge.gs`.

Rollback material is in `01_AVAILABILITY_API/ROLLBACK/`:

- restore `Code.gs` from `Code.gs`;
- remove `CloudTMSCandidateBridge.gs` if a complete source rollback is required;
- return the existing web-app deployment to the previously recorded active version.

## Project 2 — NEW MASTER ROTA

Install both files from `02_NEW_MASTER_ROTA/INSTALL/` into the existing NEW MASTER ROTA project:

1. Replace the existing `Code.gs` with `Code.gs`.
2. Add or replace the separate file named `CloudTMSCandidateBridge.gs`.

Rollback material is in `02_NEW_MASTER_ROTA/ROLLBACK/`:

- restore `Code.gs` from `Code.gs`;
- remove `CloudTMSCandidateBridge.gs` if a complete source rollback is required;
- return the existing web-app deployment to the previously recorded active version.

## Mandatory state before either installation

Verify, without displaying any secret value:

```text
CLOUDTMS_CANDIDATE_BRIDGE_ENABLED=false
```

in both projects.

The packaged operator baseline is:

```text
Availability active version: 216
Availability rollback version: 215
NEW MASTER ROTA active version: 102
NEW MASTER ROTA rollback version: 101
```

Reconfirm the live values before editing because Google state may have changed.

## Disabled-path acceptance order

1. Record/export each current Apps Script project, deployment ID/version, trigger inventory and Script Property names. Never export secret values.
2. Confirm both bridge flags are false and remain false.
3. Install and save the Availability API files first.
4. Create a new immutable Apps Script version and update the existing Availability web-app deployment so its URL remains unchanged.
5. Test the existing phone app, legacy login, tiles, one safe legacy availability journey and the relevant emergency journey. Confirm the normal Sheet/cache result remains correct.
6. Confirm no CloudTMS request, bridge retry, bridge log, `CTMS_P3_*` operation state, source link, generation or receipt was created.
7. Install and save the NEW MASTER ROTA files.
8. Create a new immutable version and update its existing deployment if that deployment is part of the current Master journey.
9. Run one normal legacy rota publication. Confirm its existing Availability API publication, Sheets and old phone-app tiles remain correct.
10. Again confirm no CloudTMS request, bridge state or Candidate Daily database row was created.
11. Stop and roll back the affected project immediately if any legacy result, trigger, Sheet, cache, emergency behaviour or deployment URL differs.

Do not enable either bridge merely because the disabled tests pass. Enabled coexistence proving is a later, separately controlled step. When that later window is authorised, Master Rota is enabled and observed first; Availability API is enabled only after the generation path is proven.

## Fail-safe invariant

With the property missing or equal to `false`, each helper returns before reading its other CloudTMS configuration or performing any CloudTMS work. The legacy `Code.gs` seams preserve the existing legacy result.

## Included supporting files

- `SCRIPT_PROPERTIES_NAMES_ONLY.md` lists property names and setup rules without secret values.
- `SHA256SUMS.txt` binds every source and rollback file in this bundle.
