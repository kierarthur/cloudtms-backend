# Phase 3 Apps Script Property catalogue

This catalogue contains property **names only**. Secret values must be installed directly in Google Apps Script Project Settings and must never be added to source, a handover, a log, a screenshot or chat.

These are **Google Apps Script Project Script Properties**, not JavaScript global variables to paste into `Code.gs`. They were installed by the operator in both TEST Google projects on 17 August 2026 with the bridge flag false. Secret values were never read back, printed or packaged.

## Required in both projects

| Property | Secret | Purpose |
| --- | --- | --- |
| `CLOUDTMS_CANDIDATE_BRIDGE_ENABLED` | no | Exact switch. Only case-insensitive `true` enables the bridge. Missing, blank, `false`, `0` and every other value are disabled. |
| `CLOUDTMS_CANDIDATE_BASE_URL` | no | TEST Worker origin during TEST proving. No production origin is authorised by this package. |
| `CLOUDTMS_CANDIDATE_ENVIRONMENT` | no | `TEST` during this phase. The source accepts `LIVE` for a later separately authorised deployment, but this package does not authorise it. |
| `CLOUDTMS_CANDIDATE_GOOGLE_HMAC_KEY_ID` | no | Phase 3 TEST signed-system key identifier accepted by both Candidate Worker layers. Installed; value must be checked for presence/equality without exposing it. |
| `CLOUDTMS_CANDIDATE_GOOGLE_HMAC_SECRET` | **yes** | Random HMAC-SHA-256 signing secret corresponding to that key identifier. Installed in both Google projects and the private Candidate Worker; never in the public broker. |
| `CLOUDTMS_CANDIDATE_SOURCE_HMAC_SECRET` | **yes** | A different random semantic-identity secret used to HMAC the trusted Credentially public ID. Installed identically in both Google projects; R15 submits its derived HMAC with the first generation so PostgreSQL can create the exact source link atomically. |

## Additional Availability API property

| Property | Secret | Purpose |
| --- | --- | --- |
| `CLOUDTMS_CANDIDATE_EXECUTOR_ID` | no | Stable 8–128 character executor identity for projection and external-effect receipt claims. |

## Exact TEST configuration catalogue

Use the following non-secret values in both projects:

```text
CLOUDTMS_CANDIDATE_BRIDGE_ENABLED=false
CLOUDTMS_CANDIDATE_BASE_URL=https://test-cloudtms-candidate-broker.kier-88a.workers.dev
CLOUDTMS_CANDIDATE_ENVIRONMENT=TEST
```

Also install in both projects:

```text
CLOUDTMS_CANDIDATE_GOOGLE_HMAC_KEY_ID=<installed TEST key ID>
CLOUDTMS_CANDIDATE_GOOGLE_HMAC_SECRET=<installed TEST signing secret>
CLOUDTMS_CANDIDATE_SOURCE_HMAC_SECRET=<installed, different TEST source-identity secret>
```

Install this additional non-secret property in Availability API only:

```text
CLOUDTMS_CANDIDATE_EXECUTOR_ID=availability-api-google-test
```

The operator's Wrangler inventory on 17 August 2026 confirmed the following secret-name presence without exposing values:

- the public Candidate broker contains `CANDIDATE_DAILY_GOOGLE_HMAC_PRIMARY_KEY_ID` and does not contain the signing secret;
- the private Candidate Worker contains `CANDIDATE_DAILY_GOOGLE_HMAC_PRIMARY_KEY_ID` and `CANDIDATE_DAILY_GOOGLE_HMAC_PRIMARY_SECRET`;
- both Google projects must receive that same key ID and signing secret through the two Script Properties above;
- the source-HMAC secret must be a separately generated secret used identically by Availability API and NEW MASTER ROTA; PostgreSQL receives only the derived HMAC and never the secret or raw public ID;
- none of these values may be stored in GitHub, written into either `.gs` source file or included in an audit pack.

Before enabling, the operator must verify the Candidate broker accepts the selected key ID as the current primary or retained overlap reader. Do not guess a key ID or reuse the source-HMAC secret as the transport HMAC secret.

## Existing legacy properties

All existing Availability API and Master Rota properties remain unchanged. Do not rename, rotate or remove an existing property merely to install Phase 3.

## Safe configuration order

1. Keep `CLOUDTMS_CANDIDATE_BRIDGE_ENABLED` set to `false` before and throughout source installation.
2. Confirm all other Phase 3 property names remain present in Apps Script Project Settings without reading secret values.
3. Run `ctmsP3_configurationStatus()` in Availability and `ctmsP3_masterConfigurationStatus()` in Master Rota. These return presence booleans only and never reveal values.
4. Prove legacy behaviour while the flag is false.
5. Enable only in a separately controlled TEST proving window after the R16 Worker/SQL authority, normalized CID1 and all-history source-HMAC invariants, ACLs and retained-reader HMAC configuration are verified. A separate source-link prepopulation step is not required.

No candidate-specific allowlist property exists. The product owner explicitly chose a population-wide TEST bridge gate. When the flag is true, every otherwise eligible TEST source row may be included. R15 creates a missing source link automatically when that row's first generation proves one exact active Candidate through its admin-entered CID1 key. Kier Arthur is the first observational phone-app journey only; no raw name, candidate ID or source identity is placed in source or Script Properties.

## Source identity canonicalisation

Both projects derive exactly the same non-reversible source identity:

```text
CLOUDTMS-CANDIDATE-SOURCE-V1\n
<ENVIRONMENT>\n
GOOGLE_CREDENTIALLY_PUBLIC_ID\n
<trimmed Credentially public ID>\n
```

The complete UTF-8 string is HMAC-SHA-256 signed with `CLOUDTMS_CANDIDATE_SOURCE_HMAC_SECRET`. Only the lowercase 64-hex digest is transmitted. The raw public ID, mobile number, email and source secret are not sent in the signed body or structured bridge log.
