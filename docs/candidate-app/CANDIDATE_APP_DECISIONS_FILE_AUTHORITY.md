# Candidate App decisions-file authority

## Later-controlling format decision

Effective from 19 August 2026, the current CloudTMS Candidate App decisions authority must be maintained and delivered as a machine-readable Markdown file rather than a regenerated PDF.

The format change does not reduce the required content. The decisions file must retain the same complete level of detail previously required in the Decisions PDF, including:

- all settled product decisions and later-controlling amendments;
- status labels, precedence and display rules;
- workflow, eligibility and authorisation gates;
- legacy-coexistence and minimal-change boundaries;
- Candidate Daily, Google, broker, database and authority-transition decisions;
- safety, idempotency, replay, recovery and failure-state rules;
- phase boundaries, outstanding gates and explicit no-change boundaries;
- decision identifiers and traceability to implementation and verification evidence.

## Handover rule

Every future handover pack related to the Candidate App must contain the latest complete Markdown decisions file. A summary, delta-only addendum or link to an external copy is not sufficient.

The receiving reviewer must be instructed to treat that packaged Markdown file as the current controlling decisions authority.

## Historical PDFs

Previously generated Decisions PDFs remain historical assurance evidence and must not be rewritten or discarded merely because the authority format has changed. They do not need to be included in every new pack when the complete current Markdown decisions file is present, unless a particular review explicitly requires historical PDF evidence.

No new Decisions PDF should be generated unless the user explicitly requests one.

## Current file

The current cumulative authority after the settled Office/My TMS branding decision is:

```text
CloudTMS_Candidate_App_Current_Decisions_20260819_R27_Branding.md
```

It contains all prior decisions plus AV-517 through AV-522. The My TMS artwork is stored for the later Candidate App rollout and is not currently rendered by Office CloudTMS.

## Packaging and verification

Future package validation must verify that:

1. the current Markdown decisions file is present;
2. it is included in the package hash and size manifests;
3. it contains the full cumulative authority rather than only the latest change;
4. implementation and compliance documents point to it as the controlling authority;
5. no required decision content was lost during conversion from the historical PDF-based process.
