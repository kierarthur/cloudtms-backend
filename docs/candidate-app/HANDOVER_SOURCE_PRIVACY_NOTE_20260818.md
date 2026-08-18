# Source privacy note for the master continuation pack

The master continuation pack is an assurance/context archive, not the Google installation artifact.

The complete Candidate bridge helper files are packaged byte-for-byte because they contain no secret values. The large certified legacy `Code.gs` and rollback files are packaged as full structural review copies, but credential-like literal values and email-address literals are replaced with explicit handover placeholders. This prevents legacy credentials or personal contact data from being transferred in a general-purpose handover.

No functional statement, function name, control flow, bridge seam, Sheet ownership, trigger owner or integration call is removed by that redaction. The original saved source SHA-256 values are recorded in the pack provenance without exposing its sensitive literals.

For actual Google installation, use the certified source held in the controlled repository/workspace and re-export the effective live project immediately before editing. Never paste a review-redacted `Code.gs` file into Google.

The following remain exact and suitable for independent R16 source review:

- both `CloudTMSCandidateBridge.gs` helper files;
- Candidate Daily Worker/contract source;
- SQL migrations/repeatables;
- workflow definitions;
- tests and evidence;
- decisions, runbooks and API schema.

Secret values, Google properties, authentication state and personal contact values are not included anywhere in the archive.
