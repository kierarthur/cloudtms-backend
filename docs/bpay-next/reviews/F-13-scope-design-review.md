# F-13 scope/design independent source review

10 October 2026. Verdict: **PASS for the proposed bounded design; no product implementation or installation verdict.** No open concrete design defect. Read the complete controlling F-13 card (foundation 748–767), checklist D01–D03, F-13-SCOPE.md, prior current-scope investigation, shared consumer and actual legacy producers/parsers, existing certificate status owner and audit schema/ACL. No product edits, SQL/DB, runtime, tests, fixtures, external calls or secret reads.

The proposed index seam and one supporting module match the three named objects. Fresh database owner precedes message admission. Existing NEXT adapters remain authoritative. LEGACY/DISABLED and unknown/nonterminal deliveries retain retry; no legacy execution/recovery/drain compatibility is restored. D03 can document the existing max-retry/DLQ behavior and F-11 durable rediscovery without infrastructure changes. F-18/F-19 reconciliation and quiet-window evidence remain separate release obligations.

## Positive certificate qualification is an existing owner

`02092026_2301_banking_pay_workbench_settled_certificate_build_v8.sql:2519–2640` defines the real service-only, STABLE read-only status RPC. It selects the supplied session owned by the supplied actor and a certificate with the exact current session version, progress-counter version and authority-fence generation. The current-ready predicate requires OPEN/READY, no discard/replacement and a finite positive selected count (2594–2601). Ready-for-draft requires SEALED_CURRENT, the real V2 contract and retained certification/digest shape (2606–2610). The response includes exact session/certificate identity and pending/polling/recovery flags (2616–2637). Requiring every stated positive boolean and the envelope's exact native session/certificate identity therefore certifies completion of this metadata build wake. It does not certify payment or generic Workbench/global scope closure.

No operation COMPLETE flag, absent lookup, absent F-18 item, zero summary or owner flag is sufficient. The scope correctly retains operation and Workbench families unless actual existing evidence proves their entire native obligation closed. This resolves the handover question conservatively: no envelope is ACKed merely because the census did not enumerate it.

## Existing audit transport fits without a new proof protocol

The structural baseline defines `audit_events.id uuid NOT NULL`, typed audit fields and required object/action (617–632); finishing supplies UUID/time defaults and the UUID primary key. Existing ACL grants service SELECT/INSERT, and the saved current contract has the existing service/owner ALL RLS policy. Migration 21092026_1817 adds database-owned event sequence/authoritative defaults. A fixed INSERT followed by fresh exact GET is compatible with these saved source contracts. The existing best-effort `writeAudit` is unsuitable and is correctly excluded.

BD022's direct representation of an exactly supported native queue ID as the UUID PK needs no envelope digest, extra command, schema or new witness. Store and compare the **complete original queue ID** in the closed bounded packet as proposed. Any unsupported format, case/representation collision, unrelated existing UUID or mismatching packet must retain the message. The documented system-generated string contract does not prove every production ID has the admitted 128-bit hex format; fail-closed retention is the correct outcome for unsupported formats. Do not coerce, truncate or hash them to manufacture admission.

Keep the product's receipt path insert/read only and revalidate the actual current certificate qualification even when the row already exists. This audit table has broader existing UPDATE/DELETE permissions and is not globally append-only; an exact fresh row check can support this flow, but documentation must not claim a database immutability guarantee. The once-only claim is the unique matching durable ID/reason event, not exactly-once console delivery. Audit failure, timeout, lost reply, conflict without exact reread or malformed response must not ACK.

One bounded deadline, capped responses and redirect refusal cover qualification/INSERT/GET. The consumer should catch only into closed bounded retention codes and keep payloads, lease text, error bodies, contact data and credentials out of logs/receipts. Native IDs and closed disposition codes suffice.

## Implementation review boundaries

The later source review must check strict raw complete-key classification against the actual ten/eight/thirteen-field producers, without their coercion/defaulting parsers; exact supported queue-ID conversion and full-ID collision comparison; exact status arguments/reply identities; awaited fresh receipt GET before the sole ACK; and all unknown/nonterminal/missing-evidence paths explicitly retrying. A design PASS does not tick Built, Installed or App tested and does not qualify any additional operation/scope family by analogy.

`F-13-current-scope.md`'s prior digest-derived UUID proposal is superseded by the checked F-13-SCOPE.md/BD022 decision. It is historical investigation, not an additional product requirement. Parked F-25 protocol and the historical external-ticket/barrier proposal remain outside scope.

## Fresh saved-source hashes

| Source | SHA-256 |
| --- | --- |
| `docs/bpay-next/design/F-13-SCOPE.md` | `5429873bda33f3fe9a0e21f0f8a2011c0380f1062bcc301e5794eae3d7292d7b` |
| `docs/bpay-next/design/F-13-current-scope.md` | `f9f05668f25350b03de6c8540964d4082fc07b3390febec1017661d83d6605e3` |
| `broker/src/index.js` | `29cfa90d56aa10b6d86c50c687445a4649c876d3799d64a7287b8a6162d623ec` |
| `02092026_2301_banking_pay_workbench_settled_certificate_build_v8.sql` | `081e490c359080ba296f06548a845100ee242cf5fafb10dd50d2aaa77fc079cf` |
