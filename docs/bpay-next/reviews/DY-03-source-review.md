# DY-03 independent source review

11 October 2026. **SOURCE PASS for the narrow sequence-89 schema and immutable-identity prerequisite.** No essential defect was found in the reviewed bytes. Producer/routing, genuine physical DAILY admission and complete evidence qualification remain DY-04; joined application acceptance is NOT_RUN. This reviewer read sources and diffs only: no SQL, database, tests, fixtures, browser/hosted calls, product edits, commits or checklist writes.

## Controlling scope and exact comparison

Read the complete DY-03 card `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:929–952`, its DY-04 continuation at 954 onward, all four local checklist rows `IMPLEMENTATION_CHECKLIST.md:2652–2655` and future clauses at 2661–2664, F-06 prerequisite, `design/DY-03-current-investigation.md`, current registry and exact current definitions.

The new scaffolded migration `supabase/migrations/11102026_0928_banking_pay_next_daily_work_origin_identity.sql:5–20` is line-for-line equal to the approved schema prerequisite's spans 5–20 at ordinary backend `codex_outputs/claude-methodology-review-20261007-r1/snapshot/evidence/CONTRACTLESS_DAILY_SCHEMA_V1_20261006.sql`. Its only other statements are transaction/error-stop packaging and comments; no business write is included.

The complete new `private.bpay_next_work_pointer_guard_v1()` definition in `supabase/repeatable/11102026_0928_banking_pay_next_daily_work_identity_guard_v1.sql:4–49` equals the complete current 1342 parent after exactly two field-list extensions: append `origin_kind,origin_work_date` to each new/old immutable identity tuple. This was an in-memory text comparison, not a test invocation. Every other branch is unchanged.

## Schema and identity safety

Migration 0928:5–14 adds only Work `origin_kind text NOT NULL DEFAULT 'CONTRACT'`, nullable `origin_work_date date`, drops only Contract NOT NULL and adds the closed two-branch identity CHECK. CONTRACT requires a non-NULL Contract and NULL date. DAILY requires NULL Contract, existing ORDINARY work_kind and non-NULL date. The non-NULL origin/work_kind fields and explicit IS NULL/IS NOT NULL operands prevent UNKNOWN from admitting an invalid shape; unknown origin labels and NULL-Contract CONTRACT/SOURCE shapes fail the CHECK.

Existing rows receive CONTRACT identity, consistent with the original core Contract NOT NULL. The restrictive Contract FK, Candidate/original physical Timesheet FKs and unique original Timesheet/booking keys in `03102026_1324_banking_pay_next_core.sql:111–129` remain unchanged. No fictitious Contract, duplicate Client, new work_kind, ledger, identity table or index is added. Actual physical DAILY/HOURS qualification is not something a table CHECK can inspect: the retained original insert guard at `03102026_1342_banking_pay_next_guards_v1.sql:171–186` checks booking only, while current route/publisher still refuses the NULL-Contract path. DY-04 owns the positive physical/evidence qualification before enabling that path.

Migration 0928:15–20 adds nullable revision `daily_approval_evidence_json` and permits only SQL NULL or a JSON object; JSON null, arrays and scalar JSON fail. Historical revisions remain NULL. Existing revision physical UUID/version/financial and source fields remain intact (core1324:132–176).

New pointer guard 0928:16–21 protects origin kind and worked date together with the original Candidate, Contract, original physical Timesheet, booking, work_kind and week. Tuple IS DISTINCT FROM remains NULL-safe. Current/applied revision seal checks, revision number agreement, monotone newer revision and withdrawal refusal at 23–45 are copied unchanged. Existing UPDATE/DELETE trigger attachment `1342:247–250` invokes this same OID; no replacement trigger or effect-guard bypass is introduced.

The unchanged whole-row revision guard already covers the new evidence field: `1342:28–38` refuses deletion/sealed updates and allows an unsealed UPDATE only for approval/seal timestamps via `to_jsonb(new)` versus `to_jsonb(old)`. Its native financial physical UUID/version check at 46–53 remains. Therefore no revision-guard successor solely for field immutability is necessary.

The date is justified by the supplied joint publisher's stable identity insertion/reuse: `DAILY_AVAILABLE_RATE_CAPTURE_V1_20261006/contractless-late.sql:469–480` records and checks origin/date with the booking, while revision identity retains the changed physical version. These spans support the card's T-23/SRQ-C24 exception. They are evidence for DY-04's later publisher, not installed by this prerequisite.

## Existing owners and finite source gate

The new guard preserves language PL/pgSQL and fixed `pg_catalog, private` search path (0928:6–7), owner postgres and owner-only revoke from PUBLIC/anon/authenticated/service_role (50–51). No privilege grant is added. The registry advances only this guard from 1342 to 0928, preserving historical 1342 and DY-04 as next guard composer. Existing `bpay_next_revision_guard_v1()` remains 1342; Daily helper remains the corrected `11102026_0925_banking_pay_next_daily_five_standard_rate_rows_v1.sql`, reused without overwrite.

The essential `scripts/bpay-next-source-gates.mjs` adaptation is finite. Constant line 12 names the one 0928 file; 1945–1957 matches only the exact pointer signature/file, extracts one complete frozen-parent/current statement, requires each original tuple anchor once, applies only its two extensions and compares the entire expected statement. Other definitions still use the original frozen-definer rejection. Frozen 1342 bytes remain checked against FROZEN_BASE at 1929–1933. Lines 1891–1895 permit only the exact owner-postgres ALTER and exact owner-only REVOKE statements for this file/signature; grants, other owners/ACLs and other protected identities remain under the existing authority checks. No broad filename, function-family or body suppression was added. The final complete allowance was re-read after root added the owner-postgres clause; SOURCE PASS remains.

## Receipt boundary and reviewed hashes

D01's storage/immutability prerequisite and D02's existing corrected-helper reuse pass source review. D03's historical core receipt is not reinstalled or edited. D04's joined physical rotation, one-Work replay, genuine NULL-Contract admission, weekly/SOURCE refusal and frozen old history remain NOT_RUN and depend on DY-04's producer/guard closure. No current Contractless publication or runtime acceptance is asserted here.

Raw SHA-256 at review:

| Source | SHA-256 |
| --- | --- |
| 0928 migration | d1bcf9d1c3fb0c7d3abd396727982eae0b9db5e8f3fb634c81ba33cf47ebdc85 |
| 0928 guard successor | 2a7a8cb0d64c8e56404b1889b8e5f5bbb6bae7ac49700342fa3e81b315a8a7f3 |
| unchanged 1342 parent | d29decd59c88c2620fe658b5d96f0deace19063cd0eaddf150ea7cf09c7b60af |
| unchanged core1324 | f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c |
| reused 0925 helper | 39c1a1f69fc5c9a5152aa08ca037e295e6e08b64bbd0651177556fa6fe52c99a |
| current registry | 8ccdc74a87def9a8b66501cd47cd077059004cffa3b509612324a03607bd96a5 |
| source gate, final exact owner/revoke allowance | 4d7cba89c1f8608078c09c51e5f7cff4067e9a8e39fff7af4c0c3f4907c96544 |
| source investigation | 44f512c638e1f49ff43f9b419ea96399455897597d1f062bf8f39409eaf96c93 |

Compile/catalogue/source-gate outcomes are root-owned evidence, not executions by this reviewer.

