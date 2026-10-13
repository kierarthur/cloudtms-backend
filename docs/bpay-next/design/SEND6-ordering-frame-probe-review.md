# SEND6 withdrawal caller-frame proof — 13 October 2026

Independent read-only source/design review after Root reported core round2 S004524: the native withdrawal-mark and audit xmins were both414519005 while pg_current_xact_id was414515689. Root reports D10 and D8 passed that round. Those are Root's reported PG17 execution facts, not a reviewer database run. This reviewer inspected actual saved fixture/native source and local reference documentation only; no SQL, test, network, product or database operation was performed.

**Frame-probe design and saved implementation: SOURCE PASS.** The broader updated lexical static proof has one OPEN preservation issue below. No runtime pass for the new0600 probe or clean overall rehearsal is claimed.

## Why the old comparison is wrong for this caller

The mandatory runner uses psql ON_ERROR_ROLLBACK. psql creates a SAVEPOINT before each client command in an active transaction; the entire ordering DO therefore executes inside that caller savepoint. Each writing subtransaction gets its own row-version xmin. pg_current_xact_id still returns the top-level transaction ID, so equal native own-write xmins later than the top-level ID can be correct under this harness.

The installed official PostgreSQL18 manual corroborates these stable PostgreSQL transaction distinctions: `doc/src/sgml/html/app-psql.html` documents implicit per-command SAVEPOINT behavior; `subxacts.html` documents writing subtransactions/parent-child XIDs and EXCEPTION blocks; `functions-info.html` explicitly says pg_current_xact_id returns the top-level ID inside a subtransaction; `ddl-system-columns.html` identifies xmin as the row version's inserting transaction ID. These are local PostgreSQL18 reference files, not a new PG17 experiment or browsed17 documentation. Root's actual PG17 values are consistent with those distinctions.

Do not disable mandatory ON_ERROR_ROLLBACK, equate all later xmins with the owner's frame, or replace the strong equality with >=. The comparator must identify the **actual current caller frame** independently.

## Saved minimal proof

Current0600 SHA256 `1DF11F96B330DBA29D2EE4C65798927249BC8512AB125474807995C99D7EA85C` adds only a text `v_owner_frame_xmin` variable, one new empty temporary probe table and one INSERT RETURNING xmin inside the original ordering DO immediately before the real withdrawal. The table is newly created without IF NOT EXISTS, the inserted row is explicit and its xmin is non-NULL; the INSERT names pg_temp. There is no trigger/default/native business writer on the probe and no intervening exception block or savepoint between the probe and call. ON COMMIT DROP plus the surrounding rollback fixture prevents durable state.

The exact required condition is now:

`withdrawal_mark_xmin = audit_xmin AND probe_xmin IS NOT NULL AND withdrawal_mark_xmin = probe_xmin`.

This remains stronger than “same later XID”: a newly introduced EXCEPTION/subtransaction around both own writes would give them the same child xmin, but different from the independent caller probe, and fail. Nesting only one own write fails equality between the two. The existing ordinary timesheet_unauthorise helper may retain its own legitimate exception/subtransaction for the three lifecycle rows; those are still checked separately with all original lower bounds.

The top-level XID before/after equality, all five original xmin >= top-level assertions, success/result checks, exact native row selectors, lexical owner prohibition, plain-English ORDERING RULE assertion, managed-root guard transitive census/broad detector, in-transaction observer/order conditions and forced-rollback coverage remain. No authorisation/audit/financial FK, owner permission, receipt, guard or native transaction behavior changes. Updated explanatory text accurately distinguishes outer ID from caller frame.

## Native owner facts

Final registry names `04102026_0556_banking_pay_next_source_factual_legacy_boundary_v1.sql` for public.weekly_source_first_authorisation_withdraw_v1. Actual source SHA256 `64EF7E6D04E1B652AF722CB9DD050310FCD8E1CD249F59B9359428A310B70933`; independently extracted LF body MD5 `11fe20659616e2a40887157973c84ff0`.

Actual marks/pointer update is before the unchanged ordinary unauthorise call (0556:2512–2550). Its late human audit uses the existing _audit_insert (2794–2811), and the ordinary helper's legitimate nested lifecycle writes remain retained. Independent source masking of this exact owner found zero executable EXCEPTION WHEN handlers, zero transaction/external-call tokens and zero executable EXECUTE tokens. The saved probe tests this owner's two writes against their real caller frame without rewriting this owner or its callees.

## OPEN S6-OF01 — preserve static external-call detection through quoted SQL

The same0600 diff adds v_executable_definition, masking comments and ordinary/E string literals before the existing no-transaction/no-external-call regex. Masking comments fixes false positives from ORDERING RULE prose and keeps a real EXCEPTION WHEN visible. However, masking **all quoted strings** also hides an executable dynamic external-call payload: a future `EXECUTE 'select dblink_exec(...)'` or quoted pg_background_launch would no longer be caught by the old external-token assertion. Normal own-frame mark/audit writes could still satisfy the probe while that extra external effect escaped. This is a concrete static-proof narrowing, not a claim that the current native owner performs such a call.

Smallest lawful closure: fail closed on executable dynamic EXECUTE in this exact owner assertion (the actual current owner has zero such tokens), while retaining every existing forbidden token and mask. Alternatively preserve quoted executable payloads in the external-call check with the existing comment-aware classifier. No new SQL lexer/framework, production owner rewrite, sourcegate exemption or rehearsal is needed to make this source correction. Root was notified; static proof remains OPEN until the saved bytes are reread.

Required next evidence: scoped final fixture diff/hash closing S6-OF01, then Root's separate SEND7-turn0600/0100 rollback receipt. Invoice-alone round3 is separate and supplies no execution proof for this new ordering DO. Historical SEND6 source and failed-round receipts remain preserved; promotion/install remains held.

## Literal-preservation follow-up and inherited regex correction

Re-read0600 checkpoint SHA256 `157AA752D7E002DB44202E1A123AA272E555380A4A1EBA264E293203768FE1F7`: the new NOT EXISTS regexp_matches assertion uses the same one-pass comment/string pattern as0900, selecting literal captures3/4 separately. Ordinary/E quoted SQL is checked with the complete original forbidden-token set, including indirect evaluator arguments. Comments are excluded. The existing executable mask, raw ORDERING RULE check, caller-frame probe and all xmin/observer assertions remain. This structurally closes S6-OF01's literal-masking gap without an EXECUTE allowance, new lexer/helper or native owner change.

**OPEN S6-OF02:** the inherited executable handler/transaction patterns and the new literal pattern all end in lowercase `\m`. PostgreSQL constraint escape `\m` means beginning-of-word, while `\M` means end-of-word. Each forbidden token ends in a word character, so it cannot immediately be followed by another beginning-of-word constraint. These three assertions therefore do not actually detect their intended tokens. The installed official functions-matching reference explicitly distinguishes the two; existing0900 installed-writer census itself uses `\m…\M`.

Smallest required correction: change only the three closing escapes to uppercase `\M`, keeping the starting `\m`, every token, flags, literal classifier and all native/source/frame assertions. This is a fixture static-proof repair; no production definition or economics change is necessary. Root was notified.

Correction to this review's earlier explanation: it claimed the old raw-definition regex would catch the quoted external payload. I overlooked its inherited closing-boundary defect. The new literal extraction is correct, but the original regex was already inert for that reason; the historic claim should not be treated as proof of effective old token detection. The caller-frame SOURCE PASS remains independent and intact. Final static SOURCE PASS remains open pending the corrected saved bytes, with no runtime claim.

## Final source closure — S6-OF01 and S6-OF02 CLOSED

**Full ordering/frame/literal static proof SOURCE PASS** at final0600 SHA256 `D0C8CEB99C7539C77C2463E6E46C2F7B26F7C59D1E1ADAC5423A7EBEE49F8A3E`. The exact saved static(i) block was reread: executable EXCEPTION WHEN, executable transaction/external tokens and quoted literal forbidden tokens now all start with `\m` and end with the correct uppercase `\M`. Only those three closing boundaries changed. The separate transitive-census schema/name starting boundary at2607 and its original ending `\M` remain unchanged.

The literal assertion still uses the0900 one-pass comment/string pattern and captures3/4 for ordinary/E strings. It checks the whole original exception/transaction/external token list inside quoted SQL, including an indirect evaluator's argument. Comments are ignored. The executable-token assertions and raw ORDERING RULE assertion remain. No generic EXECUTE allowance, new classifier/helper/framework, static-proof exemption or production owner change appears. Thus the quoted-payload gap S6-OF01 and the inherited invalid closing-boundary gap S6-OF02 are both closed in source.

Independent text inspection of the unchanged native0556 owner found334 ordinary/E literals and zero forbidden word-token occurrences in those literals. This is a source inspection with the documented word-boundary meaning, not execution of PostgreSQL regex/assertions. Its executable body likewise has no EXCEPTION handler, transaction control or named external call as previously recorded.

The probe remains a fresh temporary table/one-row INSERT RETURNING xmin inside the same original DO immediately before the unchanged real withdrawal call. Final source still requires non-NULL probe and exact authorisation=audit=probe, unchanged top-level XID before/after, all five >= bounds and every native observer/guard/forced-rollback condition. The correct comparator accommodates the mandatory caller SAVEPOINT while continuing to reject nested frames around the owner's own writes. The legitimate ordinary unauthorise helper's child lifecycle rows remain covered without rewriting its exception handling.

All historical findings and the earlier regex-review correction remain above. Source closure does not certify the pending0600/0100 follow-up execution, invoice round3, whole candidate or installation readiness. No reviewer SQL, database, test or network call was made.
