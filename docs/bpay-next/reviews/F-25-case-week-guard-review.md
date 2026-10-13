# F-25 retained CASE/WEEK insert guard source review

9 October 2026. Independent saved-source review only. No SQL, runtime, tests, fixtures, Docker, hosted reads or product edits were executed. Full F-25 remains incomplete; this is not an installation or application verification claim.

Reviewed `supabase/repeatable/09102026_2256_banking_pay_next_accepted_case_week_guards_v1.sql`, raw SHA-256 `5226a8c39e53f7900a1dfd6a466b8991b51cdff2c8095c52c300825d759dfd6f`, against locked 2153/2225 schema, current 2247 capture and 2254 enqueue tail.

**PASS for this insert-guard subset. No concrete open defect found.**

- Lines 18–25 bind the exact accepted parent/Candidate, current NEXT epoch, original run-command and enrolled member to the actual PREPARE agency sequence. They require ENROLLING with zero enrolled members, matching the enqueue tail's child-insertion window before later enrollment.
- Lines 26–40 require the retained sealed-case scope, exact CASE selection identity or WEEK key, both WEEK origin sequences strictly below acceptance, and the UTF-8 SHA-256 of the retained complete economic-origin JSON. Positive sequences and non-null typed identities remain enforced by 2153/2225 constraints.
- Lines 44–49 require byte-equivalent JSONB contents from the exact same-transaction staged record, parent, original command, run, kind and entity. The owner does not refresh current settings, Source facts, economic outputs or financial counters after allocation.
- The two BEFORE INSERT triggers retain the schema's existing UPDATE/DELETE/TRUNCATE immutability protections. The routine has a fixed private search path, logical owner and revoked direct grants. It adds no new row lock or financial mutation after the agency clock.

The guard proves consumption of the typed pre-clock capture; it does not independently prove that the still-pending weekly producer has correctly reconstructed every genuine economic/eligibility origin. That producer, complete weekly census, pre-clock FK locks and retained-input consumers remain separate F-25 dependencies.
