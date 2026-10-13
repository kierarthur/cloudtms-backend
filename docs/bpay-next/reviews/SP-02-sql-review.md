# SP-02 SQL and final-definer review

Independent saved-source review, 10 October 2026. Root alone writes product. Scope: full SP-02 card `05_SOURCE_AND_PROTECTED_PAY.md:128–153`, its six checklist rows, BD041 entry design, the four definitions in `supabase/repeatable/10102026_0715_banking_pay_next_source_retired_finance_entry_v1.sql`, the retained 0100/0555 authorities, and final-definer selection. No SQL, runtime, tests, fixtures, hosted reads or business operations were invoked. Joined acceptance remains NOT_RUN.

## Verdict

Final saved-source verdict: **PASS**. SP02-SQL-01 is closed by the root correction independently reread below. No concrete source finding remains open in this four-function/registry subset. The informational notice does not authorise payment, and the standalone admission RPC does not serialise a later external request.

## SP02-SQL-01 — closed: validate and hold the diagnostic owner decision

The three NEXT branches at 0715 lines 44, 104 and 187 directly read `active_owner` without the existing module validator or its transaction-held SHARE lock. A missing id=1 row therefore falls through into the legacy detector/absence/notice, although the design specifies an actual validated owner/epoch. More concretely, the existing owner-only `weekly_source_banking_pay_absence_declare_v1(text,text)` calls the arrival detector before its declaration INSERT/UPDATE. It can observe LEGACY, then a concurrent NEXT owner update can commit, then the declaration write can occur and its final diagnostic return `declared=false/BANKING_PAY_NEXT_ACTIVE`. That is a new stored declaration after arrival, rather than merely retention of a declaration validly admitted under LEGACY.

Use the existing `private.bpay_next_legacy_callback_enabled_v1()` before each diagnostic NEXT branch, then perform the exact owner read while the helper's SHARE lock remains held. The helper already rejects missing/invalid owner or nonpositive epoch and permits distinguishing NEXT from DISABLED without a new routine or owner protocol. Keep the original legacy bodies and the selected effective-absence design. This gives the existing declaration precheck one stable owner decision through its write. It cannot prove or extend admission across separate HTTP transactions.

## Passed boundaries

- `public.bpay_next_source_legacy_finance_admit_v1()` has zero arguments and returns only true for LEGACY. It validates the service JWT-role claim before module access; malformed fallback JSON and non-service roles raise 42501. PUBLIC, anon, authenticated and service_role are explicitly revoked, followed by the sole service_role EXECUTE grant. The SECURITY DEFINER owner is the audited logical postgres owner; its search path is `pg_catalog,private`. No business request, actor override, permission token, command, job or financial receipt is introduced.
- Admission invokes the genuine 0555 callback helper. That helper reads id=1 `FOR SHARE`, checks the finite owner enum and positive epoch, and raises `BPAY_NEXT_OWNER_STATUS_UNAVAILABLE` for missing/invalid authority. The subsequent STRICT read is protected by that same transaction-held lock: NEXT raises PT410 / `BPAY_NEXT_LEGACY_WRITER_RETIRED`; DISABLED raises PT503 / `BPAY_NEXT_LEGACY_WRITER_DISABLED`. No bool-false success or Worker-side owner decision is substituted. The existing module table also has NOT NULL enum/positive-epoch constraints.
- The three retained CREATE bodies match their original 0100 bodies exactly after removing only each added NEXT branch and restoring the notice initializer's former placement. The notice now branches before invoking absence(), so a stored legacy declaration cannot prevent the selected NEXT informational notice. Under NEXT, arrival is explicitly true and effective absence explicitly false; the original stored declaration is neither cleared nor relabelled. Original declare/clear and the two classifier probes are unchanged. DISABLED receives no NEXT wording. All three private functions preserve original language, volatility, definer owner and search path, and revoke every API role.
- The repeatable is transactional and repeatable through CREATE OR REPLACE, exact ACL reassertion and PostgREST schema reload notification. It adds no trigger, table, producer, historical rewrite, Source core or external C1 implementation. Existing 0550/0555/0556 files and current Rule B 0151 consumers are unchanged in the reviewed working diff.
- A signature-keyed comparison with HEAD finds exactly four registry additions, no existing chain content changes, and 733→737 chains. Three absence chains name original 0100 as their sole predecessor and new 0715 as finalRoot/finalPhysical/finalClosure. The sole new public admission chain has no predecessor and the same final 0715. The four Rule B finals and CM-02a reference-only metadata remain intact. Registry ordering noise is not a competing owner change.
- Shared 2336 finalisation/task owners, ordinary Source TSFIN/proposal receipts, Source C1 staging/read/local completion and approved NEXT publication are preserved. External C1 core definitions are absent from the saved source/canonical catalogue census; this change invents none. In-flight or unknown C1 obligations remain their genuine F-18/F-19 census/reconciliation concern. Database write fences remain the eventual backstop after a standalone admission transaction; its SHARE lock ends with that transaction.

## Raw saved-source SHA-256

| File | SHA-256 |
|---|---|
| `supabase/repeatable/10102026_0715_banking_pay_next_source_retired_finance_entry_v1.sql` | `2471ab61b3320e94e87d634d250ee93b765b73a3296d1b4f8ca4db82795b0c0c` |
| `supabase/repeatable/17092026_0100_weekly_source_banking_pay_absence_v1.sql` | `191cfbf4427c4dc4a35d50fcd679e6e09c06189b3f8561473f4bdc2f0c75c6a8` |
| `supabase/repeatable/04102026_0555_banking_pay_next_factual_legacy_projection_boundary_v1.sql` | `eba6b3596434aa6eecf133594579fb7833a7e3acdbe7d6fbd3676fbfb913db86` |
| `docs/bpay-next/final-definer-closures.json` | `7db4fa93592d5ed510b1ca12c1d1ee10f7613f60f528793072b951e048c7b693` |
| `docs/bpay-next/design/SP-02-entry-design.md` | `fa8bf35d4e579481f629ffb5f3147d973d7148bf253579f9aa88d3b47d6c52c2` |

The first full source read had hash `67e511e8672cb8d5ae6e91b0022d4c15557861f19bc0e78a7c2a180ea6886e3b`. A fresh full reread captured the later schema-reload notification at the reviewed hash above; the four function bodies were unchanged by that addition. These are source hashes, not installation or protected verification evidence.

## Final correction rereview

Root added exactly one `perform private.bpay_next_legacy_callback_enabled_v1();` before each diagnostic owner read, now at lines 44, 105 and 189. A fresh full source reread confirms the real 0555 helper validates the module row and owner epoch and holds its SHARE lock through the surrounding transaction. Missing/invalid authority now fails before diagnostic fallback; an existing declare() precheck keeps the same owner decision through its declaration write and final diagnostic read. Concurrent owner transition cannot interleave its commit between that precheck and write. A subsequent transition may still leave a legitimately earlier legacy declaration stored, as BD041 explicitly permits.

Independent text comparison again reproduced all three original 0100 function bodies exactly after removing only the new helper calls, NEXT branches and the notice initializer move. The admission function, configurations/ACLs, repeatable tail and registry are unchanged. DISABLED still receives no NEXT informational claim. No new helper, trigger, protocol, producer or business owner was introduced. SP02-SQL-01 is closed; this SQL/registry subset **PASSES source review**. Joined/hosted/runtime acceptance remains NOT_RUN and separate-request/external quiet-window limitations remain unchanged.

Final 0715 raw SHA-256: `358abee947f0b03aeba9cbbbcadc17f6a7fe54bc56260951592b8a2279470044`.
Registry raw SHA-256 remains `7db4fa93592d5ed510b1ca12c1d1ee10f7613f60f528793072b951e048c7b693`.
