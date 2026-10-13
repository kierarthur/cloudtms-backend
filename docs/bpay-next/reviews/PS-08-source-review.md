# PS-08 frozen capture source review

10 October 2026. **PASS — source only; no open findings.** Reviewed the exact06 card at1480 onward, four checklist objects, unchanged0718 owner/1342 callback, current write-boundary gate and ordinary source controls, BD036 scope and joined acceptance. Fresh re-read closes H01/H02 below. No SQL, tests, fixtures, runtime, app or catalog operation was executed by this reviewer.

## Findings

H01 (closed in fresh source): the first census required `private.` and missed unqualified UPDATE/DELETE in private-search-path routines. The fresh exact-target pattern admits the optional private schema and the controls include qualified/unqualified, quoted and ONLY forms. It still excludes similarly prefixed mutable relations.

H02 (closed in fresh source): the first direct EXECUTE scanner could interpret text inside an inert diagnostic literal as executable SQL. The reported positive case was:

```sql
do $body$begin
  raise exception using message=$msg$execute 'update private.bpay_next_run_work set id=null'$msg$;
end$body$;
```

The final implementation collects literal spans with its existing literal scanner and requires each discovered EXECUTE keyword to be outside those spans. Fresh positive controls include this exact dollar diagnostic and a SELECT string containing escaped EXECUTE-looking text; direct single/dollar/format executable UPDATE/DELETE controls remain negative. This finite correction closes the reported false positive without a general interpreter or product SQL rewrite. Computed/unclassified SQL remains subject to existing D09 rules.

## Correct retained database owner and scope

The sole0718 owner has exactly four attachments: BEFORE UPDATE OR DELETE per row (type27), and BEFORE TRUNCATE per statement (type34), on each of run_work/run_line. Its preflight refuses same-name conflicting callback/type/enabled/args/WHEN/columns/constraint/defer/internal metadata. The callback1342 unconditionally raises23514 `BPAY_NEXT_FINANCIAL_EFFECT_IMMUTABLE` for non-INSERT operations. No depth, whitelist, exception or INSERT attachment is introduced. No SQL product source changed in this batch.

Legal preparation remains complete INSERT-only capture. Existing progress belongs to worker, hold, request, job and outcome rows; the census permits INSERT, SELECT FOR UPDATE, trigger DDL and other mutable relations. CM-03's later RBC reconciliation must retain these protections. The existing cancellation lookup/pending indexes are preserved, not authored again under PS-08.

## Census integration and future acceptance

The census is placed in the unconditional `assertSqlWriteBoundaries` path. The source Set comes from all inventory migration/repeatable physical paths, including recursive includes, not only dirty files or current final definitions. Existing changed-file D09 special-construct rules remain separate. Current direct-body controls cover both targets, CTE DELETE, ONLY/parentheses and routine bodies; positive controls preserve comments, diagnostic text, legal INSERT/locking/DDL and similarly prefixed tables. The extra direct executed literal/format support remains finite; the final literal-span filter closes H02.

D03 explicitly keeps actual joined acceptance **NOT_RUN**: legal preparation/confirm/cancel/expiry/outcome and direct UPDATE/DELETE/TRUNCATE rejection remain future authorised checks. D04 is a release Bible/F24 ownership reference, with exact four-attachment readback and no hosted installation claim from local source presence. This review is not a database or connected acceptance receipt.

## Reviewed SHA256

| Source | SHA256 |
| --- | --- |
| `scripts/bpay-next-source-gates.mjs` | `501d284f08fd0bd6f74d88f4162405ce1017818e7d3333123503b3e1250fac13` |
| `tests/bpay-next-source-gates.test.js` | `69ec39405dd6f0cf3bce16f1ecf1952188a09317b10943b38e17d5bfa554fe03` |
| `supabase/repeatable/08102026_0718_banking_pay_next_frozen_run_read_safety_v1.sql` | `0a884c8cb2568ca568ac51069fcf8fac945159a7f3f4d688d57b1731c295feae` |
| `supabase/repeatable/03102026_1342_banking_pay_next_guards_v1.sql` | `d29decd59c88c2620fe658b5d96f0deace19063cd0eaddf150ea7cf09c7b60af` |
| `docs/bpay-next/design/PS-08-SCOPE.md` | `26600c33eb9faa5358c2ab82a3164653dcac545cea30910967d7fb0c6a50538c` |
| `docs/bpay-next/PS-08-JOINED-ACCEPTANCE.md` | `f9c8098b5bebd07bd702efa19863d026ec2293d8444d85b8fdfbe831bce53d11` |
