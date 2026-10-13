# PS-01 independent source/scope review

**PASS for the current card's source delivery and authored D06 checklist.** Read-only static inspection on 10 October 2026; no product edits, SQL/runtime/test execution, fixtures or hosted operations. No installed, app, financial or queued-path acceptance is claimed.

Binding card: normal R3 `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1327–1349`; exact build checklist PS-01-D01/D02/D05/D06 and shared-context D03/D04. Master 08 line63: historical0130 chain **PS-01 → DY-09 → DR-38 → DR-22 → CM-13a**. The accepted ledger/schema work is independent of AUTH-PS; no Worker/Screen delivery belongs to this card.

| Row / mandatory substance | Source review |
|---|---|
| D01: exact predecessor assertion, stored and schema-enforced projection/transfer bridge | PASS: new0533 migration alters only the two required private tables. It compares named constraint on the exact projection relation to the complete0300 predecessor definition, adds dedicated nonnegative finite two-decimal `payment_case_target_inc_vat` and `deduction_case_target_inc_vat`, and replaces only UMBRELLA cash with inc+payment−deduction and NET NULL. Original1324 ex+VAT=inc,0300 nullability and F06 strict WORK constraint remain. PAYE_MANUAL/PAYE_IMPORT and CASE_PAYOUT cash clauses have identical fields/operators/constants to0300. |
| D02: current simple-transfer successor, exact claim reuse, member-guard confirmation | PASS: build successor is composed from actual final2018. Static complete-function diff has only one new terminal UMB block after the existing cash reconciliation. Claim stays in0130, unchanged. Existing0145 member guard remains channel-neutral and unchanged. The supporting transfer guard successor is only the exact captured-bridge freeze below. |
| D05: preserve positively evidenced baseline | PASS: new migration supersedes the named0300 constraint; it does not rewrite/reinstall historical0300/1324. Exact baseline hashes are recorded below. Current2018 and historical0130/0145 remain unchanged. |
| D06: future joined acceptance checklist | PASS, authored and reviewed: `PS-01-JOINED-ACCEPTANCE.md` covers ordinary100/20/120/0/120; schema cash and partial/foreign bridge refusals; two Candidate legs; preserved PAYE/CASE_PAYOUT and payroll NET distinction; captured bridge immutability; member sum and atomic terminal completion; ordinary Case/conversion/nonzero target-term refusals; F06 preservation; and CM-13a/14 later120−36→84 example. All real app/Worker/queue/minute/installed/business clauses explicitly NOT_RUN; no fixture/hash proof programme or completed PS UI is claimed. |

## Enforced bridge and current owner behavior

Migration0533 lines37–69 require an entirely NULL bridge or a complete nonnegative UMBRELLA tuple on an original projection-backed Umbrella beneficiary transfer. The complete tuple CHECK enforces ex+VAT=inc and inc+payment−deduction=cash. Required-bridge CHECK covers financially assembled original Umbrella transfers, with BUILDING/CANCELLED exemptions so an unassembled cancellation cannot be forced to invent earnings. Exact nine-column FK includes projection ID, worker ID, actual projection `input_kind`, all five captured amounts and cash. The explicit discriminator closes the type ambiguity; original worker/Candidate FKs and unchanged builder scope checks preserve one leg per Candidate. Reissues retain the original cash lineage and carry no fabricated earning bridge.

The CHECK's all-NULL/complete arms use explicit IS NULL/IS NOT NULL tests; a partial tuple cannot slip through a NULL CHECK result. A complete discriminator cannot point to PAYE/CASE_PAYOUT or another worker's projection. Existing0390 projection whole-row immutable guard automatically includes both new dedicated fields. Exact FK therefore fixes every captured bridge value to the same immutable projection. Current0533 transfer guard additionally refuses any change/clear after a bridge has first been captured, including a later cancellation; its full static diff against0145 is only that ten-line block after the original delete refusal. All original status/execution-kind/bank/internal receipt/terminal money checks, definer search_path and private revocation behavior remain exact. Historical0145 is preserved and its existing trigger attachment now calls the registered successor; no new trigger binding is introduced.

Builder0533 lines175–188 refuse either nonzero new Case term, including a balanced pair, and copy all five amounts plus actual UMBRELLA kind from the already scoped immutable projection. Capture occurs only after existing complete member count/no-omission/frozen-gross/final-cash reconciliation, immediately before unchanged MEMBERS_READY/job DONE/command COMPLETE updates in the same transaction. Nonterminal pages retain all-NULL bridge. A later terminal failure aborts the entire page, including the separate bridge UPDATE. Static complete-function comparison to2018 confirms that ordinary Case/destination dispatch, all existing module/epoch/lease/cursor/replay conditions, Rule-B-derived frozen input usage, PAYE adjustment and output shapes are unchanged.

Member-sum evidence is precise:0145 permits INSERT only while the actual parent is BUILDING and prevents later edits; it covers UMBRELLA without channel exclusions. It does not itself calculate SUM. Actual frozen member accumulation and exact count/gross/cash reconciliation are performed by the unchanged builder, while1324 CHECK requires stored positive member_count/member_cash_sum=cash at MEMBERS_READY. This is the combined current owner/schema/member-freeze proof. No new general proof framework or unrelated money owner is introduced.

The F06 ordinary WORK-only constraint remains installed in source. Current simple builder refuses unsupported Case/conversion and nonzero dedicated Case terms. Existing0410 Case owner is not rewritten or claimed as ordinary Umbrella Case delivery; CM-13a owns that actual producer and explicit F06 replacement, and CM-14 owns moved origin. PAYE and CASE_PAYOUT rules are preserved. The projection's new Case columns identify TARGET INC explicitly and do not reinterpret ex-VAT accepted_gross additions/deductions.

## Registry, scope and release limits

Final registry correctly lists0533 as current physical/root winner for `private.bpay_next_build_simple_transfer_page_v1(uuid,uuid,bigint,bigint,integer)` and the minimal `private.bpay_next_transfer_guard_v1()` support successor. Its predecessors retain2018/0130 for build and0145 for guard. Claim remains0130; begin remains2018; projection guard remains0390; Case owner0410 remains separate. Next0130 editor is DY-09 as the master chain requires. No Worker/Office product change or calculator is needed by this card.

No-backfill schema requires any pre-existing ready original Umbrella rows to already have complete retained bridge facts before installation, otherwise migration validation fails closed. Hosted contents and actual `pg_get_constraintdef` output were not inspected here. Exact catalog-expression match and schema validation are therefore later local/release verification obligations; the source includes the correct explicit predecessor assertion and makes no compatibility backfill claim. The CANCELLED exemption does not extend original guard transition permissions or manufacture a completed member/bridge receipt. All financial acceptance and actual TEST installation remain NOT_RUN.

## Reviewed raw hashes

| Source (build repository relative path) | SHA-256 |
|---|---|
| supabase/migrations/10102026_0533_banking_pay_next_ps_c01_umbrella_cash_bridge_v1.sql | 23d0375090ef26692d19051eed62b3033b2218b6cb4622430ce2c6421a822964 |
| supabase/repeatable/10102026_0533_banking_pay_next_ps_c01_simple_transfer_bridge_v1.sql | a7175c21cac86b372ad59c3a114dce69c9298708154898955848877b020fb314 |
| docs/bpay-next/PS-01-JOINED-ACCEPTANCE.md | a4ad570d596f5bceccf6376130369fb21b9c99366dc473fe2a054c79af44f2cf |
| docs/bpay-next/final-definer-closures.json | 83d8a27dc37d7193feaf651ca026b612344995d508281bbd839ec14b17d1d3cc |
| supabase/migrations/03102026_1324_banking_pay_next_core.sql | f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c |
| supabase/migrations/04102026_0300_banking_pay_next_case_instruction_core.sql | 04b44425d288e714037012c3b4eb4d27679137905e1c380682812cc8273ad40e |
| supabase/migrations/09102026_2011_banking_pay_next_initial_tsfin_umbrella_work_shape_v1.sql | 8dd6232ccda068022e9ab8faef163d89eeaa883720a0e95e441db56177a7aeef |
| supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql | 7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b |
| supabase/repeatable/04102026_0130_banking_pay_next_simple_transfer_members_v1.sql | 763176b54482954adf8c07e7415a86e005baae71848d03bba3245ee8c385907a |
| supabase/repeatable/04102026_0145_banking_pay_next_transfer_guards_v1.sql | 2006007829398c9d849ddad959ea7e665f07b37a49f08dddd3e56385c8623a2c |
| supabase/repeatable/04102026_0390_banking_pay_next_case_paye_net_v1.sql | 9622c5b9259f80e69a4fb661425c0016a4f3ee69ee8b02fcb3a7baa7ba389fbb |
| supabase/repeatable/04102026_0410_banking_pay_next_case_transfer_post_v1.sql | f56898dc7e992fc271d2a90c0c7fdb85d77dde4bac5e8b852e008c0da90793d8 |
