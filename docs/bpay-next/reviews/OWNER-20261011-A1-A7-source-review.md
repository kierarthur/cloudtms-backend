# Owner 11 October A1/A7 — independent source review

Date: 11 October 2026. Current verdict: **PASS for the exact A1/A7 source boundary. A1-G01 is closed; no essential source finding remains open.**

Reviewed the exact scope in design/OWNER-20261011-A1-A7-scope.md and the prior independent investigation, then the saved migration1048, current Worker/Office finite decoders, gate insertion, native accepted-choice0426 and reused A7 source0904/0910. No product change, DB call, compile, test, fixture, hosted operation or business interaction was executed by this reviewer.

## Results

| Owned change | Result | Evidence |
| --- | --- | --- |
| Exact held-choice CHECK | PASS | Migration11102026_1048_banking_pay_next_held_choice_skip_check_v1.sql drops and recreates only private.bpay_next_run_choice_member.bpay_next_run_choice_member_skip_code_check inside one transaction. Both branches add ON_HOLD and retain all other original literals; only an actual stored PAY_ON_HOLD row selects the branch retaining that historical spelling. No row is rewritten. NULL skip_code remains allowed by SQL CHECK semantics and the separate CASE_COMPONENT shape is untouched. ALTER TABLE holds the native table lock, so the stored-row branch and replacement constraint are atomic against concurrent writes. |
| Current writers and decoders | PASS | Unchanged0426 append_work_skip and create_preparation emit ON_HOLD. Worker and Office each remove only PAY_ON_HOLD from skipCode. Remaining finite validation, bounded skip arrays and UUID/uniqueness validation are unchanged. Historical installed definitions and acknowledgements are not rewritten or falsely described as current emitters. Decoder-only PAYE_EXCLUDED_BY_SAME_WEEK_GUARD does not enlarge this CHECK. |
| Source-gate current-owner scan | PASS | The existing registry's final create/append bodies are selected by exact signature using the existing statement reader, with CRLF normalised only for statement extraction. The current gate requires exactly two conditional CHECK sets and rejects each current literal assignment/direct return if absent from either set. PAY_ON_HOLD remains explicitly forbidden as a current output. The scan uses only the named1048 migration and final registered owners, with no general CHECK or historical-family waiver. |
| A7 existing fence/table | PASS | Existing0904 migration retains its Candidate/week PK, real command/job/worker/state FKs, owner-only private access and unique partial active-Candidate index. The existing0904 assertion validates exact native identities and accepts only the original live NET lease/owner/module tuple. It refuses other active owners with 55P03 and does not calculate or change money. No new fence/producer/framework is added here. |
| A7 current expiry hook | PASS | Current final0910 expiry source performs the exact assertion once at line52, after terminal replay, command/scope, ordered lease and checkpoint admission, before its first mutation at line70. Work-before-Case release and original continuation/count/completion remain. Existing runtime mapping treats 55P03 as retryable TRANSACTION_RETRY. Reuse matches A7; full DR37, D12/D17 and F26 remain separate. |

**A1-G01 — CLOSED: conditional CHECK branch coverage.** The first gate version unioned literals from both CHECK lists, so a current literal present in only one branch could pass. The saved correction parses two separate checkSets, requires checkSets.length===2 for current outputs and rejects checkSets.some(allowed=>!allowed.has(match[1])). Thus the current emitted literal must appear in both installation branches. The explicit PAY_ON_HOLD current-output refusal is retained. Migration1048 is unchanged and already contains ON_HOLD in both branches. This closure is read-only source evidence, not a claimed executed negative-control or business-row test.

The current final-definer registry independently confirms append_work_skip and create_preparation at0426, assert_candidate_financial_fence at0904, and expire_preparation_worker_page at0910, with matching finalRoot/finalPhysical values. Migration-lock adds only the exact1048 path/SHA; no immutable predecessor hash or body is changed by this boundary.

## Current provenance

Backend HEAD: 3867273d663cb46a564ea5c01d5208416d799abb. Frontend HEAD: f0723f0279af533b5fbbd3cd94d77cf5328bf451. The A1 edits are saved working-tree source.

| Saved source | SHA256 |
| --- | --- |
| migration1048 held_choice_skip_check_v1 | d4fe1898d84e17cdcc101896dc0c80580c64816e4840e525669c58b60cd000ce |
| Worker bpay-next-preparation.js | 5839856b51ccfae4628dbafeb8490bb68197a9d30555465898426653d179c4e9 |
| Office bpay-next-office.js | 0dcf5fc982195684fc2001985bb258a6c9397b89b7c34f0888612860c0f77c15 |
| source-gates.mjs | e1115032c1d75f8f0a266c733da61a9bce110611328655075db5cfa46856cd7f |
| accepted-choice0426 | aec4c904cee22e748ea1ab3006e8cfdbf50bcdb9dd567b1fb1b2bc9da7391b92 |
| D14 migration0904 | fbcd4a69aac8e14389c371313e25e2e8839e152603f7e30522a0a129740f145b |
| D18 assertion0904 | fa3c73a83075f67246e62adea7ba1467c019bada601d3db880ec51b48cdd1a9e |
| current expiry0910 | 14803cd993ef6bb7e3c11835e73ad2219520ce5f1dcf55b0dabdb74088d9c654 |

The scope note reports root's fresh local PostgreSQL17 constraint identity, zero old-literal rows and installed A7 metadata. These are attributed root evidence, not independently executed or certified by this source reviewer. Hosted installation, contention/runtime behaviour and whole-item application acceptance remain NOT_RUN here.
