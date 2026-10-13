# XD-01 server source review

Independent read-only reviewer, 13 October 2026, BD179/seq147. Scope: the saved1452 index migration, four-argument sorted private reader,1455 current public preparation-page successor, preservation records, registry and the exact new source-gate hazards. No SQL/RPC/database/business execution, test, fixture, render, network, Git mutation or product/checklist/BUILD_LOG edit was performed. Root alone implements/runs checks. This is a source verdict, not installed-catalogue/performance/A4/release evidence.

**SQL SOURCE PASS at this saved checkpoint.** No concrete defect was found in the new index/reader/public dispatch source. Source-gate admission for the new named overload is still required and is specified below; this review does not claim the full gate or whole XD01 is complete. XD07 retains its oversized-row obligation. Detailed policy/access qualifications in `XD-01-server-sort-investigation.md` remain binding.

## Exact native preservation

The original three-argument2049, entire1646 native source and current2232 common args owner retain their previously reviewed hashes. The registry still points the original three-argument signature to2049, the common args signature to2232 and the separate financial-job owner to1646. The only preparation-page final owner moves to1455; the new four-argument private overload is registered at1452. Historical preparation predecessors remain and selection closure includes the new public successor after1646.

Independent saved-text inverse comparison, rather than reliance on preservation JSON booleans, found exactly one new sorted dispatch block. Removing it restores the entire original public function byte-for-byte. Original declaration SHA-256 is `4b3256b487c8535b00cac2993508695aa809e55275ec99c5fcf48320a3a72072`; new declaration is `cf8ce062beda5f667b96d6f5933c8adeaf98540c0c1f78b06099b7e31182253d`.

Independent comparison also proves the complete eligibility/masking point query is byte-identical to2049:2,816 characters, SHA-256 `b5c390e91f331900cfb12410af4e75b669e3cb3075e8df36e377f0091b223159`. It retains all eleven fields, exact Candidate, manual/non-taxable/one-off bank predicates, unreserved/unpaid/native outstanding checks, component count1 from a bounded first-two probe, original source family/basis, finite native amounts/times, exact bank hash/version/last-four masking and bound-origin exclusion. No money/creator/producer/adoption owner was altered.

## Index, anchor and raw-window checks

The migration adds exactly three read indexes. Created is `(candidate_id,created_at,id)`; Amount is `(candidate_id,original_amount,id)`; both retain the original2049 literal partial-index predicates, without enum casts. Paid-to is `(candidate_id,left(beneficiary_name,128) COLLATE C,finance_case_id)` on the original bank table. It does not index the entire unbounded name or add a producer constraint, table or trigger. Credit retains the original Candidate/UUID index.

Reader1452 validates Candidate, limit1–100 and exactly eight CREDIT/CREATED/AMOUNT/PAID_TO ASC/DESC strings at18–20. NEXT/current active-PAYE Candidate boundaries at21–24 match the native reader. A non-null cursor is resolved through a Candidate/UUID PK point: bank finance_case_id for Paid-to, advance id for other sorts. Missing point raises the fixed `BPAY_NEXT_PREPARATION_CURSOR_CHANGED`. Anchor resolution deliberately does not reject already-bound/ineligible facts while their raw seed still exists, preserving cursor continuation after adoption.

Closed v_direction and v_compare values derive only from the validated enum (27–29). Their `<`/`>` tuple predicates match the corresponding full ASC/DESC ORDER BY and UUID suffix. Created uses native created_at/id; Amount uses native original_amount/id; Credit uses id; Paid-to uses prefix/full unchanged C-name/finance-case-id in the same direction. All values are bound through the six typed USING parameters at68; caller name, amount or SQL is never interpolated.

The returned raw SQL window is limit+1 before point eligibility; at69 the lookahead exits before another point check. Every consumed raw fact, including excluded/bound facts, advances v_seen/scan_cursor at116. Empty nonterminal pages are valid. The cursor-progress guard at118 requires a non-null cursor different from the supplied native UUID; it correctly does not impose ascending UUID order on sorted modes. Eligible rows exceeding the shared byte budget remain unconsumed, preserving next-page reachability. A first oversized row still raises the original54000 at109; no XD07 typed consumed row is claimed here.

Paid-to raw driver includes all native bank rows for the Candidate, with no pre-LIMIT eligibility join. The full name remains the actual secondary order within bounded prefix ties, so alphabetical C order is exact. A shared-prefix group can cause additional physical reads/sort work before LIMIT. Only the returned raw window and point count are bounded by limit+1/limit; this review does not certify fully covered physical seeking, memory, latency or a real5s top-level deadline. The source includes local5s statement_timeout; previous session/top-level and future PF timing qualifications remain unchanged.

The UUID-only cursor is current-fact paging and does not promise a frozen cross-request financial snapshot. Current native key edits can change their subsequent point-resolved position; no new generation/digest/protocol was added to conceal that limitation.

## Public dispatch, ACL and transport observation

The new public page's sorted branch follows original preparation_authority. It requires a string/eight-value enum, strips only sort, passes the original three fields to unchanged2232 validation, then invokes the new overload. Unknown fields/null sort refuse. Sort absence traverses the original three-argument branch and every other existing kind unchanged. This avoids broadening the shared numeric/default args validator.

Private overload is STABLE/security invoker with the native search_path, logical ownerpostgres and full revoke from PUBLIC/anon/authenticated/service_role. It is reached through the existing authorized public owner. The public successor preserves its original security/configuration and owner/revoke/service-only grant, then sends the genuine last `notify pgrst, 'reload schema';`. Its final root has no further executable statement beyond COMMIT.

The saved Worker source now has the optional finite storedCreditSort enum only for this kind, and a sorted decoder before the legacy scalar-ID decoder. It validates the same eleven fields/Candidate, unique output IDs, native row order, and raw UUID progress while allowing empty nonterminal pages. Its comparator reuses C-byte text and microsecond time helpers and applies DESC consistently to the tie. The original unsorted schema and decoder remain. This limited observation is not a full Office/lifetime or runtime review.

## Exact source-gate admission still required

Root saved `XD-01-sorted-reader-source-hazards.json` contains exactly four sites for the new signature:

1. The Paid-to fixed dollar SELECT base assignment.
2. The advance fixed dollar SELECT base assignment.
3. The single `for v_raw in execute v_query using` site with Candidate, afterUUID, limit+1 and three native scalar anchors.
4. The unchanged first-two component `min(probe.id::text)::uuid` inside the complete eligibility point.

These are expected detector refusals, not evidence that caller SQL or UUID aggregation is valid generally. Existing `fixedQueryLiterals` cannot classify two alternate bases and controlled direction/operator variables. The safest small admission is exact1452 physical path plus exact four-argument signature, canonical whole file/function-body pin and consumed literal hazard-key counts, one each. Keep global dynamic/UUID/unknown aggregate detectors unchanged. A changed SQL fragment, enum, direction/operator assignment, predicate, extra assignment/caller or duplicate site must fail the pin/finite-key checks. Hazard keys alone are insufficient: changing v_compare elsewhere could leave the EXECUTE key identical.

The UUID cardinality conversion is safe only in its exact native point: probe is limited2, count must equal1 before c joins, and zero/two matches produce no row. Do not exempt other UUID aggregates or singleton assumptions. Complete point-query identity above supplies the reviewed evidence.

For typed financial transport, add only the exact four-argument signature's native eleven-field to_jsonb projection, two original rows/complete/scan_cursor constructors and existing `_bank_hash` callee evidence, matching the original three-argument entries. Do not borrow a whole-file/whole-name allowance or amend the original signature. Public1455 has zero newly reported raw special-construct hazards because its unchanged signature/body constructs retain their existing evidence. Normal write/frozen/ACL/RPC reload/final-owner/coupling gates still apply independently.

## Source receipts

| Saved file | SHA-256 |
| --- | --- |
| migration1452 sort indexes | `82DBD7A3C4697491CEB896296D4EAFC3EE98386EF9E65595E9C90205B495674B` |
| private1452 sorted reader | `58EEC52DEC8F9DD78ED6AC764A06522784FDB96A3AD4940802D5859D5482E4D6` |
| public1455 dispatch | `B92BFE45D52DE9A6B1DCB6FD960DB1D1C1864B186ED05DCE6B48B7CDA07A2618` |
| public preservation JSON | `39EB3C31F7483D35CE1DBBC5AF2D4336BE936BA133265413F2AEFC5F1F5E38AA` |
| actual sorted-reader hazard JSON | `311EBF33176CAB2C2FCA807C7A376818DE41F45EFB3862E0FB4852800887D144` |
| final-definer registry | `F01B2DECAD4D006E84E457F1B774C21D2668B61EDE72C2E5336CB36F44BA52B9` |
| Worker at this checkpoint | `C2B44593CF333F507071DC0AC9E85575C7FBE750C11EC5D19466D532C7F5EDFE` |
| source gate before named classification | `47FC928AFA04D0B040F5900CC72AAFFCB262AD66DB6D6913D040518E41AD87CA` |

Original2049 `94BD5588758782A0D32482D5684E18A790BE67911EB3C8E25DB4EA72CE73310B`,1646 `FEC5DD533F37C45F26E46011A551009AE8D62FB93A18862E082B2ACF74197456`,2232 `CC1491F55FF1F4AFE48F82E8892015AA2F91500F42D0D41D692F0E6B78DD3E48` remain unchanged. No reviewer execution or catalogue/release/completion claim is made. Named gate implementation, required root checks and independent Office/A4/joined acceptance remain separate.

## Final named source-gate admission receipt — 13 October 2026

**SOURCE PASS: the earlier named gate-admission requirement is closed at this saved checkpoint.** The historical SQL receipt and initial pending finding above are preserved. This is a source inspection of the actual implementation, not a reviewer execution of the detector, suite, managed compile, catalogue, release or business path.

Current source-gates.mjs2128–2164 classifies only supabase/repeatable/13102026_1452_banking_pay_next_stored_credit_sorted_reader_v1.sql and only private.bpay_next_current_stored_credit_page_v1(uuid,uuid,integer,text). Admission requires canonical CRLF-to-LF whole-file SHA-256 58eec52dec8f9dd78ed6ac764a06522784fdb96a3ad4940802d5859d5482e4d6, exactly one classified executable part with that signature, and the detector's comment-normalized executable-body SHA-256 af8c8e2cbf274faadc2730197781a9098dbb156b9dc26c9ba8ad3bca06b91433. The independently computed current physical file hash agrees with the file pin. The body pin refers to executableParts/sqlWithoutComments output, not raw SQL body bytes; these representations must not be conflated.

All actual special-construct sites are collected into a counted map. It must have exactly four distinct literal keys, with count1 for each reviewed two fixed query-base assignments, one six-parameter typed EXECUTE and the original bounded/guarded component cardinality point. Admission merges these consumed counts into the baseline; the original assertSpecialConstructs detector still runs. Wrong path receives no allowance. Changed SQL anywhere in the whole file or executable body, enum/direction/operator assignments, predicates, additional/duplicate sites or another signature cannot retain this admission. No generic dynamic-query or UUID-aggregate rule was relaxed.

At631/662–663/709–711, the new four-argument signature alone receives the same native eleven-field projection and two original rows/complete/scan_cursor envelopes as the preserved three-argument signature. The original entries remain. Existing _bank_hash callee evidence and typed/write/frozen/authority/ACL/reload/final-owner gates still apply independently. The changed-file branch2477 invokes this exact classification before the ordinary detector, rather than skipping the detector or borrowing a broad function/file exemption.

Saved gate SHA-256: 43331432430040D6795B91A849D40EFE6136018D6DDAA13702CAFE6C7DF18FB5. Current Worker SHA-256: 51CFC44CE1F852792601FA2BD8D89CFC2D2196AC7A5D32F1C04A69049352E797. Worker/Office lifetime, hidden replies, raw-empty continuation and exact native error handling are recorded separately in XD-01-client-source-review.md. That client receipt is explicitly pinned to the reviewed XD01 checkpoint and excludes the concurrently saved XD02 action/binding join.

Root subsequently reported local PG17 load/catalogue, source-suite and sealed coupling checks. They remain root execution evidence and were not independently run here. No new A4, latency, fully bounded physical Paid-to prefix-tie scan or whole-card completion claim is added by this closure.
