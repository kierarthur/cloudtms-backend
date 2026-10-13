# SEND6 final targeted evidence review — 13 October 2026

Independent read-only handback review of saved source, all four targeted rollback receipts, generated SQL/maps, the verifier-only executed assertion manifest and final source manifest. No SQL/RPC/database/business, network, test, fixture execution, product or Git mutation was performed by this reviewer. Root owns the recorded executions. Earlier source/design findings remain in their original review files.

**Verdict: scoped SOURCE/EVIDENCE PASS.** The complete saved receipts support the successful final targeted checks for SEND6 items1–5 and the later fixture/ordering forward fixes. **Item6 remains OPEN; this is not a clean full-release rehearsal, installed-contract verification or installation/promotion approval.**

## Complete finite receipt reconciliation

| Saved round | Recorded UTC interval | Source / returned statements | Verifier statements mapped | SQL errors | Rollback / fresh18 / marker absent |
|---|---|---:|---:|---:|---|
| R1 original targeted set | 18:05:37.790–18:31:40.952 | 5,910 / 5,910 | 1,544 | 12 | All true |
| R2 four core verifiers | 18:45:50.320–18:54:52.960 | 4,560 / 4,560 | 194 | 2 | All true |
| R3 invoice admission alone and native includes | 18:56:19.342–19:09:19.410 | 4,947 / 4,947 | 581 | 0 | All true |
| R4 schema and first-authorisation follow-up | 19:10:00.133–19:21:05.181 | 4,455 / 4,455 | 89 | 0 | All true |

For every round, this review independently compared every generated marker's SQL text SHA256 with its source map; all matched. Every mapped ID was returned exactly once, with zero missing, duplicate or unknown IDs. Each verifier-only assertion entry exactly matches the full map's file/ordinal/kind/hash/head and its actual saved receipt's SQLSTATE/error, with zero omitted or duplicated verifier entries. These are SQL statement receipts: an enclosing successful DO covers its internal assertions, but the manifest does not invent separate receipts for each internal assertion.

R2–R4 record exactMembership=true. R1 did not originally store that field; its complete unique membership was independently checked from its retained map/receipt, without changing the original record. Exit0 did not erase R1's12 or R2's2 failures. The recorded failedStatements/errors/nonzero SQLSTATE counts agree in each round.

All four receipts identify cloudtms_test_clone, explicit rolledBack=true and freshConnection=true. This reviewer compared the actual18 before/after fields, including migration/repeatable/routine/relation/column/index/policy/trigger/constraint/default-ACL fingerprints, identity/control facts and task-marker absence: every field matches. R4's current scope is TEST, null Customer key and LEGACY:1. These saved facts support the recorded transaction boundary; they do not prove that every business row outside those fingerprints was examined. Sequence gaps remain possible.

R4 contains two core files, not four: publication0300 and withdrawal0610 already passed R2. Final browser isolation and native support are included in the successful rounds. The recurring94 migrations/219 closures belong to the targeted rollback packet; they are not a claim that all154 mandatory eligible verifiers or the installed contract were verified.

## Final source-to-execution binding

All ten current saved verifier SHA256s match SEND6-verifier-source-manifest.json. Static comparison of the mapped untransformed verifier statements against current saved files found R1's1,514 matches plus its six subsequently replaced statements, R2's184 matches plus the two subsequently replaced statements, R3's577 matches and R4's85 matches with no residual differences. The remaining24/8/4/4 entries respectively are the already-reviewed fixture savepoint transformations of source transaction envelopes. The old six/two statements remain in their historical maps and failure receipts; final coverage comes from the later rounds.

| Current assertion | Successful executed ID | Current statement SHA256 |
|---|---|---|
| Schema0100 S19 / ordinal19, verify_entitlement_heads | R4 S004390; also R1/R2 | d08cbc90355ba83c31ac09f8673d2dac0fad6b1f15ded75aa5bf0a72443f35f9 |
| Schema0100 S21 / ordinal21, verify_entitlement_receipt | R4 S004392 | 2d5e337a870ed9d0b8dcf2876bdd88796607cc87abdda1d3ff457e7bdb71203f |
| Publication0300 D10 / ordinal65 | R2 S004468 | 59d995920644a679e8f2fba723f226ad636c3e36132a4b171fb8cb8e97a18294 |
| First-authorisation0600 ordering / ordinal44 | R4 S004448 | e310afc730f80c3964c17ac6f83ff604b30c982b8277ec06fbcc01360dcd667d |
| Ordinary projection1534 F5 enclosing DO / ordinal78 | R1 S004994/S005289/S005574/S005886; R3 S004621/S004916 | b99825c2105a4c3f7b7bd212317fa8967a4713fa82b62489b7a9ecbaa86e6f6a |
| Invoice admission1534 / ordinal46 | R3 S004943 | f5f32ab154bf44b0f6c4a5ad05d5f46bfbefc170e6a2a540cc3cb08f7fb13cc0 |

The final0600 ordering DO was reread from current disk: the caller-frame probe INSERT returns xmin immediately before the real native withdrawal. Both owner writes must equal the same non-NULL probe xmin; top-level before/after equality, allfive row bounds, managed guard/observer proofs and forced-failure rollback proof remain. Executable and quoted-SQL forbidden-token checks retain the original closed token set, with all three relevant closing word escapes now \M. The separate transitive guard prefix/suffix at2607 is unchanged. This closes S6-OF01/02 in both source and its final successful enclosing execution; it does not weaken same-frame proof to a loose lower bound.

The final0100 S21 preserves real receipt UPDATE/DELETE immutability refusals and every existing receipt shape/release-record assertion. Its real TRUNCATE must refuse: incoming real FKs are detected from the actual catalogue and require0A000 with the native FK refusal text; without those FKs the native55000 immutability refusal remains required. A FK-free empty temporary LIKE copy installs the exact existing native BEFORE TRUNCATE statement trigger/function and asserts exact real/probe trigger identity, flags, absence of arguments/predicate and enabled state before requiring55000. The native plan6 immutable function unconditionally raises the same55000 and is not replaced. No CASCADE, removed constraint, disabled guard or swallowed refusal is introduced. R4 executes the exact complete current S21 successfully.

Invoice ordinal46 is byte-identical to the R1 failed40P01 statement, rather than a narrowed replacement: its same hash now succeeds in the separate R3 turn. The actual F5 enclosing native ordinary-pay assertions succeed in all four R1 include contexts (both invoice parents/sections) and both R3 invoice-admission contexts; named inventory timing and the original negative/rollback assertions remain as reviewed.

## Owner item handback and limits

1. The two manual-review pins are the only changed manual pins; both existing authority verifiers passed R1. The retained25-family authority/ACL/security assertions were not relaxed. Their current full-file hashes match the final manifest and previous source review.
2. The sole Authorise pin and provenance comment match the clean PG17 canonical native readback. Final withdrawal0610 passed R2. No Authorise owner replacement occurred in this verifier scope.
3. Exactly six legitimate fixture actor rows receive the required permission; unrelated actors/pins are preserved. The corresponding unchanged affected absence/audit/ordinary verifiers passed R1, while final core fixture behavior is covered by R2/R4.
4. D10 uses the fresh dedicated native Save result and original request/lock helpers, not whichever older pending bundle happens to be latest. The current strong positive publication receipt/persisted state assertions and deliberate rollback control pass R2 before the original tamper/refusal/no-write negatives. D8's matching zero-head receipt and withdrawal's named native construction flush are covered by successful R2; final0600 is covered by successful R4.
5. F5 defers only its named inventory constraints inside the existing exception scope, completing native construction before forced IMMEDIATE checks. Its original hostile assertion and rollback controls remain; actual successful enclosing executions are listed above.
6. **OPEN — original full-release inventory mismatch and five cascades remain undiagnosed.** Current instrumented S19 succeeds in targeted R1/R2/R4 with its original real count2 fixtures. Therefore its failure-only closed phase/head/count MESSAGE instrumentation did not produce the missing original failure facts. That targeted success is useful evidence of this packet, not a diagnosis or repair of the original full-release failure. Installation/promotion stays held under SEND6; commit/push handback alone does not remove that hold.

Prior clean local PG17 compile/seal records semantic contract1f5b6396215c1ccaec01e69c64beb5663ac905a37329f0d7278adca55dc62a26. It remains compile/readback evidence, not a full local/TEST rehearsal claim. This reviewer did not repeat a SQL call or approve an install. SEND7 session-turn source review and the older S6-R01 success-predicate closure remain in SEND6-final-source-review.md; this handback independently confirms complete membership in every retained round.

## Durable evidence pins

- SEND6-targeted-rollback-round1.json: D379667446E70D7AEF37825D72F25A6FD070E6397D9B9A222699850C1800CD97.
- SEND6-targeted-rollback-round2.json: 198A26681A157D7D7AE4521A7AC5D9AC29520965BBB9EDD271F73D0AA33D696F.
- SEND6-targeted-rollback-round3-invoice.json: 31979BA075FFD6F20127F055B2DE943F854F29CDFC82E4A688F03012C30B5E96.
- SEND6-targeted-rollback-round4-core.json: 3CD725EAE010E49B7299FB9260BF52DF8C744979C7F33A086A4C60E57FA04198.
- SEND6-executed-assertions.json: AFD5C87A1AC8958F53F6BF4E90A24DBB7006977DE11F4F2FA9189BD1DA1F3BAA.
- SEND6-verifier-source-manifest.json: D78D855671444B3174645DC478467877ED03575E3C1EAE7DDCF906D9EF587B35.
- Current schema0100: 282FBCDFDD33ED4D59C6E5A0ACFBD929C8A2C2C96229D64BDEF8C927085ABFD4.
- Current publication0300: 9043CA8E254CDEA64867C9DF0EC7FDF55A21B2C29AE12907B2B87BECEA4EC646.
- Current first-authorisation0600: D0C8CEB99C7539C77C2463E6E46C2F7B26F7C59D1E1ADAC5423A7EBEE49F8A3E.
- Current withdrawal0610: 98B819B77FF45889F559C772E10E5A4763D6272CDDA86119F560CAC1031DD557.
- SEND6-clean-PG17-seal.json: B115BB4B943D43EB75C473EC2AB919439A750BF3361EBB1516DB1DB4BFE87E9C.

The executed assertion/source manifests bind the remaining exact files/statement hashes. Any later source or evidence change needs a new scoped review; historical failure evidence is preserved.
