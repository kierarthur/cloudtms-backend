# SEND10 linear packaging source review — 13 October 2026

Verdict: SOURCE/DESIGN PASS for the exact local packaging commit below. It preserves the owner-approved source tree while avoiding introduction of the rejected historical merge into TEST. This review does not itself authorize edits, promotion, installation, policy bypass or runtime activity. Root's existing SEND10 fix-forward authority controls those actions and their required checks. Reviewer reads static Git/source evidence and writes this document only.

Read the full current original-backend SEND_10_TO_BANKING_PAY_FIRST_INSTALL_GO.md. It approves backend code2c61f97b38c31b4751177f1d1f62b815679b1193 with docs-only record0706991301857b6deaccad482471db36bf66d124 and Office413cea9b35975c6ccac0b084327ef109b05c1c48, canonical contract1f5b6396215c1ccaec01e69c64beb5663ac905a37329f0d7278adca55dc62a26. It expressly permits reviewed fix-forward/gates/targeted rollback after failure, forbids force/rule/ledger weakening, retains LEGACY and requires fresh remote/ref/connection evidence and the SEND7 lock throughout. Root reports GH013 rejected the promotion solely for historical merge a4e87c601bd567b14240af483283d602d21cc715; no install occurred and remote TEST remains e0a02bb5. This reviewer made no network call to independently re-probe that reported remote state.

## Independently verified local commit/tree boundary

- Actual named worktree:C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-first-install-linear-20261013.
- Actual branch:codex/bpay-next-first-install-linear-20261013.
- New commit:c2472e941575f7820c5a4207dc867c2bbe8cb080.
- Its sole parent:e0a02bb5f86b1fb2134e540bbf0ccc52f6feec92.
- Its tree:36c7cacaefc54aa1b804af36b9225366ce741289, exactly the approved07069913 tree.
- `git diff --name-only 07069913..c2472e94` is empty, including scripts/workflows/SQL/locks/contract/Worker/package inputs. Working tree is clean.
- e0a02bb5 is an ancestor; its new range has exactly one commit and no merge commits.
- The original item6 branch still points at0706991301857b6deaccad482471db36bf66d124. Original2c61/e67/a4 objects and their identities are unchanged.

The approved07069913 differs from2c61f97b only in docs/bpay-next/FIRST_INSTALL_CANDIDATE.md. Thus exact full-tree equality covers the reviewed item6 verifier line plus approved candidate metadata and all prior reviewed source; no later DR22/DR24 source is included. c247 is not claimed to descend from07069913; it carries exactly that source state with a different, truthful linear parent. Old commits/branches remain auditable. This is a new forward commit on top of current TEST, not an amendment/reset/rebase/force rewrite of published commits. The forbidden new merge is absent from the introduced range; no rule configuration was changed.

The ignored authoring script was inspected, not executed: it uses a fixed exact approved tree and fixed TEST parent, checks remote TEST equals that parent and the approved worktree is clean, checks identical tree/empty file diff after commit creation, creates a previously nonexistent named branch/worktree, and records the new identity. Its Git operations do not reset/amend/force/update old branches. Source review verifies the resulting actual Git objects independently rather than trusting its JSON claim.

## Gates and release semantics remain intact

Full tree identity means locked migration source/path hashes, recursive include authorities, F04 metadata, security/legacy gates, contract, toolchain and protected release workflow are unchanged. Current verifier17092026_0100 is exactly the SEND9 reviewed one-line addition, SHA2EAF7DA4939C8A3CCFFB20F089D1785A2BA210119327ADEC1050D1D4B23CB565. No verifier assertion or financial/capture/Source owner was altered by packaging.

One potential ancestry failure was checked explicitly: the F04 registry releaseBaseCommit is a6f7423eb71e6e7cd173e705c2290e5408a1d077, and it remains an ancestor of e0a02bb5 and actual c247. The existing changedSourcePaths function still requires that exact base to be an ancestor and includes both first-parent/working changes and the full base→HEAD range. No environment override or baseline change is needed. Protected database source-gate checkout has fetch-depth0 and derives that same registry base. Old reviewed baseline objects remain retained on their existing published refs; a shallow/missing-object checkout must fail rather than omit them.

Contract coupling still checks actual HEAD diff-tree and working changes. c247's first-parent range represents the whole approved delta from e0, so the real regenerated current-contract.json accompanies the contract-bearing SQL in that same commit. An old code/doc commit's PASS is not substituted for new commit-range checks. Root must rerun db:check/coupling/source/legacy/final-definer checks at c247; packaging does not exempt them or authorise regenerating/blessing another contract.

The release coordinator and Bible inspected at the actual approved tree still require source committed/pushed to canonical backend TEST, fresh agent-created connection evidence, protected UPGRADE/AUTO, immutable installed ledgers, exact source/contract admission and full required verification before VERIFIED. Migration selection/recursive closure identity is based on the installed ledgers and source hashes, not synthetic commit ancestry. Same SQL bytes therefore preserve the reviewed94/219 pending source inventory, but a fresh actual plan must prove it; do not reuse an old commit-bound admission, manifest/resume packet or successful release identity for c247. Any APPLYING-only verifier still belongs to the genuine protected install, never fabricated rollback admission.

## Required retry evidence and concrete risks

Before non-forced retry, Root must freshly re-check backend TEST/deploy and Office main against SEND10's expected heads. This review's local origin/test or author-script earlier remote check is not proof against a later race. If any head has moved, integrate under SEND10 and stop that promotion until its changed scope is reviewed; do not force the original approved tree over another task's newer work.

Run the exact same finite pending94/219 plus item6 targeted rollback with inventory guards explicitly IMMEDIATE on verifier entry, complete mapped IDs/S19–S24 bodies/zero diagnostics, explicit ROLLBACK, fresh18 fingerprints/marker absence and actual lock continuity. Root reports the SEND7 external shared lock is already held; this reviewer did not inspect the database/session. Keep that lock through the approved release/verification and release it under the owner sequence. Do not release/reacquire invisibly or run a duplicate protected job. No broad rehearsal framework, business fixture expansion or ledger write is needed for packaging.

Bind the new published/release receipt to actual c247 (or any separately reviewed docs-only successor), retaining the honest mapping to code2c61 and approved070 source tree. FIRST_INSTALL_CANDIDATE currently retains the old candidate's valid historical record because its tree is identical. Subsequent actual installation records must identify the new release SHA; never describe c247 as070 or claim unchanged Git commit identity. Any extra source delta after this receipt invalidates exact-tree approval and needs its own review; later BUILD-BE DR22/DR24 and this review file remain outside the frozen release source.

The deployment order, Cloudflare trigger/secret-name checks, fresh deployment identity, real→synthetic→broker ordering, Office-last gate, LEGACY:1, read-only smoke checks, TEST_INSTALLS handback and three-failure limit remain the owner's exact requirements. Promotion rejection is not a database install attempt; record each failure stage truthfully. This source verdict is not a guarantee that GitHub accepts the next push, a green protected workflow, unchanged target data or complete app/activation acceptance.

## Static source pins

SHA256 raw saved bytes:

| Source | SHA256 |
|---|---|
| SEND10 owner instruction |7631DB62BC4FC1CCE432D608D1906FD5CD7937EDC449C4C5276DA4FFDAD31DDA|
| current-contract.json |5ADFD4B690AD8D21CE148AAFF5D50F8DE71EC18E9BBB7840CDAC661B481354F8|
| migration-lock.json |FDEF5B5A726C74EAB1E679DBF1C8C13765D170F9472AA78C8B71B711BCB80C68|
| Candidate F04 registry |384C04E7D3452806FB508F45CB6AF7B4B196DCD60176E5F6E0E8687C2465F60A|
| Source gates |27A089CDF4C856737C66681275AC8F9A237A0FC564AD0DD90B17D93FFDF0799D|
| Final-definer gates |4D7AD99B9AF129BB3D641A03E9F6EC13C29A7527CEA47470713B57FDA52C4E8D|
| Coupling gate |4F99EC5C4861680EE5245C40D58677BFA6E19A00B5A5523181C21C44773A8598|
| Protected database workflow |602168AFAC04F4ED08A826D1714D9060780742D95559408AC037EB0866B851A1|
| AUTOMATIC_TEST_DEPLOYMENT.md |9BCAD58F6C0C1D7B015BCDC881770A45D16AD2FF6F69628D4EF3CC9C59F118FE|
| DATABASE_RELEASE_BIBLE.md |968AB48D5B88181E6EB12F067309DD41162D3A2D816B47FB076E2BB4B87A7F5B|

Candidate registry/gate pins intentionally differ from ongoing later BUILD-BE files: exact approved frozen tree is the only authority for this package. No product edit, SQL/test/module execution, database/RPC/network call, Git mutation, promotion/deployment/install, secret read/disclosure or message to another chat was performed by this reviewer. Concrete local tree/ancestry/source subset has no remaining blocker; Root's actual checks/rollback/promotion/release receipts remain separate.
## Actual gate-range failure and narrow checkpoint repair design

Root's actual local c247 gate run reports31 PASS/1 FAIL. G10-01 OPEN in the unchanged candidate gate: line2428 requires intermediate FOUNDATION_RECONCILED3c91e5e6ce35b5833a5c218b77b741261eeebafa to be an ancestor of HEAD. Independent static Git inspection confirms it is not an ancestor of e0a02bb5/c247. It is a reviewed build-branch checkpoint retained on the unchanged published item6/build branches. F04/FROZEN_BASEa6f742 ancestry remains valid, as recorded above; that distinct check did not prove this intermediate checkpoint prerequisite. The original exact-tree packaging receipt remains true, but the actual new-range gate failure blocks promotion until fixed/rechecked.

Narrow fix-forward design PASS: replace only the intermediate ancestry prerequisite with fixed checkpoint commit existence/type (`git cat-file -e <exact FOUNDATION_RECONCILED SHA>^{commit}`), keep historical `git show` at that same fixed SHA/file, add one static literal SHA256 check7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b on the retrieved canonical-LF whole2018 file, then retain the existing full current-file equality and every subsequent signature/hazard/latest-successor check unchanged. This is a change to the existing evidence test, not a new framework or financial allowance. It permits the GitHub-required linear package to consume the same immutable original source checkpoint without requiring its old branch topology to be introduced into TEST.

The exact independently observed checkpoint identities are:

- Commit:3c91e5e6ce35b5833a5c218b77b741261eeebafa, actual Git type commit.
- Tree:9f0c4bb84d4b94f5f5ddc14f7f717d3b949e365d.
- Historical path:supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql.
- Git blob:793ed62d1f80b722fa7d4eaec445b6aa58439c28.
- Canonical-LF whole-file SHA256:7433AC2A1CCFE64613666EED4FF672E81C67A36BB8C9ABBC6585833B9DCBE09B.
- Current retained2018 complete LF bytes compare exactly equal to that historical file; the current raw file has that same SHA at this checkpoint.

A fixed full Git commit identity already binds its actual tree and blob; separate runtime tree/blob assertions are not required beyond the proposed explicit source SHA. Their values above document provenance. Missing object/incorrect type/Git failure, wrong checkpoint file/hash or any changed current2018 byte must fail. Do not substitute HEAD, change the checkpoint constant, dynamically derive the expected SHA, remove the original current-file comparison or trim source text. Only CRLF→LF normalization is permitted by the existing comparison. Required frozen-base ancestry remains unchanged; preserved old published refs/fetch-depth0 supply the checkpoint object to CI. No network fetch, mutable reference or branch-presence assertion belongs inside this source gate.

The existing exact four correction signatures remain unchanged: bind-simple-CSV destination, issue-simple-CSV, receive-simple-reissue and work-collection recovery. Each must still have a genuine final successor outside historical2018, its actual complete definition must still pass baseline special-construct checks, and only the original checkpoint's named rowtype statements receive their exact-key finite occurrence allowance. All other source checks still run afterward. No new row/aggregate/dynamic/history/schema wildcard is admitted. The same fixed checkpoint's closed payment-history producer comparison at1892 remains unchanged; it already consumes exact historical source identity rather than independently requiring ancestry.

Root should implement this as one source-gate fix-forward on the linear branch, rerun the actual32 source/legacy/final-definer checks and coupling/db/node checks, retain/pin the new source commit and redo required exact targeted rollback under the owner's held SEND7 lock before promotion. No application SQL/DDL/financial/fixture/contract body needs to change for G10-01. The already-running same-tree rollback proves the unchanged candidate SQL only; it does not prove a later gate commit's source checks. Document both receipts honestly.

Reviewer approved this exact design via parent message and wrote this appendix only; no gate/product edit, tests/module/SQL/database/network/Git mutation was performed. Actual amended-source closure is pending. No promotion of failing c247, oldcommit rewrite, repository rule change or financial source weakening is authorized by this review.
## Final saved checkpoint-fix SOURCE closure

Verdict: SOURCE PASS; G10-01 CLOSED in the actual saved linear-worktree gate. Current raw gate SHA256:677AC11232B3D7C2728F22945C3F31F640BA3F01C1B76D33E61849792B370437. The entire change is exactly the agreed five added/one removed lines: three explanatory comments, fixed commit-object cat-file check instead of intermediate merge-base, and the static7433ac canonical-LF historical-file hash. Git diff has one hunk in this block. Reversing only these exact patch lines in memory reconstructs the original raw candidate gate SHA27A089CDF4C856737C66681275AC8F9A237A0FC564AD0DD90B17D93FFDF0799D, proving every other gate byte—including FROZEN_BASE ancestry, historical/current complete equality, four signature set, final current authority checks and finite hazard consumption—is unchanged.

The new fixed checkpoint-object and source-hash checks fail closed. Expected hash is a literal from the independently inspected original2018 source; it is not derived from mutable current source. No financial/history/dynamic/aggregate/type/route/body/ACL exemption was added. Source-historical complete file comparison remains after the pin, and latest current successor validation remains before historical row allowances. Both historical source identities and the actual tree/blob facts in the design appendix remain the same.

The current working diff contains only scripts/bpay-next-source-gates.mjs. Comparing against approved070 while excluding that exact gate path returns no other changed file. Application SQL, item6 verifier, migration lock, candidate F04 registry, workflow and contract remain the exact approved tree bytes; their previously recorded hashes are unchanged. This new reviewed gate fix is therefore the sole source exception to c247's prior exact-tree packaging identity and must be recorded in its actual new fix-forward commit/receipt. It is not described as another identical070 tree.

Root reports the actual32-check rerun is underway, and the held-lock same94/219 targeted rollback is in progress with no error at the supplied checkpoint. This source review makes no completion/PASS claim for that running execution. Reviewer ran no tests/modules/SQL/DB/network, made no product/Git edits and wrote only this review appendix. No remaining concrete source blocker is identified for the exact five-line fix; actual gate/rollback/promotion/release receipts remain Root-owned and separate.