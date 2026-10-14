# Prepared output retention — independent source critic

Reviewed 13 October 2026. Verdict: **SOURCE REVIEW HOLD — one P2 native-shape compatibility finding; native/money qualification remains held.** The proposal addresses the correct missing authority: immutable retention of the actual bounded writer's normalized output. This report is a focused source review, not final Part B, a native financial certificate, installation approval or permission to expose Report money.

The reviewed lane is `C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026`, branch `codex/healthroster-v2-report-build-13102026`, HEAD `54e06721ab030e5060272ec6a0a58bf47a17587d`. Saved lane status was clean and the local origin tracking ref equalled HEAD; this critic did not fetch or independently contact GitHub. Root was `C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-v2-backend-13102026`, HEAD `cea15289e0283e3383e71f0acb8b18ce184647c2`, with only the pre-existing untracked `docs/healthroster-build/checkpoint13102026/FULL_VERIFIER_SOURCE_PROTOCOL_CENSUS.json`. That file and every root/lane/frozen source were preserved.

Reviewed packet: `docs/healthroster-build/proposals/report-prepared-output-retention/`. Exact `SOURCE_MANIFEST.json` SHA-256: `80a2577cbf5016f1a76e323717809c5553070e47b0b9a80e11b1ef3a0af34c70`. Package digest: `f196d1f0b089307333c17458b7502d28a02023228caab070c384f8827cce10c1`. All 30 saved files were read, all 29 listed member byte lengths/hashes and the package digest were recomputed, and all 15 controlling root input byte lengths/hashes passed.

## Ranked source finding

**P2 — the closed codec narrows a valid native `text[]` shape and can refuse an otherwise valid initial preparation.** `02_codec.sqlinc:221–224` and `05_retained_amount.sqlinc:118–121` require each immediate `hr_crosscheck_issues` array element to be JSON null or string. The full saved native schema/current contract declares nullable `hr_crosscheck_issues text[]` and contains no array dimension constraint. PostgreSQL `text[]` permits multidimensional arrays. The unchanged bounded writer preserves `prev.hr_crosscheck_issues` when the snapshot omits this field (`inputs/bounded_writer.sqlinc:735–742`). The complete controlling `weekly_source_ordinary_projection_snapshot_assert_v1` has a closed TSFIN keyset which omits `hr_crosscheck_issues`, so this inheritance is reachable on the reviewed ordinary projection route.

For a prior native value shaped like `ARRAY[['RATE_A','RATE_B'],['RATE_C','RATE_D']]::text[]`, native JSON rendering contains nested arrays. The proposed validation rejects those immediate array elements as `WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID`. A supported initial preparation therefore refuses after the native writer instead of faithfully retaining its valid native output. This is a compatibility/availability regression, not evidence of an incorrect amount being accepted. It is a source-derived counterexample; no database reproduction or claim about existing TEST business values is made.

Before treating this packet as source-ready, preserve all native array dimensions/content in both capture and historical validation, without flattening or accepting non-text leaves; alternatively establish an exact enforced native one-dimensional invariant covering retained rows and all writers. Do not add a new data restriction merely to satisfy this codec. Add a meaningful boundary check for the inherited multidimensional case; the present 30 source checks do not cover it. The generator's `outputValidation` produces both affected validators, so a correction must update the generator and refreeze the entire packet with new pins through its owning lane.

No additional definite source defect was found in the reviewed capture/receipt/amount changes. The following holds remain independent of this P2 finding.

## Ranked qualification and integration holds

1. **P1 — actual first capture, atomicity and first use are unproved.** The packet expressly records `semanticSQLCompilation=false`, `nativeWriterCapture=false` and `nativeFinancialAcceptance=false`. The data-free verifier invokes only explicit NULL behavior. Thirty source tests verify bytes, field lists, signatures, ordering and source predicates; they do not execute PL/pgSQL, compare an actual normalized financial row, prove timestamp/marker reconstruction, exercise temp ownership or certify a genuine monetary journey. A native returned ID/version, output-vs-input normalization difference, known zero, first capture, replay, same-ID lawful rewrite or native replacement, expense interval, intervening component/policy/amount/lifecycle rewrite, context collision/forgery/direct-role negatives, failure rollback and fresh restoration are all required. Source SQL packaging is not semantic compilation.
2. **P1 — canonical composition, full contract and combined ACL qualification are unproved.** The proposed 2300 migration, 2301 repeatable and 2302 verifier names were absent from canonical root. Existing receipt schema, V1 insert owner, public DTO and all source lifecycle classes remain the authority until root integrates and qualifies an exact successor. The packet's declaration census is **105 tables / 239 private / 142 service**, where 239 is retained 232 plus these seven. It is not an installed census. Root must own the combined union with any future Q/258 or A06 packet, regenerate all central/final and independent inventories from the exact composition, retain every old registration and review the full generated PostgreSQL 17 contract. Installing this fixed 239 verifier beside another additive census without a guarded combined successor would be incorrect.
3. **P1 — protected active/withheld and headless NEXT money remain unavailable.** Report's native SourceHEAD and LOCAL predicates are byte-preserved, not newly financially qualified by this packet. RETAINED_C1 invoice provenance supplies no active Banking amount. DIRECT_NEXT retains the exact I3 call and its headless PROTECTED exclusion; the new proposal does not supply a Bank application join. Withheld amount remains NULL/known=false. Root/Bank must establish the actual first-installed controlling owner and exact current component/application authority. Prepared minus active, COMPLETE, accepted transport, authorisation, current TSFIN status or paid flags cannot manufacture withheld/settled pay. No Bank worktree was accessed.
4. **P2 integration caution — preserve the rehearsal outer transaction.** `13_data_free_verifier.sqlinc` contains `\set ON_ERROR_STOP on`, `BEGIN`, and `ROLLBACK`. Raw use in the saved root full-rehearsal renderer correctly refuses `REHEARSAL_OUTER_TRANSACTION_ESCAPE_REFUSED`. A read-only source exercise of root's existing `neutraliseReviewedVerifierEnvelope` removed exactly those three reviewed envelope commands; rendering then succeeded without a database connection. The source verifier hash was `8002ea36d365056771c6fa340210c38b54a504f46300969e2df32cf3954e0010`, reviewed body hash `023befe27091df66c888bf27762d63d26074c6546adaad0ff9e25676521d04e3`, single-unit renderer hash `0ccbfdaa23106b6f06ce6f9546e961e2c4f02af65cafe987efa44d80a135454a`. Root must record that exact envelope review when mounting the new verifier. This is an unmounted integration prerequisite, not a request to weaken its body or a new database rehearsal result.

## Source assessment

The two additive nullable columns belong to the existing immutable ordinary projection receipt. No durable table, backfill, default, new origin, ledger, caller amount, calculator, rate lookup or provider behavior is introduced. The pair constraint uses null-safe schema-version comparison, a 32-byte hash and PREPARED/financial-ID bounds. Historical two-NULL attachments return unknown before reading any current authority. Partial or bad attachments refuse.

The complete ordinary successor reverses exactly by removing only its capture hunk. Both replay exits precede the writer and capture, and cannot backfill historical receipts. Native current revision/export/Client/Contract/actor/request closure, source units, I1 locks, preflight, canonical calculation and immutable Candidate submission checks precede the unchanged bounded writer. The supported predicate resolves the final revision's actual immutable rule version, CYCLE/ROSTER export and complete Acacium separate-report purpose. It does not use a live flag, caller vendor selector, report totals or capture GUC. NHSP, no-preset/NULL or unrelated purposes return the unsupported arm; no-op, proposed and target-managed paths retain their original bodies and receive no output attachment.

Capture selects the financial ID actually returned by the writer, verifies current native root/version and Candidate/Client/Contract identity, and compares the complete original eight-key writer response and original ten-field marker. It serializes the stored row; it does not relabel service input as output. All 90 codec names/type/nullability declarations match the full saved current-contract financial relation, whose complete native row has 95 columns. The remaining five lifecycle lock/payment columns are covered by the entire native row digest during capture-to-receipt comparison. The 39 numeric money/rate/quantity fields are PostgreSQL text and the nine JSONB fields are PostgreSQL JSON text. No JavaScript numeric conversion or new arithmetic occurs. The array compatibility finding above prevents claiming universal native type-shape fidelity.

The owner-created temp context is fresh and ON COMMIT DROP. A pre-existing relation refuses. The source checks exact temp namespace/owner/relation kind/columns/defaults/column ACL/owner-only table ACL/trigger absence and primary key, then binds one row to backend PID, transaction ID, provider-neutral executing owner, request, actor, root, actual FIN/version, Client/Candidate, frozen rule, input service hash and canonical calculation fingerprint. The receipt INSERT guard forbids any supplied attachment, including a matching one. It requires one exact matching context and unchanged V1 receipt/hash identities, re-reads the same writer-held financial row and compares both the closed output and complete native row digest. Drift refuses with exact `40001/WEEKLY_SOURCE_PREPARED_OUTPUT_CHANGED_BEFORE_RECEIPT`. Context consumption is row-count checked, then the temp relation is dropped. This source has no new family/group/cycle/Timesheet lock or GUC authority bypass.

The intervening retained expense owner inserts immutable Source expense materialisations and does not update TSFIN; the retained root hash is a read. The saved full contract lists native financial AFTER row/statement triggers and the receipt's existing immutable update/delete/truncate guards. It is saved source, not fresh installed enabled ordering. Root must verify the actual trigger/dynamic owner interval on the owned SEND_7 turn. Preserve CAS/write hashes, dates, native actor capability, I1/F1/G1/family locks, existing returned failure handling and rollback semantics. Compilation cannot replace first-use proof.

The independent retained reader derives only stored `total_pay_ex_vat` text after attachment shape/hash, unchanged V1 receipt, exact scope and original writer marker checks. It selects no current TSFIN amount/status/paid flag. The complete Report successor adds only the retained amount call plus Candidate/version fence, known flag, conditional unavailable reason and ROOT_FAMILY/IMMUTABLE_NATIVE_WRITER_OUTPUT metadata. Removing those four changes restores the controlling Report body exactly. Known stored zero remains zero; NULL remains unknown. Saved prepared pay, active entitlement, invoice provenance, first Authorise and settlement remain distinct. The compact nine-key invoice witness and explicit public ordinary receipt DTO remain unchanged.

Both full central ACL owner proposals reverse exactly to their retained 232-helper roots after removing only seven entries. The full independent verifier similarly reverses with its seven entries and 239→232 count correction; no old diagnostic/assertion was removed. The final closure installs codec/context/guard/read, then complete successors/registries/ACL, finally owner mapping and private revocations, with PostgREST reload last. No new public service signature is introduced. Service_role/anon/authenticated/PUBLIC have no intended new helper EXECUTE. Actual provider-owner ACL and route denial remain native tests.

## Exact pins and checks

- Controlled runtime input root: `cea15289e0283e3383e71f0acb8b18ce184647c2`; source forward verifier `d67f94e19000a612ee4f1099c14429d8a8b4bee1`; original reverse pin `b65df0997ac69188eadf4de2a88f70226fa99532`.
- Saved complete current-contract SHA-256: `cd57300a2913fe10ffbda4bfe03febf3ee859f54c53d7d92a1118205910c7334`; all 90 financial field types/nullability matched. It does not seal this unmounted proposal.
- Native bounded writer full extracted definition: `a2c478f9f90e17a61c8c4fbf039ce422fbf3c84ce94ef5a162b7616c7909354a`; native receipt insert: `2a7022217def63a069a9c3ca149179e1fa99a43994cb36a31e1c62d11dda43f7`.
- Full ordinary reverse definition: `5043fc2a667f47d7e1c6b3f09675744d7ca82b08a89da92edcc1557d3924d670`; full Report reverse definition: `ceaecb6d25cfd64023eea2f5b69a19ed64a545fab6a666942d72380b3f8ebeee`.
- Full registry reverse: `14b0b8336da22b7111b0f1ff8811286c4e4e7646e820e1261fe5532ecc79e7e9`; final ACL reverse: `2c1efa029d5c4f2f0e170dc0afa5f3c93a2bc85b60d6921ee04781edcba951f3`; independent verifier reverse: `4fe4739a64a3d6971eae24153053a003fb074aadf7e89da51b5930d10607a9a4`.
- Root verifier reverse restored `0b80cdf4eeb411407fcf4faca6a35462a71575c7497dc3fc0e6b39c676bbe9a1` from current `28c053d075970324387d33d86402156fe7bcc55225c03c769abe15c14d18e273`, preserving the retained_proc alias and exact closed 55000 forward correction as separately owned bytes.
- All eight copied complete owners independently matched extracted controlling saved sources. All SQL leaves/inputs passed the repository's official statement scanner. All three MJS files passed `node --check`. `node --test proposal.test.mjs`: **30 tests, 30 pass, 0 fail/skip/cancelled**. No generator was executed and no package was rewritten.
- Additional saved source-derived schema census hashes: CN/A0 lock fence `b840e89a0e378b466806b00af25cb1bb6d7643284991d6f7b7f75b17c4cbda16`; A0 native scope `35fe14245d88e324fe4242eea2733c60e43e1ccb001538b0dee8ace87f6a33dc`; creator schema/source audit `6ba738d1d1aeb05d10f760bc7d6b3f130100592d9803520ab9aac50b2617bedb`. These are source censuses, not installed/native acceptance. Earlier shared-owner SQL and saved Report schema receipts likewise do not certify this new trigger/codec's first use.

The verified 29 manifest members are:

| Relative packet file | SHA-256 |
| --- | --- |
| 01_schema.sqlinc | b0fcf6cf1ef5ba366a2c62deb2ddf11bdaf242e40cc88bc7b322bfc4eba15838 |
| 02_codec.sqlinc | dd735cdeabd778609eb30a4da2e852f57ba555e2fc029606966b699d14510008 |
| 03_capture_context.sqlinc | 71443e074b700730db85e6dc20e35e5d8acd38dee0365d39d81e136adf24fb14 |
| 04_receipt_insert_guard.sqlinc | fc27a20d6013d4a34574612f53e86588d6b6e3ecd1840fcd05b2ecba9a999c2f |
| 05_retained_amount.sqlinc | 7ed19c7ceab15a98d0e9ec75c7fd3f969d2dfa11e2625bbe873913a1889057fb |
| 06_complete_ordinary_apply.sqlinc | d6c408eaa4791ce967fa75c19bbc975c21f54da16801c01719260c4459feb9c0 |
| 07_complete_report_pay_authority.sqlinc | e1fdc46f3b8c97273236c6df09f62c4e947d1b252cb986bb4444613e5384e8c8 |
| 08_complete_registry_union.sqlinc | 72b0a2a14f8bfc9607df6cfd0a074390aaa02b6e0ddd6aec958f4db8a85ba3fb |
| 09_complete_final_acl_union.sqlinc | 60768cfcc1f98eda515407c0705f85f37381b55b178688c9ae98c4f5e0f80976 |
| 10_complete_independent_acl_verifier.sqlinc | d72a062d74fc6cd1cd53ffa43ba18e03896a8c4ccd72d217f59a8da31a2cb2a8 |
| 11_final_private_acl.sqlinc | 2be68072c40c0b0520f0a4664172f4f8c230324ac61a789ee0bbfefc0002239c |
| 12_proposed_repeatable_root.sqlinc | f42c8e858b02d4cc8a53773187b97319cd699fb717ea14e00d7242a155d530ff |
| 13_data_free_verifier.sqlinc | 8002ea36d365056771c6fa340210c38b54a504f46300969e2df32cf3954e0010 |
| CODEC_FIELDS.json | 87fae1af68fb75d23d1c625300abedcaadde1922bed02250213de96505b51fe5 |
| NATIVE_PROOF_RECIPE.md | 6e010d83bf8a3e20311d36dc0fc1f14ea8e54f86d11e2fcd897f4bf61bd6e8f1 |
| REVIEW.md | d9cd9c2bef616f338e25a2abe49df9fc384517651e27ffd221b150e44ace2379 |
| ROOT_INTEGRATION.md | 4a50778fc821f5925854137fd82492b60c69980f63b1e2c50fe8dce81a85ebbe |
| SOURCE_RECEIPT.json | 5ffb55dc568ffe4ccaf790f7b88f4805a0795f4cb6c43ed8b33cb56227cd290e |
| build-proposal.mjs | 4efb27933b4b305d15ca97c2a0b21ce40916d9b217cca1d6a59e5afe72b4012f |
| freeze-source.mjs | 47e74105c6e2a250cce5029a5128ba77436803326b366b1833351d4afb4bbae5 |
| inputs/bounded_writer.sqlinc | 3345f5ffd3c4ea69ab95b3d0903b798c6be6989705de075ddbfe6aa587d1fc2f |
| inputs/compact_report_witness.sqlinc | 9b7631caedc95d335cdddff4202e095b2b74b02b68c0546074aa1780c33b48f3 |
| inputs/expense_materialisation.sqlinc | 59705d5fb026c6245c1b0d9c26ba18ce2e4aa42057d019b6def19c19e0106e12 |
| inputs/ordinary_apply.sqlinc | f9e7809025b4bd47ea1b4d6e1021d47844d238de69e61fe40bb2de6ff6975a5b |
| inputs/receipt_assert.sqlinc | c2d638524b74808aaac59b13aa43a9801cfce95c6dedcf4bd659cbfb935255de |
| inputs/receipt_insert.sqlinc | 5140fbeb312d03f7d11b2a1f30804d5b4745b56b7ddb3ae1aae5debf18704a92 |
| inputs/report_authority.sqlinc | 36f7e5c89bf944b97539334cf4e8e6585244cb33df26a72a4b9e06d65e9fc97d |
| inputs/root_hash.sqlinc | f1fb653d96d1d0028f1c74df52750c3948c42f79ec32a2fbbf1a1a9ea49a46e7 |
| proposal.test.mjs | 58b996858321aa686326dab2ab01926382ec768f93be6a2e525153320881bab0 |

## Required holds and scope record

| Gate | Disposition |
| --- | --- |
| Native preparation/output normalization, 39 decimal and nine JSON text round-trip, zero, replay/history | HELD; source checks only |
| First-use SQL, temp ownership/forgery/collision/consumption, actual writer return/marker and trigger ordering | HELD; genuine native rollback fixtures required |
| Monetary active entitlement, native protection, C1/Direct NEXT/withheld/headless join and settlement distinctions | HELD; no competing amount/store/classifier added |
| Generated combined contract, final central/independent ACL, release ledger and installation | HELD; canonical 2300/2301/2302 unmounted |
| Report A/B admission, zero-pay/pay-isolation, real renderer/document checks and actual timing/F1 capability | HELD; retained native gates unchanged |
| Real authenticated API/PostgREST/Worker route, browser security negatives and deployment freshness | HELD; no route invoked |
| Office/MyTMS source release, TestFlight and physical phone journeys | HELD; no app work performed |
| Owner documentation/progress, concrete native recipes, exact source backup and future handback | Root-owned; this critic closes only source review evidence at the pins above |

Workspace/lane AGENTS, DATABASE_RELEASE_BIBLE, owner SEND_5/SEND_5B/SEND_7 and the Postgres best-practices skill governed this review. No separate money skill is present in the supplied skill catalogue; monetary boundaries were assessed against Policy X and the complete native source. SEND_7 ownership, release vacancy and post-Banking sequencing remain prerequisites for execution; this critic acquired no database turn.

Only this new report was written. Database/lab/API/Worker/business-network calls: none. Credentials/auth state read or secrets printed: no. Git mutation/fetch/push/commit/PR: none. Root, Bank worktree, frozen packets, generators and application source changed: none. Deploy/release/feature activation/payments/provider/settlement/email/drain/phone actions: none. Policy X drift introduced by this critic: none; the reviewed proposal has no post-Draft live finance fallback, economic-key ladder, pay origin or provider behavior change.
