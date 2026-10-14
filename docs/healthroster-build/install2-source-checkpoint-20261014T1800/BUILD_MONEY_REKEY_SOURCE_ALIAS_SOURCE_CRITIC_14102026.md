# Money rekey Source alias — independent source critique, 14 October 2026

**Disposition: P1-REKEY1 OPEN.** The frozen identity repair has a concrete foreign-alias ownership hole. Preserve this freeze and supply a separately reviewed successor before composition. This is a bounded source review, not native, financial, API, UI, installation or final Part B acceptance.

Reviewed saved proposal:
`C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\healthroster-v2-money-build-13102026\docs\healthroster-build\proposals\rekey-source-alias-current-native-successor-20261014`.

Own local HEAD and exact predecessor Git objects: `a67a44319e25e136fa34c61fb6ca4d270d85ceda`. No reviewer remote query was made. Current native input objects: `33927fff2f7b90b90241d746d9e35aa21c57bd07`. Primary `03_IMPLEMENTATION_PLAN.md` SHA256 `b9f5b95393bf2f4c338da8110895dc7f80cb896f389aa0f27ea1c01468aef2d3`, particularly §6.1. Adjacent `proposals/MONEY_REKEY_SOURCE_ALIAS_REVIEW_HANDOFF_14102026.md` SHA256 `d0f624c1556fa395732a6670d907d6b39a9a18fb4381ae5dc6cf795dca692e6d`.

## Exact measured pins

| File | SHA256 |
|---|---|
| `FROZEN_ARTIFACT_MANIFEST.json` | `44706e1eef850a647458d1af77e4a860520ca60d68801b1b91f88bae6135be07` |
| `SOURCE_CHECK_RESULT.json` | `4d7afb2a3ab7b4095bb74a71629cdc15ec169155091adb1fd705edcf58653851` |
| `forward/00_known_source_key.sqlinc` | `6cf09e5142613772b8af399099e59cc480715f3161cccdc88789787ee4771145` |
| `forward/01_complete_identical_rekey_prior.sqlinc` | `7243e1b99f016f22efcf5b1bbaa1a061bb2fc6f2e621c4ca24bbe0368f397eaf` |
| `forward/02_complete_upload_context.sqlinc` | `1a23b8da7645ed0ce0482ed1647165076c7b0d2747dba1dee3fe2c95d417e4df` |
| `forward/03_complete_projection_rows.sqlinc` | `14f01e7ca69c1f1ada8720194d0aa7311e2b9bd408f6865e9e563b4505a1fe60` |

All 15 listed artifacts match frozen lengths/hashes. All three complete existing owners reverse uniquely through four hunks to both saved predecessors and exact a67 committed bytes. The three schema/projection/context input pins match actual 339 Git bytes. Original Keep32's 32 artifacts and the Source39-containing original OFF98 artifacts remain saved/manifest/a67-Git identical. No creator or frozen file was changed.

## P1-REKEY1 — foreign aliases are removed before ownership and ambiguity are measured

In `forward/00_known_source_key.sqlinc`, function `private.weekly_source_known_source_key_v1(uuid,uuid,uuid)`, the event join at lines 32–34 applies `event.candidate_id=p_candidate_id` and `event.client_id=p_client_id` before `count(distinct event.id)`. Thus a genuine literal-key alias owned by a different Candidate/client becomes indistinguishable from an entirely new key. The canonical-key precedence check at lines 21–23 cannot close this hole: the event deliberately retains its original key after a rekey.

Concrete source counterexample:

1. In group G/profile P, native event E1 retains canonical key A, Candidate C1/client L1. A genuine A→B rekey has produced the actual RESOLVED row, immutable Source link and correct native link hash for B→E1.
2. A later finalised B row names Candidate C2/client L1. There is no canonical event with key B. The new reader filters out E1 and returns null.
3. Give C2 a genuinely eligible event E2 with another absent canonical key C, exact date/start/end/break and the same qualified Contract as the incoming C2 row. The unchanged identical-rekey helper can return E2. Alternatively, with no such prior event it returns null and the ordinary new-event branch remains reachable.
4. The context at lines 406–423 accepts that fallback. The projection at lines 587–603 repeats it. Its conflict guard at lines 613–623 searches only `other_event.profile_external_key=B`, so it does not see the real B→E1 alias. It can attach B to E2 or create canonical B for C2, splitting an established Source-key identity across Candidates. The same problem occurs for a foreign client.

Even if both local and foreign B aliases already exist, the reader can return the one local event without raising its advertised ambiguity refusal: the other event is filtered out before counting. Native unique profile-key and durable-identity constraints cover canonical keys; the existing Source-link unique keys cover row/resolution and full link hash, so neither prohibits these distinct B links.

Smallest source correction: first census **all** genuine hash-valid literal-key owners in the same group/profile; refuse a foreign Candidate/client owner and refuse multiple distinct events; only then return the single exact local owner. Preserve ordinary canonical precedence, Contract reproof, genuine-source joins, actual SQL refusals and the frozen predecessor. Add actual source-selected foreign-Candidate, foreign-client and local-plus-foreign alias cases through both context and projection, including both row orders. Coordinate the selected immutable successor with the root; this report does not approve shared owner changes or mounting.

The inspected creator test explicitly expects a foreign alias to return null and does not follow that null into the genuine new-key/projection path. Its nine passing cases therefore do not close this finding. Four independent in-memory, source-selected predicate cases confirmed the exact-local positive, hidden foreign Candidate, hidden foreign client and hidden mixed-owner ambiguity. Those are source predicate reproductions, not PostgreSQL execution or native hash qualification. Finding sent promptly to root and Money author.

## Other bounded observations and verification

Repeated B resolves the genuine stored alias. For B+C and C+B, the new-key helper's whole-file absence predicate sees canonical A, latest raw B and all hash-valid prior Source references; C therefore cannot reuse that event while B is present. Genuine C-only rekey still requires the original finalised/complete-coverage and full Candidate/client/Contract/date/start/end/break proof. The independent projection reproof and latest native Contract comparison remain; continuing known B identity can carry changed values into the existing economic owners rather than asserting unchanged pay itself.

Actual 339 schema fields, link producer/hash domain and native canonical-key collision branches were read. Stored links/resolutions can arise during native projection before final economic acceptance. This reader proves recorded Source identity only; it does not establish accepted final authority, paid status, invoice membership or a financial no-change result. Existing calculator, VAT, expense, Source authority, policy, qualification, worked/unfinalised boundaries and Report delegation bodies outside the four exact hunks are retained. No new table, column, public signature, service/browser grant, alias store, event-key rewrite, route, managed caller or Banking body is supplied. The one new helper is stable SECURITY DEFINER with fixed search path, logical postgres owner and PUBLIC/anon/authenticated/service_role execution revoked; effective final classification and installation remain unproved.

Inspected and rerun `source-alias.test.mjs`: **9/9 source cases PASS**. Three inert JS syntax checks PASS. These include lexical SQL/schema checks and small identity models, not SQL compilation. Native alias birth, namespace races, RC/RR visibility, explicit Separate and exact current caller composition still need their actual engineering/proof; a read-only helper or scope mutex alone is not a concurrency certificate.

Full paid-overlap confirmation, genuine ordinary/Source paid grain, Same/Separate and `pay_effect NONE` footprint, payable versus invoice-only snapshot/manifest/prepared consumers, Office/phone and financial proof remain real missing implementation or qualification work. Shared13 kernel/capture/inventory proposals remain unapproved and unmounted. Source39, original receipts, the three bundles/exceptions, PolicyX/OR44 and two-install rulings remain controlling. No native call, source publication, root mount or full acceptance was performed or inferred. No additional P0/P1 was found in this bounded slice; **P1-REKEY1 remains open**.

## Separate corrective10 review — P1-REKEY1 source closure

**Latest disposition: P1-REKEY1 CLOSED at source for the selected corrective helper only.** The initial finding and defective frozen15 bytes above remain preserved. This closure does not certify native concurrency, financial behavior, mounting or completion of the paid workflow.

Reviewed separate saved proposal:
`C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\healthroster-v2-money-build-13102026\docs\healthroster-build\proposals\rekey-source-alias-all-owner-successor-20261014`.

| Corrective file | SHA256 |
|---|---|
| `FROZEN_ARTIFACT_MANIFEST.json` | `400b1b49fe5534b79baccbea9a9e3b7017458b40c84afeb66ebafedced92ff5c` |
| `SOURCE_CHECK_RESULT.json` | `1a8604c53c669c319934264c55714f51431796e72db60e2f1066294f8eac5015` |
| `forward/00_known_source_key.sqlinc` | `3efed0771093465c90faed992811b660b8a6f885bcbe7bfd17adfc36eb16d56d` |
| `COMPLETE_OWNER_REVERSE.json` | `c1ce1df034d13cf579cc9af8176555c043318a0a62c7c08884e672e56c27b8fa` |
| `all-owner.test.mjs` | `ffd11e7d42fc7b35387b32b1cce8657b78ec2f608a9395899193a5e6f1006a2f` |

Adjacent `proposals/MONEY_REKEY_ALL_OWNER_REVIEW_HANDOFF_14102026.md` SHA256: `e42fac07034cc8c1d3494f903c6a83632bad1f94185789d90d5d5a006f9d3758`. The current-source and predecessor identities remain the exact a67/339 objects stated above. No reviewer remote, publication or native operation was performed.

All 10 corrective artifacts match manifest raw lengths/hashes. The complete 4,123-byte helper reverses through four unique literal hunks to the exact 3,920-byte `6cf09e…` helper, both corrective `before` and unchanged frozen15 input. Independently reverified all 15 predecessor artifacts and its exact `44706e…` manifest. The three selected predecessor leaves retain their full raw reversals to the exact a67 committed helpers/callers, and the actual 339 native input pins remain exact.

The corrected alias SELECT retains every literal group/profile/key, resolution/event Candidate/client consistency and native link-hash join, but removes the **requested** Candidate/client filters from the join. It aggregates `count(distinct event.id)` across that complete genuine namespace, alongside `bool_or` of requested-owner mismatch. Multiple distinct owners raise the original `WEEKLY_SOURCE_SOURCE_KEY_ALIAS_AMBIGUOUS`/`55000` before selection. Zero owners still return null. Exactly one foreign Candidate or client raises `WEEKLY_SOURCE_SOURCE_KEY_ALIAS_OWNER_CONFLICT`/`55000`; a unique local event returns its UUID. Repeated links to that same event do not create false ambiguity. The min-ID aggregate supplies a value only after the one-event check; it cannot select among multiple events.

Both actual full callers were reread. Upload-context's `55000` catch permits only the two existing ambiguity messages, so the new OWNER_CONFLICT is rethrown and never reaches identical-rekey fallback. Mixed-owner ambiguity keeps discovery's null prior/count-two response; projection independently rederives it and raises. Projection's outer exception handler only remaps invalid text/numeric conversion errors, so both alias `55000` refusals propagate. Its existing canonical-conflict and full latest Contract reproof remain unchanged. Both incoming B/C row orders refuse the foreign B owner before accepting a new-key fallback. Event creation and affected-row resolution/link insertion are downstream; existing policy snapshot insertion and earlier rows may precede this refusal, but the preserved handler does not swallow it. Actual rollback/effect closure is not established by this source test.

Inspected and rerun **15 corrective source-selected relational/caller cases plus 9 inherited cases: 24/24 PASS**. These reproduce the original foreign-Candidate/client null-to-fallback defect and show corrected refusal through both caller branches in both orders; cover local-plus-foreign, all-foreign and same-local-owner multiple-event ambiguity; retain local/duplicate-link positives, no-owner fallback, canonical precedence, eligibility order, different group/profile/key and invalid hash/resolution negatives. The test predicate interpreter selects comparisons from actual saved SQL; its synthetic link hash and small relational facts are explicitly source scaffolding, not native PostgreSQL JSONB hash proof. Three inert JS syntax checks PASS. Four-hunk complete helper reverse and the inherited three-owner/four-hunk reversals PASS.

Select corrected helper `3efed077…` followed by frozen15's identical-rekey/context/projection leaves in `SELECTED_SOURCE_RECIPE.json`; exclude the defective `6cf09e…` helper from executable composition. Signature, stable/security-definer mode, fixed search path and owner-only ACL are identical. No additional signature, table, column, store, grant, calculator, event-key mutation, Banking body or public transport is added by corrective10. No further P0/P1 was found in this corrective scope.

All earlier stated full paid-grain/overlap/Same-Separate/prepared/Office engineering and actual alias-birth/RC/RR/security/financial/API/phone/classification/contract/catalogue/native gates remain held. Source39/Keep32/OFF98 and immutable receipts/bundle exceptions remain controlling. Root's separate native fixture and any mount or publication are **outside this closure**. The historical original finding above is not relabelled as a passing original implementation.
