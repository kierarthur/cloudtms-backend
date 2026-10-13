# Independent A0 creator source critic — 13 October 2026

Verdict: **USEFUL GUARDED SOURCE PROPOSAL; KEEP UNMOUNTED AND NATIVE-HELD.** The new SQL is a coherent adjacent proposal for the retained creator/phantom gaps. I found two concrete source-evidence defects, ranked below, but no demonstrated defect in the six proposed SQL helper bodies or the two guarded successor edits. This is an independent focused SOURCE review, not final Part B, installed acceptance, financial acceptance or operational green.

## Scope and exact evidence

Reviewed lane: `.codex-worktrees/healthroster-v2-protection-build-13102026`, branch `codex/healthroster-v2-protection-build-13102026`, saved HEAD **`eed87636a595990a6fe9d61bae5f8bce3dec6069`**.

Reviewed root input: `.codex-worktrees/healthroster-v2-backend-13102026`, branch `codex/healthroster-v2-backend-13102026`, saved HEAD **`b65df0997ac69188eadf4de2a88f70226fa99532`**.

Creator manifest: `docs/healthroster-build/proposals/cn-a0-phantom-writers/CREATOR_SOURCE_MANIFEST.json`, SHA-256 **`762f65ac35f11f1c33b779799854c1373eb0ccf9c624e44e6cebc70266cdb33e`**. Independently read and hashed all **19** artifact files (2,912,700 bytes), all **1,319** saved root input pins (98,640,102 bytes), and the four prior manifest pins. Every artifact/input byte count and SHA matched. Large census/body artifacts were consumed in full for JSON, graph, whole-definition and copy/hash checks; the critical native gate/I-1, trigger, signature, invoice and canonical context bodies were also inspected directly.

Applicable workspace/backend/lane AGENTS were read. Owner scope was reconciled from BASE:

| Owner instruction | SHA-256 |
| --- | --- |
| SEND_5_TO_HEALTHROSTER_BUILD_STAGE_NONSTOP.md | `62174e646395f01c199558ebc8f5bb1e04839c0127ab182e08fb207edd9bed91` |
| updated SEND_5B_TO_HEALTHROSTER_FULL_REHEARSAL.md | `e1e098b316cfa7e9dd2d416232b44dfbe47f18d697161e016889f4b0e3257c9e` |
| SEND_7_TEST_REHEARSAL_TURN_TAKING.md | `1d0f0a3d6d264422cac6288af4bb49aadff5ff86b7cecf89a1dae4b12bcb671c` |

No Banking worktree/chat read or message was made. Banking's first-install status was not freshly inspected by this reviewer; the supplied hold remains. No full rehearsal/release is authorised by this source verdict.

## Ranked findings

### 1. P2 — The “controlling writer” census contains provably stale/nonexistent declaration identities

Location: packet `census-first-creators.mjs:19`, `:35`, `:45`; generated `FIRST_CREATOR_CENSUS.json`.

The signature parser consumes declaration comments without masking them, and the definition map records CREATE/REPLACE without applying DROP FUNCTION. Two exact saved-source counterexamples are present:

- `public.timesheet_qr_restore_version` is reported as a controlling writer with `inputTypes=uuid,uuid,text,'pending' or 'signed' p_actor_user_id uuid` from `supabase/repeatable/16122025_timesheet_qr_refuse_restore_planned_delete_audit_rpc.sql:312`. The quoted text is the inline comment on the preceding argument, not a PostgreSQL argument type. This creates a separate invented identity, allowing the historical body to survive in the map even though the later `08082026_2035_timesheet_route_version_rotate.sql:2810` replaces the real four-argument function with a delegating wrapper.
- `public.timesheet_weekly_manual_adjustment_delete_apply(uuid,uuid)` remains in the controlling list even though `supabase/repeatable/06092026_2355_candidate_advanced_expense_completion_v1.sql:102` explicitly drops that overload. The current larger signature remains distinct.

The saved counts **73 controlling literal writers / 30 first creators / 233 historical occurrences** accurately describe the packet JSON, but the 73 cannot be called a complete effective final-owner census. I have not guessed a corrected count: additional DROP/RENAME/dynamic cases must be reconciled in the release order. This is a source tooling/evidence defect, not an observed installed application failure. The packet's existing dynamic/admission/installed limitations remain appropriate.

Action: preserve this frozen packet; produce a reviewed successor that strips declaration comments using a SQL-aware boundary, resolves canonical input type identities, applies explicit DROP/RENAME events in order, and compares final signatures to the generated catalogue. Add regressions for these two saved examples. Keep possible callees/dynamic text separate from proved invoked routine edges.

### 2. P2 — The booking trigger-order test checks function names instead of trigger names

Location: packet `build-creator-audit.mjs:33`; `tests/healthroster-cn-a0-phantom-writers.test.mjs:17`.

`triggerReview.push({...t,name,...})` overwrites the original trigger's `name` with its routine owner name (for example `private.bpay_source_timesheet_family_gate_v1`). The test then asserts `'aaa_healthroster_a0_booking_before_v1' < t.name`. PostgreSQL orders same-kind triggers by trigger name, so this assertion does not test the intended order. An offline in-memory counterexample renamed the saved trigger definition to `000_preexisting_family_gate`; the existing assertion still passed because the routine owner was unchanged. No actual source or database trigger was renamed.

The current saved ordering itself is correct: the proposed `aaa_...` Timesheet/invoice triggers precede the current `bpay_...`, `trg_...` and `weekly_source_...` owners; `zzzz_...` Candidate admission follows `set_tms_ref`, bank hash, revision and updated-at owners. Thus this finding weakens the regression proof; it does not establish that today's proposal runs in the wrong order.

Action: retain distinct `triggerName` and `routineName`, assert the actual parsed CREATE TRIGGER name for each event, and include the earlier-name counterexample. Reconcile enabled state and complete installed BEFORE/callee bodies at the native gate before installation.

## What the proposal supports at source level

The proposed SEND insertion is after actor/shape/request-key replay admission and before existing invoice/chain/workflow/Timesheet locks. Qualifying signed Weekly/no-ANY-FIN SEND uses the genuine immutable manager witness, the unchanged Candidate try-gate, and one complete native I-1 family acquisition under 1ms lock_timeout. It rereads family/chain/signature and checks the actual canonical context against the real Contract Candidate/Client. Existing-finance, Daily, agency and replay bodies reverse unchanged. The native gate has audit effects; it is not literally read-only, and containing-unit rollback remains required.

The creator admission takes all persisted request Candidate gates in UUID order, then one I-1 whole existing set, then NOWAIT request/parent intents. It compares the immutable request vector and family membership again and refuses growth/drift. An absent same-unit P0 may be omitted only while root/Original exist; genuine P0 allocation remains after request creation and uses native reentry. Timeout restoration exists on success and exception. Outer DML can already own its row before a BEFORE trigger, so the bounded refusals are essential; this is not a complete ordinary/Bank lock-graph certificate.

The new validation/raw-booking/Candidate precursor admission targets actual manager-signed Weekly Electronic Timesheets with a Contract and no financial row of any vintage. It takes actual Timesheet KEY SHARE and adds no new store, lock identity, resolver, pricing authority or READY evidence. The visible-active booking guard checks null/foreign Contract siblings and trimmed/raw collision identities. SEND also checks a conflicting sibling already committed before allocation.

The Candidate trigger covers INSERT, DELETE and every UPDATE, including an unrelated edit that changes ranking through updated_at. Its OLD/NEW key and JSON alias comparisons agree with the existing Weekly/Electronic canonical resolver's equality/containment; no Candidate fallback or resolver replacement is introduced. The actual existing BEFORE bodies do not directly rewrite protected booking/Contract/key/alias identities. Nested effects, enabled trigger order and late AFTER hooks still require the retained native graph proof.

The unchanged `public.invoice_line_archived_timesheet_guard_v1` already uses actual Timesheet KEY SHARE for an exact ID, or current raw-booking fallback when ID is absent. The new invoice trigger adds visible-active admission before this owner and deliberately omits a second ordinary precursor lock. `invoice_lines.timesheet_id` and `timesheet_validations.timesheet_id` have no Timesheet FK in the saved contract. The proposal does not invent either FK. The existing Weekly Source invoice family guard and controlled source-owner exceptions remain intact; a direct exact dangling invoice reference retains its native unresolved-family refusal.

All six new functions are private, owner `postgres`, SECURITY DEFINER with `search_path='pg_catalog','pg_temp'`, and explicitly revoke PUBLIC/anon/authenticated/service_role EXECUTE. Referenced physical fields were checked against the saved contract/request migration; no fictional Timesheet Candidate/family column is used. No new public/service operation, role grant, table, lease, identity namespace, calculator or financial writer is proposed. Root must preserve its complete 105-table/232-private/142-service registry union, add only the six owner-private signatures, apply the audited logical-owner mapping, and generate/verify the resulting contract and ACL.

## Reverse and preservation proof

- Complete request successor SHA `5d36d746d772f28083e032b72e6d8384c6295f5fe85643dabf4d28e0c9c78cfe` reverses byte-exact to current root request SHA `5fb1e628a66dd1a295e0b5533da5e02ff57f4e484d9ba52632bd64cc08f37675`. Independent `git apply --check` passed without applying the patch.
- Complete atomic lock-fence successor SHA `693ba3df876823767a9eb455f0646ea444d468470a83e49425c7316812282315` reverses byte-exact to frozen fence SHA `455768aaa41bf9d949a1c4f52830938a86c1ba231f45a95c72f772fc89b58f8f`. Its only edit adds the scoped non-RC refusal before the missing-A0 scope owner.
- All **170** prior lane manifest outputs remain hash-identical, including all **38** native-normalisation and **36** lock-fence outputs. Those two frozen packets also remain identical in root. Root's already-integrated `03_qualified_coverage.sqlinc` and `04_contact_retirement.sqlinc` are later root successors, so they differ from the older broad Protection packet copies; the creator manifest pins the current root inputs correctly. They must not be overwritten from an older frozen file.

Prior manifest identities:

| Manifest | SHA-256 |
| --- | --- |
| `docs/healthroster-build/PROTECTION_SOURCE_MANIFEST.json` | `2d0947db59f875da1fd4aeed4b49c6566174b19cc244c41785670248c3fd1070` |
| `docs/healthroster-build/proposals/f4-ai/F4_AI_SOURCE_MANIFEST.json` | `6de00fa2d72625b8b3d39b38b14dd4765ab76989ae297abfcf2c97b7ae5010a8` |
| `docs/healthroster-build/proposals/cn-a0-native-normalisation/A0_SOURCE_MANIFEST.json` | `f04bd3656846b5ba1e395095b64689fef839ada1e4dcc0102aa7f030cb16709e` |
| `docs/healthroster-build/proposals/cn-a0-lock-fence/LOCK_SOURCE_MANIFEST.json` | `0c1c69391c772ec5fda1b2945094fdd14128159e6514fa02772fcdc71383ec1d` |

## Holds that this review does not close

**Hidden-P0 repeatable-read is a release/exposure blocker, not an observed failed test here.** No saved-source guarantee of actual PostgREST/native RC admission is proved. SEND, the exact missing-A0 atomic fence and visible-active creator arms refuse non-RC, but an earlier RR snapshot can hide a later committed P0/WAITING request. Taking locks on unchanged signed Original/Candidate rows does not refresh that snapshot or change their versions. This proposal does not bump signed/issued timestamps/revisions, change global ordinary RR, or prove the hidden-request schedule safe. Root must qualify actual supported caller isolation/visibility or obtain a separately reviewed real native fence before exposure.

All five named native holds remain **UNEXECUTED / HELD**: VALIDATION_FIRST_INSERT, INVOICE_FIRST_INSERT, CANDIDATE_ALIAS_RANK, RAW_BOOKING_SIBLING, and NATIVE_FIN_LATE_HOOKS. The nine NATIVE_CREATOR_JOURNEYS cases are source recipes only. Genuine native first validation/invoice/booking/Candidate admission, two-session contention, self-reentry, request/family CAS, ordinary/historical negatives, complete late Banking/expense/FIN/document hooks, role-effective ACL and fresh rollback/restoration have not been run by this reviewer.

PostgreSQL compilation and first-use checks, complete root rollback install rehearsal, managed protected release, actual Original materialisation/reread, exact R0 sign-copy and P0 approval, financial vector and component equality, historic settings/rate/Source evidence, immutable documents, NEXT, actual Worker/PostgREST routes, Office browser patched-asset proof, MyTMS/TestFlight/physical-phone acceptance and feature activation remain unexecuted. They must pass on the complete reviewed root composition in the required Banking/SEND7 order. Source success does not waive Policy X, OR-44 or owner rulings.

Keep the creator packet, both complete successors, and the prior A0 missing-finance approval/Worker/producer compositions unmounted for executable release/exposure purposes until those gates are satisfied. Documentation retention and reviewed source composition can continue. Input-only body pages must never be separately enumerated release roots. No operational green or final Part B sign-off is issued.

## Offline checks and safety

Independently reran the five supplied Node source suites: **92 tests passed, 0 failed, 0 skipped** (17 creator tests plus 75 retained checks). Four packet generator syntax checks passed; no generator was executed to rewrite frozen files. Whole-body SHA comparisons passed for **25 native owners** and **30 copied first creators**. Both successor reversals, source schema/name/security checks, 1,319 exact root input pins, all 19 artifact hashes, prior frozen-file preservation and the dry patch applicability check passed. The two source-evidence counterexamples above were demonstrated with read-only/in-memory checks.

Only this new critic Markdown report was written. Root/lane/frozen source and pre-existing dirty files were preserved; both reviewed source worktrees were clean at initial and final pin verification. No commit, push, PR, database/Worker/browser/API/network-business/Docker call, SQL execution, TEST mutation, payment/provider/settlement/bank/email action, release or deployment occurred. No credential/secret file or value was read or printed. No LIVE resource was accessed. Policy X/economic behaviour was not changed.

## Artifact identities read and verified

Paths below are relative to the packet unless the tests path is shown. The manifest itself has the separate SHA stated above.

| Artifact | Bytes | SHA-256 |
| --- | ---: | --- |
| `13102026_2153_a0_native_lock_fence.complete-proposed.sqlinc` | 12314 | `693ba3df876823767a9eb455f0646ea444d468470a83e49425c7316812282315` |
| `13102026_2153_native_creator_intents.sqlinc` | 19166 | `53c4301a9b556e91358287f23654e5284877e7577ead7561197b2efb2a5ea4a1` |
| `13102026_2153_request.complete-proposed.sqlinc` | 23160 | `5d36d746d772f28083e032b72e6d8384c6295f5fe85643dabf4d28e0c9c78cfe` |
| `13102026_2153_send_family_fence.sqlinc` | 5935 | `e904da7a8cebab76a3baba0e04b3fa5d64ee4cb4d67e27f2ce0062c5e58aacae` |
| `build-creator-audit.mjs` | 7719 | `308d0911f2b3815b7b47054c8c00c59b9ec1499d303214d5cee123ff69c2ddd0` |
| `build-creator-successors.mjs` | 3417 | `8235ba5255e7f4d5b7cf9f1fc6753e4f23046fe17e833dceeeeec4385e5b6d42` |
| `census-first-creators.mjs` | 9834 | `9a3b75dc724e5dee65e1eb2555a9945087acc1b1174dca63eb5efaf3218a2e1a` |
| `CREATOR_SCHEMA_AND_SOURCE_AUDIT.json` | 28554 | `6ba738d1d1aeb05d10f760bc7d6b3f130100592d9803520ab9aac50b2617bedb` |
| `FIRST_CREATOR_CENSUS.json` | 1010036 | `e76c197f62c2e86e2a1b10fc1e50d4705d823fa0e9c9f452914de671fe9b8e0b` |
| `freeze-creator-packet.mjs` | 7313 | `0dea704ada297ba9ca94011acd7ad454c6e9b3f9797eb952b5c78eccad5e16ab` |
| `NATIVE_CREATOR_INPUT_BODIES.sqlinc` | 80981 | `77d58af59c959e5d8d49fb589fa79a86c993e3914616bb8d67cadeea4ca128fc` |
| `NATIVE_CREATOR_JOURNEYS.json` | 6836 | `8a6161b26e9f0d8db10b51cc7ba3e54f00d1fd3bcebf03256be08b4bddcfe571` |
| `NATIVE_FIRST_CREATOR_BODIES.sqlinc` | 1669956 | `67e585a45dc6874ebe62ef08187f762a7e1056e6da87489dfa497afc399b8e9c` |
| `README.md` | 6760 | `07cd015a6124f0ec0aa0177992c8a083559e3aabb841bf3ea7c6a3d45cc612bd` |
| `REQUEST_ROOT.diff` | 955 | `719e5cff4a07204c13711beb940835fd085f61d8dacab20d9c6e2beca32c3bdb` |
| `ROOT_INTEGRATION_RECIPE.md` | 3609 | `af5c1c77d93010f85fbf3db5fc39049ec3777ff93fd3ff52e31abd938d51c72a` |
| `SOURCE_CHECKS.json` | 4374 | `2b6bd4b5732dd7eb4933190d301e9ec3104465faf588eb506413cef9a1680373` |
| `SUCCESSOR_GUARD.json` | 1805 | `21dbc9fcafef7dac45878e827100fbd3ba210412b29901f44801b38d7c9331c8` |
| `tests/healthroster-cn-a0-phantom-writers.test.mjs` | 9976 | `0fba84f5707fd3ed55819c59db0c2574169e2be7f3f19e5173397d6edc307d05` |
