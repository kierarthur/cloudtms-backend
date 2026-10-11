# WB-39 transport and management source review

12 October 2026. Independent read-only source review by Sol 6.1 Extra High. Scope: BD129, the approved WB-39 card (02_WORKBENCH.md:1401–1426), existing SNOOZES GET transport and the three management handlers. Product, database, business actions, tests and captures were not executed or edited. Visual confirmation and joined acceptance remain pending owner evidence; this receipt does not close FC-02 or claim its renderer/sort work.

## Current verdict: findings open

**W39-T01 — Unknown CLEAR cannot reopen after native acceptance.** In `js/bpay-next-snooze-actions.js:76–78,93`, `unsnooze(id)` requires an active native point row before the original shared window can load its retained journal. The current SQL row and page owners (`12102026_0041_banking_pay_next_snooze_management_seek_v1.sql:13,103,116`) exclude inactive controls. If CLEAR commits and its reply is lost, closing/reopening yields zero rows and fails before the exact retained CLEAR can be read back. Reach the existing journal/receipt for that exact native control after acceptance while refusing a fresh clear of an inactive control. Preserve the original request UUID, arguments, bytes and owner/actor namespace; no new financial command is needed.

**W39-T02 — Changed existing assets retain old cache versions.** `index.html:3024,3037` still references the prior snooze-window and Workbench versions. Cached Workbench code lacks the specialized SNOOZES branch; cached shared window code lacks the management context and will follow current-revision admission instead of clear-only management. Bump both changed asset URLs with the new actions asset.

**W39-T03 — Native owner refusal is rejected by the common decoder.** The new Worker branch (`broker/src/bpay-next-workbench.js:667`) marks MODULE_NOT_ACTIVE `reload_required=true`. The retained common refusal validators (`BE:588`, `FE js/bpay-next-workbench.js:587`) permit true only for CURSOR_STALE, so `createWorkbenchClient` converts this known native refusal into RESPONSE_INVALID. Preserve an envelope admitted by the existing decoder, or change only the specialized condition consistently.

## Confirmed source properties

The specialized GET stays on the existing fixed Workbench RPC and trusted admin actor. Duplicate query parameters, extra keys, bad limits and foreign actor cursors are refused. Input uses the original 32 KiB envelope, response streaming is capped at 128 KiB, and the reader has a four-second deadline. No Financial STEP, new route or command is invoked by reading the list. Unrelated Workbench shapes and dispatch remain retained.

BE and FE reader contracts agree on the specialized scope, closed row fields, bigint string versions, active state, actual Work/Case identity pairs, native recovery kinds, filter qualification, strict cursor echo and deterministic Created/time plus UUID ordering. Ordering compares all six native microsecond digits rather than JavaScript millisecond truncation. Current/applied/supplied label authority remains in the native reader; labels are not calculator inputs. Amend refuses a withdrawn/noncurrent Work; Unsnooze uses the original exact CLEAR under a clear-only management context. New Case snooze opening admits only Payment Advance or Manual Debt kinds; carried-over Overpayment management remains actual-kind based, and Credit is absent.

The shared journal remains version 2 and keeps its original TEST/agency/NEXT epoch/actor/typed scope namespace. Retained requests are checked against Candidate and exact native snooze ID before use; original readback precedes any identical retry dispatch. A management window rejects SET, warning and MANY paths, and cannot replace a pending request with a fresh CLEAR. Shared confirmation semantics state return to Banking Pay for the next Draft rather than promising Ready. FC-02 remains the sole list renderer; other displayed sort keys must be added at that immediate next owner, as BD129 records.

## Reviewed bytes

| File | SHA-256 |
|---|---|
| BE broker/src/bpay-next-snooze-reader.js | `f72ddc5b6f2d68aa1a5b295524c3f0606313af26972b3f2fe25a07c04d276e7d` |
| BE broker/src/bpay-next-workbench.js | `33e02a50905144e30124c213c6d204d75cf84bdb00008197949141b2572a0c4a` |
| FE js/bpay-next-snooze-actions.js | `644b1d1f676b28e1db85459bb9b2cbc04438132f89eba8e4de01c54fb1bf35c1` |
| FE js/bpay-next-snooze-window.js | `096bacfbd44f5ba9bff907374f4201f7a6d1c6fdb7e35ea4579498a6fe0d31ee` |
| FE js/bpay-next-workbench.js | `b1259f048cb63fb1e8f385a46edf5b93751d358e9f7759f11211bca9afeb0d18` |
| FE index.html | `543d0c3b3bea1b5e4eff62eb25a7319fe66d8e0d010aadf4a6ef2113be44f2e8` |
| Native reader 12102026_0041 | `b5d44ec2eaf41c1a13c716b0fba973f5d30d7a60cd5acf980fda9d1a5e5194fe` |
| design/WB-39-build.md | `a99ef68b5a9bb06b99239f8cb92a91580a0d4eb1391919448a4aa531e067463f` |

Final source closure requires rereading fixes for W39-T01–T03. No broader proof, label projection, producer change or future renderer is required by these findings.

## Final source amendment — PASS

Fresh saved bytes were independently reread after all three fixes. This section supersedes the open verdict above. No essential transport/management source defect remains in the selected WB-39 boundary.

W39-T01 is closed: the exact-ID native point can return an inactive control; ordinary lists remain active-only. Both finite response validators admit inactive rows only for the exact requested snooze ID, require `can_amend=false`, and require `can_unsnooze` to match active state. The management context now carries the native active boolean. Without a retained request an inactive control has its fresh Unsnooze action disabled; with the original pending CLEAR it can reach the unchanged receipt-first replay using the original request/arguments and journal namespace. Amend still refuses both inactive and withdrawn authority. No replacement request, new command or storage is introduced.

W39-T02 is closed: shared snooze-window, new actions and changed Workbench assets all use `20261012-next-snooze-management-r2`. W39-T03 is closed: MODULE_NOT_ACTIVE now uses `reload_required=false`, admitted by the original shared refusal decoder; CURSOR_STALE retains its existing reload behavior.

FC-02 handoff remains explicit: its renderer must reconnect the existing actor/agency/epoch/scope-qualified pending CLEAR journal to `unsnooze(id)` exact point recovery after reload, including a control that no longer appears in the active table. It must also supply all actual displayed column sorts and indexes before FC-02 completion. These are the immediate next renderer's integration obligations, not a claim that WB-39 has built or accepted that screen. Shared confirmation A4 and joined/business behavior remain outside this source-only verdict.

| Final file | SHA-256 |
|---|---|
| BE broker/src/bpay-next-snooze-reader.js | `aa1c36eeacf558f551b4743e75f9451e5d37f67cd05718c79e9592b7f07ddbf8` |
| BE broker/src/bpay-next-workbench.js | `a7ccc4e563a48b8dc51f56496e065040f24780d240af359e78960652d08f8e15` |
| FE js/bpay-next-snooze-actions.js | `1a58a2158e46296d3c76297ebaca6041c3333bc981ce287e4322d82f47e5bc6f` |
| FE js/bpay-next-snooze-window.js | `77d342849721785217abd0f9450981d5e835a1c01ec508d9b6601b079eea4135` |
| FE js/bpay-next-workbench.js | `b1259f048cb63fb1e8f385a46edf5b93751d358e9f7759f11211bca9afeb0d18` |
| FE index.html | `3a4352caf008a9b3b54bdc95230ebb8f5ed66bf3a5adab98f984feeb50c52127` |
| Native reader 12102026_0041 | `7ac799d177f99cf912b3021dd6bb6ce3ebc5b90b7c13c88add4c0100f1c0c0b8` |
| design/WB-39-build.md, unchanged | `a99ef68b5a9bb06b99239f8cb92a91580a0d4eb1391919448a4aa531e067463f` |

### Final native point packaging amendment

The last native source split was reread: 0041:101–110 now handles exact-ID lookup in its own point branch with all supplied Candidate/Client/scope/date filters, while the ordinary two directional list branches retain literal `s.active` for their partial indexes. Point completion is true with a null cursor. This preserves the reviewed inactive CLEAR recovery and active-only list contract. No FE or Worker bytes changed after the source PASS above. Current native SQL SHA-256 is `58d05480da8e138d89233044e985b34782035e86d0cc16e271d237afd0a8a599`; the appended BD129 handoff note is now `5d32f184a2ea578b9250de3eed46a9ddeb5a8ef0e94b926a65604b32bc27bb7a`. These supersede the older SQL/design hashes only. SOURCE PASS remains; no business or joined acceptance was executed by this reviewer.
