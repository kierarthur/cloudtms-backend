# D10 actor-continuation successor — independent source critique, 14 October 2026

Verdict: **P1-D10-ACTOR1 source-closed in the exact successor; new P1-D10-FRAME1 OPEN. Overall bound-navigation source acceptance remains held.** Preserve the original report `BUILD_CHECKLIST_D10_REVIEW_ACTION_SOURCE_CRITIC_14102026.md`, SHA `699e4e7f2d9c455b1b114cdfa943031e5801e70e59fde985ee9bc276d8936f40`, and its confirmed 144e counterexample. No native, API/UI, financial, installation or final Part B certificate is issued.

Reviewed target is `8c1727a77f8840527afe8266ec928abb988df2ab` in `C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-checklist-native-14102026`. Later `db69ac3352dab8049e8d37ce0c464c08113bc001` historical WIP is outside scope. The frozen target's `docs/healthroster-build/proposals/checklist-native-20261014/SOURCE_PINS.json` was read from its exact Git object, SHA `c2ac9c29e0f15519c257a5b78071f3f178e2cfbf74a5855d7e2e8eed245004b5`. All 45 listed source files and 28 native dependency files match saved hashes and exact target Git blobs; all three external Office saved pins match. The later manifest is not substituted for this freeze.

Key successor hashes:

| Source | SHA-256 |
|---|---|
| `build-d10-review-packet.mjs` | `6102aa32997d6e9f824bb758992e4ffbb34db8f6f9e2becf711cd5e60c4f75c6` |
| `office-d10-review.forward.json` | `ddca160dcc59169b67cdd99763bbefc602039c16eb7afd0aa730a5d616e7024e` |
| `office-d10-review.reverse.json` | `50299bd619094270f116d1b61d9d9afe961b8033d7c10b90fb0dee200c10919c` |
| Whole `openReview` forward | `55687f0b568d8d404bed0ce44a396baa8a99f88b4fbb5f3cfe8007a8d153cfe8` |
| Whole `openReview` reverse | `8a758f06863993c4284670f437e41e265bfc8ccaaf4da83c2a74afe0ba192c68` |
| Complete LF Office review forward | `e0342aaba3dcc17ae827a51d86aafe9616f410d61f38e82c286fe63857fc10e5` |
| `tests/healthroster-checklist-d10-actions.test.mjs` | `e67b199f540e6c09c330164ef1ccbab891f3541157f14fc0ad7ca6722ae7d2fa` |

Ten unique full Office hunks and the saved forward/reverse inventories match the actual pure builder; both complete file reverses recover the pinned Office `2d15d121ac888a2e2a076dc7a2c4be85f9958556` inputs after the previously disclosed CRLF→LF conversion. Workspace action output stays `519db99c7f6862b3f3c1b8a76e7e1f8a85a32e87a6013e508e52e90d194735c6`. No creator generator or Office file write was run.

## Original actor-race disposition

The complete successor captures SESSION/user references, actor ID and immutable serialisations of roles and role/activity flags before the bound read. It rechecks those values after the actual header/proof, scope and action-page awaits and retains bound data locally until those checks pass. Bound polling/reopen and return/error callbacks propagate or enforce that context. The ordinary unbound branch retains its original behavior. Inspected whole-function deferred actor/user/session/roles changes refuse state publication, rendering and poll continuation, while unchanged and ordinary controls pass. The original scope-await C1→C2 race is therefore source-closed at this exact pin.

## P1-D10-FRAME1: unchanged bound navigation renders an empty child

Location: `build-d10-review-packet.mjs::forwardOpenReviewOwner`, final `showScreen` renderer guard. The callback requires `returnCallerCurrent()`, which expects `owner.reviewFrame || owner.caller`. On the first child open, `owner.reviewFrame` is still null until the subsequent `installReviewReturn(state.review)` call.

Actual pinned Office `js/main.js` executes the opposite frame order to the creator's positive stub: `showModal` pushes the new frame at line 333319, calls `renderTop` at 338635, and invokes the utility tab/render callback at 333661/329871. Its only preceding hydration await belongs to the Timesheet branch; the import-review utility frame reaches its renderer with the new child already on top. At that instant `returnCallerCurrent` still expects the parent, so the new renderer returns an empty string.

Independent in-memory counterexample used the complete composed actual `openReview`, actual composed `installReviewReturn`, a valid native-shaped C1 header/action, unchanged active Admin/session/roles and the actual push-before-render host sequence. Result: `renderReview` was called **zero** times, initial HTML was `''`, then the child was installed as `owner.reviewFrame` after the blank render. With no evidence-preparation item, no poll repairs that initial display. This is a positive path regression, not a changed-actor case.

The creator `wholeReview` stub currently calls `render()` before assigning `[parent, child]` to the modal stack. It consequently hides the real failure. Fix only the finite initial owned-child handover: admit/establish the exact native child before first rendering while retaining session/epoch/parent-token and unrelated-frame refusal. Preserve ordinary unbound rendering. Add complete-function positive and foreign-child/session negative tests using actual host push-before-render order.

All six inspected focused source suites report **83/83 PASS**, including the new whole-function race tests. That pass does not override the independent initial-child counterexample. No additional concrete P0/P1 was identified in this bounded actor successor.

Earlier native parent/QTARGET1 and retention/scalar source closures remain preserved. Historical real-witness preparation caller, R254 actual reminder writer, final PPv2 parent/target/page union, Bank/A0 owner composition, private/service classification and compiled contract, complete native graph/financial/process/action/role/API/UI/device acceptance and feature activation remain held. These are not qualified by source models or case counts.

Reviewer performed only bounded local/Git reads, inspected effect-free suites and pure in-memory source assembly/counterexamples. No DB, connector, generator, installation, publication, canonical/source/Git mutation, cleanup, Banking worktree or Banking message access occurred. Only this independent critique was written.
