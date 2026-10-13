# WB-29 W2 transport source review

Verdict: SOURCE PASS — 0 open findings. This is not whole-item, screen or runtime approval.

Scope: the new closed RESOLUTION_SAVE/CLEAR validators and original-request receipt on the existing workbench command POST, the FE headless journal mirror, and the native ENROLL wake. Product files were read only. No checks, SQL, RPCs, business data, hosted requests or mutations were executed by this reviewer.

## Finding and closure

- W2-T01: the first receipt validator checked carried old decision IDs only against requested owners in the same Work. A carry on another Work could alias an explicitly cleared/replaced owner. Root added the global `oldOwners` / `originalOwners` disjointness check in both mirrors; the saved source has been re-read and this finding is closed.

## Final source findings

| Check | Result | Evidence |
| --- | --- | --- |
| Closed request and common context | PASS | Resolution module lines 74–106: exactly the six native arguments, fourteen common context fields, Candidate/actor equality, nullable paired expected decision identity/version, SAVE intent versus CLEAR exact owner, at most 100 Works and 100 total rows, and 32,768-byte full RPC envelope. Manual decimals remain raw decimal text; no browser valuation is introduced. W1 loses no fields or validation. |
| Exact immutable receipt | PASS | Lines 108–147: exact request/Candidate/action/original epoch, received/replay semantics, sorted unique requested Work set, based-on revision equality, unique published revision and child command IDs, strictly increasing native agency sequences, exact submitted change set and version progression. Carries are bounded globally, advance versions once, and have disjoint old/new/requested owner identities across all Works after W2-T01. Historical processing/adoption is retained without comparison to a fresh offer or invented READY status. |
| Definite versus unknown result | PASS | Closed SQLSTATE/message pairs preserve known native rollback refusals. The workbench branch sets attempted only for a mutation; response/timeout/transport/unrecognised failures remain unknown. A malformed fault is conservatively rejected and leaves the FE pending request retained. No broad SQLSTATE-to-definite mapping was added. |
| Retained FE request and recovery | PASS | FE lines 246–310: agency/actor/Candidate journal key and original epoch inside the validated record; exact immutable request bytes stored before dispatch; authenticated actor/current-frame checks before send and after read; bounded UTF-8 response and 12-second client deadline over the unchanged eight-second Worker boundary. execute refuses an existing pending request. recover is read-only and never resends. Only an explicit retry after typed absent readback resends the original bytes/UUID. Readback errors and replay conflict retain the journal; typed received receipt or definite mutation refusal settles it. |
| Native publication wake | PASS | index lines 199163–199178: only a fresh typed received RESOLUTION_SAVE/CLEAR receipt with replay=false sends the existing `{id:'1',kind:'ENROLL'}`. No fabricated job/status or new queue kind. Failure records only the existing safe error code and leaves native recovery available. Existing retry POSITION_APPLY and other actions are preserved. |
| Static queue allowance | PASS | source-gates lines 2106–2120 change only the exact send census from four to five. The finite two-field envelope predicate, existing identity decoder, native ENROLL identity and financial continuation assertions remain enforced. This is not a broader waiver. |

The saved BE and FE validation mirrors were read after the global owner-conflict fix. No further essential source defect was found in this batch.

## Final SHA-256 evidence

| Source | SHA-256 |
| --- | --- |
| BUILD-BE broker/src/bpay-next-resolution.js | F07C8E08474D5C01708658BCB5EB102EE70D80C8661826F6BAE7D5DEAF1845EE |
| BUILD-BE broker/src/bpay-next-workbench.js | 715D3B419F5725845EEB814C6DD3D2A47DB6168B3E3E7DE07D0C01E1E5A82C6D |
| BUILD-BE broker/src/index.js | 7E5747350540B6CDAF644D8F47F17B53ED5A71FF0F0F44CD2E3D3B7B2777832B |
| BUILD-BE scripts/bpay-next-source-gates.mjs | 21C8677CEC3E7210DB02DDA3F13A099D661B7BD35D4A4D516F4C618F8B712375 |
| BUILD-FE js/bpay-next-resolution.js | 4457B1935F13F1CB5B2EDE9804C72719CEAE6FC1E88213717DFB476E5FFF10BA |
| Native SQL2320 (separate lock/source receipt; commit 8012c45b) | 1DD72479BA41C1997439F80B8A2546D285F29CD1F791BBDF28DC2FC615385CE2 |
| design/WB-29-w2-transport-design.md | 063B0EE0495DB2647BA85C3F0A253217F6103DF0C55AE49B1455E7387B886FA2 |

## Review boundaries

The native SQL2320 source and lock review is a separate receipt. This review covers saved source only, including the headless controller and exact native enqueue join. Actual rate-window mounting, screenshot policy approval, joined SQL/business execution, queue execution, later owner joins and the whole WB-29 item remain unapproved here. The reviewer did not execute syntax checks, source gates, tests, SQL, RPCs or mutations.
