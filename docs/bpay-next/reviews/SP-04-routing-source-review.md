# SP-04 routing source review

12 October 2026. Initial independent SOURCE review; native result UI and final callers remain under construction. No product changes, tests, SQL/RPC, fixtures or hosted activity.

Initial router SHA256 `fa88d8fc796e7e35a7420bfe93eb7c04264d58698bbe6475ac453e3a91e7d9cd`. Later physical-identity exclusion was read as saved source; this initial hash does not certify that subsequent version.

- Advisory availability exceptions are correctly ignored so receipt recovery remains reachable; S04-D01 closed.
- Original request carries exactly the four native server operands and success requires returned original signature/Timesheet/receipt UUID. Authenticated actor and NEXT status are retained. Exact rollback SQLSTATE/message mapping preserves unknown transport outcomes, and the existing guard-refusal recorder cannot alter the original error.
- **S04-S01: broad native refusal acknowledgement.** Current final branch accepts any <=200-character code with ok:false/withdrawn:false as definite Refused. An unsupported/malformed acknowledgement must remain UNKNOWN. Constrain to the actual finite owner/context/lock/receipt refusal contract, including its required types and native reason/nature; do not add a broad prefix catch.
- **S04-S02: incomplete successful acknowledgement.** Current success defaults missing/invalid canonical_timesheet_id to requestedTimesheet and forwards unvalidated native contract_week_id. Native success always carries canonical identity; validate its UUID and exact expected identity before presenting success. A malformed successful acknowledgement is UNKNOWN.
- **S04-S03: DAILY early exclusion not yet proved.** New lines60–67 return ordinary from requested physical DAILY alone before native receipt lookup. Existing Office routing excludes DAILY, but direct current first-authorise/core and self-bill approval route do not independently assert WEEKLY. Preserve receipt-first and use the existing native Office presentation fact for the narrow unresolvable contractless Daily case; see design review amendment. No direct owner-only root-authorisation history access.

Known native Source checks and stale W9 reasons remain the interpretation authority. No all-INTEGRITY-to-stale classification is justified. Ordinary currentness/correction handling must stay after Source receipt routing.

**Verdict: OPEN pending the finite acknowledgement and Daily qualification corrections.** Product UI/A4 and joined behaviour NOT_RUN.

## Final scoped SOURCE closure

Freshly read actual router `74c63f39beedffd7d8c7fb6a249f6441fffa98130efd202292d537852bfbeb52` and caller/index `f1121d3ba4fa70fd246de85a99617cee98581b231e940c4cc68c994c26400cf7`, against current design `c5105c44a77e72b4aa6bdf0f2679c780a839ea90cda7f9c7491ac814f3de09b4`. **S04-S01/S02/S03 closed; scoped SOURCE PASS.**

S01 now accepts only finite actual native check/context/gate/receipt branches. CHECKS validates eleven unique native W1–W11 rows and the exact nonempty failed subset; unknown/malformed acknowledgement remains UNKNOWN. Stale requires only the exact single failed W9 caller-operand reasons. Audited rollback messages require their exact SQLSTATE; transport/auth/timeout is never rollback proof.

S02 now binds returned requested ID/signature/receipt UUID and canonical identity exactly to original expectedTimesheetId. The nested successful native owner ID must equal that canonical identity, with contract_week_id null or UUID. Missing/mismatched native success remains UNKNOWN; no guessed canonical identity is accepted.

S03 now attempts the original native receipt owner before Daily classification. Only exact CANDIDATE_UNRESOLVED at original canonical identity plus native current DAILY presentation and exact physical contract_id NULL permits ordinary scope. Failed qualification retains the original refusal; unknown owner result never falls through. No owner-only history query or grant is present.

Fresh `runCorrectionPairLifecycleIfApplicable` at94011 runs read-only complete native-member qualification only after valid confirmed preview and before UNAUTHORISE bulk atomic. Selected-bulk at94668 has the same qualification. The distinct native membership cap is100; original selected atomic inputs/signatures/fingerprint remain intact; AUTHORISE caller at168570 remains unchanged. Managed/unavailable/unknown members block the ordinary atomic lane. Native single atomic1235_30:36–44/179–198 and final0556:2546–2558 prove that a managed Source single pair refuses transactionally, so its receipt-first request remains ahead of fresh pair preview. The earlier proposed inversion is superseded.

No SQL delta, business execution, tests, fixtures or hosted calls were made by this reviewer. This verdict covers router and reviewed Worker caller seams; final FE result controls/A4 and joined withdrawal, paid/history, Gold/background and lost-reply acceptance remain separately NOT_RUN.

### Final ownership-read amendment

Re-read final router SHA256 `64cb2495467daf7166d294f98bb6e4ee2e652d45380568081a55e70f5d07e0f7`. Its ordinary request now constructs UNKNOWN before ownership lookup and returns that same unknown result when the fresh status read fails. This correctly preserves a possibly committed original request during retry; an unavailable current ownership read cannot establish a definite refusal. It dispatches no new mutation while status is unavailable. Pair preflight retains its unavailable refusal before any mutation, which remains appropriate for that read-only qualification lane. The reviewed original operands, finite native contracts, receipt ordering and pair protections are unchanged. **Scoped SOURCE PASS retained** at the final router hash; index/design hashes remain as recorded above.
