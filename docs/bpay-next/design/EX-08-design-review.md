# EX-08 independent design review — sequence102

11 October2026, Sol6.1 Extra High. **DESIGN PASS — required precision closed in the supplement reviewed below.** Read-only review of BD114, the complete EX08 card318–342, all checklist rows2833–2858, master order/dependencies, owner OR44/46 and current investigation. No product edit, SQL/RPC, business data, fixture, test, hosted action or screenshot was performed. This note marks no item/checklist or application acceptance PASS.

## Exact authorized boundary

D01/D02 current valuation-policy owners remain untouched. D03/D04/D05 add only named write-free preview/current-fact apply owners and their smallest immutable operation/scope support. D07/D10/D11 use the existing GET/POST paths behind exact NEXT ownership exemptions; D09 reuses the existing Candidate modal. D06/D08 baseline transport/queue evidence introduces no new command lane or job. Sequence102 follows FC03/FC16 and precedes FC01; WB29/WB45/CM11/CM19 remain named later consumers.

The design correctly permits a future Candidate pay-method/Umbrella change while an earlier accepted Draft remains unpaid. Only current candidates.pay_method/umbrella_id are changed; accepted selection/rates, Work/Case instructions, held slices, original destination and reissue creditor stay frozen. Strict current-fact input rejects bank fields or requested captured-object rewrites. Protected Case identities can be described through FC16 bindings without invoking a blanket Case-change assertion against the entire Candidate change. An unchanged channel with a genuinely different Umbrella is valid scope.

Shared native quote/valuer and approved tax classification remain the money authority. Reimbursement is not converted for a channel change, and Additional retains taxable channel-sensitive treatment. Source identity or a current Candidate company cannot fabricate an original Work creditor. Contract-move comparison work is excluded from this preview. Compatible old-to-target work is not automatically an action requirement.

## Required precision in the same design note

1. **Finite complete scope.** Name the maximum affected native members/encoded bytes, preview page limit/byte envelope and closed cursor. The current phrase “complete maintained Candidate scope captured set-wise” does not itself bound Apply. Use indexed current membership with an explicit overflow refusal before any Candidate update or accepted receipt; no silent truncation, history scan or synchronous whole-Candidate revaluation. Include both protected and future portions where one native component has both. Future consumers must receive exact Work/revision/line-or-segment and Case/component/revision identities, source/target/tax/policy operands and compatible versus exact unresolved disposition, rather than a generic browser scope bag.
2. **Native accepted ordering.** Name the retained pre-change context and post-change native metadata_version, plus the existing METADATA task identity/generation or equivalent exact native receipt facts. The original trigger advances metadata_version and task generation in the same transaction; reuse those actual fields. Successive A→B→A or U1→U2 changes must not choose precedence by equal transaction timestamps, random UUID order or current source/target mismatch. Capture the accepted typed result immutably so identical unknown retry returns the original result after the Candidate has changed. No new counter/protocol is needed.
3. **Closed context and replay.** Specify exact request/context keys, nullable semantics and raw string/UUID equality rules, with actual native actor/Candidate/module epoch, current method/company, metadata/FVR and policy/native revision operands consumed by classification. All preview pages must qualify that same context; changed target or page drift invalidates Apply. Under module → Candidate → worker-control guards, replay of the exact operation/actor/original bytes must be checked before fresh context rejection; altered reason, target/company, context or intent conflicts. A new request then checks currentness and the original policy SHARE owner. A preview never obtains those business locks or calls the mutating metadata authority.
4. **Current projection versus later resolver.** Remove any wording that implies existing METADATA_GROUP already consumes EX08 conversion decisions. Current0426 has no accepted EX08 lookup; it classifies native residual/accepted/H/R portions. Retain it and the current classifier chain, with WB29/WB45 as the last chain editors and sole saved-resolution consumers. EX08 emits complete immutable bindings and schedules existing bounded metadata work; exact unresolved action/admission and shared accepted quote consumption in the named later owners remain explicitly NOT_BUILT/NOT_RUN until their cards. If any card-required current support must change, name that exact supporting lookup and its owner before authoring; do not add a new resolver or silent duplicate rate decision.

These are implementation details still needed in BD114, not missing authorization and not a request for new framework, money policy or future card implementation.

## Reuse and source evidence

- Current metadata writer/trigger:10102026_1429:151–196. It updates current target metadata and increments native metadata_version, then schedules METADATA once. The original task stores generation/epoch and native cursor/through-key. Apply must prehold Candidate control before the Candidate UPDATE that invokes this trigger; do not enqueue the same metadata task again unconditionally.
- Native bounded continuation:1429:335–341,353–359 and373–387 discovers at most100 maintained groups/summary keys and preholds Candidate → Work → Case before task locks. Exact task epoch/generation/cursor checks govern replay. The refresh-many wrapper1438:354–409 already preholds whole-page domains and bounds100 identities/16KiB.
- Current Work core0426:50–59,124–163,181–200 reads current target and native member/residual/accepted facts, preserves Ready by default and protects accepted/H/R portions. No EX08 receipt-resolution producer is present.
- Shared pure target valuer1157:932–990 uses actual inclusive dated policy authority and the existing quote. Legacy public.pay_finance_component_resolutions_apply writes legacy Case components and is not a NEXT receipt store.
- FC16 current1509 bindings provide typed active holds and pending accepted scope, with NULL protection/capacity truth where not allocated. They cannot be replaced with unpaid-status-only refusal or invented zero.
- Existing legacy route handlers reject same-method changes before their old logic and accept bank fields. NEXT must branch before those legacy-only validations, enforce the exact closed current-fact patch and never call the legacy apply. LEGACY behavior is preserved; DISABLED/unknown authority fails closed.
- The smallest support is one owner-only immutable operation receipt with typed native scope rows/indexes/FKs and exact point replay. It is not the later manual resolution store. Private ACLs stay closed; only the two exact service entrypoints are exposed and independently enforce service role, active admin and NEXT.

## Screen and approval boundary

BD114 retains supplied SCR74 B6's native chooser, three impact categories, protected/current distinction and single primary Apply change. Its illustrated manual Night-rate choice belongs to later resolver ownership and must not appear early. Unknown amounts stay unknown, no raw technical identifiers or pre-execution pay date appear, and every supported cursor page remains reachable by scrolling. Dialog sorting is exempt under SDP34. A target change requires a fresh preview; accepted/unknown request bytes survive native Close and explicit same-operation recovery. Actual source and A4 review remain separate.

## Reviewed document hashes

| Document relative to BUILD-BE | SHA-256 |
| --- | --- |
| docs/bpay-next/design/EX-08-implementation-boundary.md | f35761f8ba0f18610b7850fe67720a952b42fb9b4ea8d4ce02e4399f256761c6 |
| docs/bpay-next/design/EX-08-current-investigation.md | fcc385a1f7d9dc0e403ae870e8e07a20ab0d768564edcc68d22464bf725aeb39 |

Review saved immediately at the design boundary. Product/source, local compile/catalogue, real A4 screenshots, full queue and joined application acceptance remain NOT_RUN by this reviewer.

## Design closure — before-SQL precision supplement

**DESIGN PASS.** Re-read the current BD114 supplement. It closes all four requested contract corrections before product authoring; no remaining essential design contradiction was found.

- Complete affected scope is explicitly bounded at1,000 native members/1MiB. Preview and Apply refuse overflow rather than truncate or hide a protected category. CardR4 requires finite exact membership and no synchronous history refresh; it specifies no conflicting larger/unlimited limit, so this bound is a permitted implementation choice. Source review must still prove the overflow guard precedes mutation and that the exact native member census is complete.
- Page20/request8KiB/context4KiB/cursor8KiB/response64KiB bounds are explicit. Cursor retains the first-page closed context and final native key; every page compares that context. Identity uses native Work/revision or Case/component basis, not transient Workbench item UUIDs.
- Immutable header/children are inserted only after the existing Candidate trigger updates metadata and schedules METADATA, with pre-context, post metadata_version, task identity/generation and accepted result retained. There is no placeholder immutable row update. Native post metadata_version establishes accepted order; timestamp/random UUID cannot reorder successive changes.
- Replay qualifies the original operation/actor/epoch/exact request, including reason and context, and returns its retained result before currentness checks. Fresh Apply compares the same closed context under module → Candidate/control → original policy guards. Exact no-op refuses; same-channel different Umbrella is admitted.
- The supplement explicitly supersedes any earlier implication that METADATA is an EX08 resolver. Current0426 Work core,1438 wrapper/many and0559 Case classifier remain unchanged. The complete compatible/needs binding is emitted for named later WB29/WB45/CM11 consumption. Their saved resolution/classifier completion is not built or approved early, and no queue/task/trigger/money-owner feature is added.

Current reviewed BD114 SHA-256: **2fb51f1658a5d3c723d2275af05ca5ab4bc830f1fbb744077686b5a51f5422e5**. The initial reviewed hash and required-precision findings above remain historical evidence. Investigation hash remains unchanged.

This is design readiness for the exact EX08 owners/support only. Future source implementation, local catalogue/ACL verification, real A4 policy review, protected/future app journeys and full queue acceptance remain NOT_RUN by this reviewer. No item or runtime PASS follows from this design verdict.
