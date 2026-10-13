# SP-07 named-owner reuse review

10 October 2026. Independent source-only review of SP-07 D01/D02 and the explicit D03 mapping. **PASS for the current owner reuse and mapping.** No product SQL/code changed; no SQL, tests, business, runtime, hosted or fixture operations were performed by this reviewer. Actual joined journeys remain NOT_RUN.

## Scope and findings

Reviewed the full SP-07 card and three current deliverables, the saved `SP-07-legacy-pending-mapping.md`, current `1257` Local completion/release hook, `17092026_0700` six-argument pending-release apply owner, `0155` migration and guard, and `0624` saved-status reader. The five SQL files have no `git diff HEAD` changes. Their fresh hashes below match the earlier source investigation.

No actionable named-owner defect was found. The mapping correctly limits the clean NEXT replacement to real immediate common-HEAD publication when the existing legacy census is RELEASABLE. It retains genuine legacy pending exposure in F18 and does not fabricate a NEXT freeze ledger, a legacy fixture, a new adoption producer, F26 work or parked F25 evidence. Rule B selection, physical snapshots and existing PIN remain independent unchanged owners.

## Exact preserved boundaries

- **Authority, replay and admission:** `1257:103–123` checks the retained action's actual report-cutoff-scoped `APPROVE_PROTECTED_PAY` authority before returning exact actor/hash/key-bound receipt replay. Its existing admission at 125 precedes I-1 family locks at 128. The under-lock replay check remains intact. No pre-replay financial write or new command was introduced.
- **Unauthorised saved basis:** `1257:260–301` uses actual guarded unauthorised TSFIN and retained snapshot/financial/detail hashes. It neither authorises nor creates Banking WORK. The common-origin tuple correctly remains NULL because no common decision bundle exists before first Authorise. Current Banking protected-root qualification independently refuses that unauthorised basis as financial authority.
- **Actual later NEXT gate:** `1257:235–253` uses the existing owner share lock and requires actual NEXT plus installed Local context/proposal/capture capabilities before inserting a new authorised Local receipt. A newly created later authorised Local Save under LEGACY is not promised. Real immutable replay remains available before this fresh gate.
- **Typed sealing:** `0155` adds exactly the five nullable fields with an explicit all-null/all-present check, positive bundle revision, fixed Local origin kind, two 32-byte digests and the actual composite decision-bundle FK. The guard allows sealing only while the receipt is PREPARING in its original transaction. Accepted snapshot/origin/completion time is frozen; later result/state mutation is limited to the original PENDING_FREEZE→COMPLETE transition. DELETE/TRUNCATE refuse. The actual authorised branch fills the tuple at `1257:362–373` before the existing immediate publisher at 382. No backfill, inferred origin or fictitious typed first-Save bundle is required.
- **Real pending-release owner:** `17092026_0700:1116–1249` retains its service-role guard, exact immutable deferred-receipt replay before lease/freshness checks, existing admission at 1211 before real family/census locks and the final pending-row lock at 1232. Under-lock lease/request revalidation precedes every write. Later exact current-decision/census checks and unchanged DEFERRED core call at 1445 preserve stale/manual-review refusals; success or exact core replay records the real receipt and RELEASED state.
- **Same-run release, unchanged original audit:** `1257:491–534` requires actual pending Local receipt plus exact released publication/bundle/revision/digest/member and prior family/generation evidence. It completes the same Local receipt and advances the genuine family/generation. At 537–561 only the already COMPLETE original run's `after_state_fingerprint` changes, conditional on the exact old request result, actor/family/run identities, old fingerprint and original pending step. No run identity or first completion time changes. At 564–566 the retained request result changes. **The original completion step is not updated.**
- **Positive read-side release proof:** `0624:415–455` requires exactly one original Local completion step with the real owner, signature, request hash, before-state, finite completion time and canonical original result hash. For release, that step must remain `PENDING` with `SAVED_PENDING_FREEZE`; the current Local result must be the exact permitted released overlay of its unchanged original result, and the real bundle must be RELEASED with matching typed bundle/revision/publication receipt/digest. A synthetic completed step or unrelated completed C1 run cannot satisfy this proof.
- **Ownership/ACL:** the Local public function retains VOLATILE SECURITY DEFINER, original search path, 2500ms lock timeout and 30s statement timeout, logical owner `postgres`, revoked browser/public privileges and service-role execution. The pending hook and six-argument private apply remain revoked from public/anon/authenticated/service_role; the actual guarded public wrapper remains the service entry. No permission expansion was made.

## Correction to the preliminary investigation

My initial investigation incorrectly said release updated the original Local completion step. Fresh full source confirms it updates the Local receipt, generation/family, run fingerprint and retained request result while checking and preserving the original pending step. `SP-07-investigation.md` is now corrected with an explicit correction note. This was a documentation error; the product source already implemented the correct immutable audit relationship. The root's current mapping accurately states it.

## Evidence limits

The mapping distinguishes local installed ledger provenance reported by the builder from the historical hosted D02 exact-unit receipt caveat; this reviewer has not queried either ledger. No inference of hosted non-installation or absence is made. Six-hour/zero Office/MyTMS display, Authorise→one WORK, actual pending release, queue/replay/unknown and UI refresh checks remain explicitly NOT_RUN. This source PASS does not relabel them as exercised.

## Fresh SHA-256

| File | SHA-256 |
|---|---|
| `supabase/repeatable/04102026_1257_weekly_source_local_protected_decision_v1.sql` | `2a4f0d619ec2800f3d5a880459d0c0421f41c890bb6706750eb0464cab24fb03` |
| `supabase/repeatable/17092026_0700_weekly_source_pending_entitlement_release_v1.sql` | `177400e1d5f1be23e789ef4cd3596cdac1f9860fe94602a5a031bdd1142488c6` |
| `supabase/migrations/05102026_0155_weekly_source_local_publication_origin_v2.sql` | `2c065e71ed09ef1d393f30f6518776379c1ea14e43e50c861d624a69ba7391e1` |
| `supabase/repeatable/05102026_0155_weekly_source_local_publication_origin_guard_v2.sql` | `87ada3dcd6e1f33484acc2d09f67e7b164a23ccaf94689479f6e43c816a5e70b` |
| `supabase/repeatable/05102026_0624_weekly_source_local_saved_status_v1.sql` | `bd41da3a4f9fdb97d0deaa52b03d635e3101de44fffc2c02ca10e9724ac5c4fa` |
| `docs/bpay-next/design/SP-07-legacy-pending-mapping.md` | `aba41be93bb76ff82b3c59f04530827f2efeeca10023d713ae229b3a0fc7418a` |
