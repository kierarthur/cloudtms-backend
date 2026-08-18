# CloudTMS Candidate App phase-completion matrix

Date: 18 August 2026

This matrix is a navigation aid. The complete decisions remain in the current Decisions PDF.

| Boundary | Current disposition | What is already established | What remains before the boundary is complete |
| --- | --- | --- | --- |
| Candidate core DB/RPC/backend/API | GO remains in force | Seven-table/fourteen-RPC core, authentication, sessions, timesheets, evidence, expenses, approval, QR, rejection/resubmission, documents, mail/R2, public/private broker boundary | Preserve under regression; change only for a concrete reproduced defect |
| CloudTMS Office Candidate integration | Implemented and app-ready regression boundary | Shared Candidate statuses/actions across Summary, Simple, Bulk Process and Bulk Authorise; Evidence/Issues rules; reminder workspace; server-owned gating | Revalidate alongside full Candidate clients; preserve all decisions and modal quality rules |
| Phase 0 | GO | Daily/Emergency decisions, operation matrix, identity direction, minimal-change legacy rule, phase sequence | Regression only |
| Phase 1A | GO | Dark signed-system transport, closed routes/errors/retry, replay and throttling | Regression only |
| Phase 1B | GO | Strict Worker-to-RPC operation mapping and dependency seams | Regression; real specialist executors remain Phase 6 |
| Phase 2 | GO and installed disabled | Daily database authority, generations, availability, commands, transitions, projections, effect receipts | Regression; no real enablement inferred |
| Phase 3 R17 source correction | Under independent verification | R16 identity protections plus durable transition conflict containment, shared SOURCE-before-scope order, malformed-source item validation, exact PostgreSQL 17.6/18.1 matrices and actual cross-writer concurrency proof | Independent GO/NO-GO |
| Phase 3 publication/install | Not performed | Complete proposed R17 source, exact PostgreSQL 17.6/18.1 and JavaScript evidence are packaged | After R17 GO and explicit authority: rebase, rerun if changed, commit/push, exact-commit PG17/18 gate, safe TEST SQL install and read-only parity; no Worker change is required by R17 |
| Phase 3 disabled Google qualification | Not performed | Certified source/rollback copies and operator baseline exist | Separately authorised Google install/version while switches stay false; prove exact legacy equivalence and rollback |
| Phase 3 enabled coexistence proving | Not performed | Architecture and runbooks exist | Separately enable TEST bridges; prove population-wide first-use binding, replay/outage/quota/projection, old-app and Candidate-UUID convergence; final Phase 3 GO |
| Phase 4 | Not started | Frozen APIs and Office compatibility exist | Build full responsive web/iOS/Android Candidate App, not Daily-only; functional/visual/accessibility/offline/parity testing; independent GO |
| Phase 5 | Not started | Database transition authority exists | Controlled TEST cohorts, reconciliation/drain, parity/soak/error budget, real transition and rollback rehearsal; independent cutover GO |
| Phase 6 | Not started | Durable effect receipt seam exists; legacy providers continue | Full Emergency/attendance/specialist/messages/content/Past Shifts/DAILY approval acceptance through old and new paths |
| Phase 7 | Not started | Rollout principles are frozen | Gradual entitlements and monitoring; separately approve legacy-browser/adapter retirement; retain Availability/Emergency/Master until individually migrated |
| Production | Not authorised | None | Separate explicit production plan, review and authority after TEST completion |

## Gate semantics

An R17 GO means only that the exact correction may proceed to collision-checked TEST publication/install and disabled Google qualification. It is not:

- final Phase 3 GO;
- bridge-enable authority;
- Candidate feature/entitlement authority;
- Phase 4 authority;
- full-project GO;
- production authority.

Final Phase 3 GO requires enabled coexistence evidence. Full project completion requires separate GO through Phase 7.
