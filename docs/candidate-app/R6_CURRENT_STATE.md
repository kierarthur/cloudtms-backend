# Candidate App R6 Current State

## Completed

| Area | State |
| --- | --- |
| Candidate core auth/session/timesheet/expense/approval/Office API freeze | Previously accepted; preserved |
| Phase 0 decisions, route contract and executable fixtures | R5 independently accepted |
| Google Evidence Gate | Complete read-only for Availability API and NEW MASTER ROTA |
| Phase 1A route/policy source | Complete locally; dark |
| Phase 1A HMAC/nonce/correlation source | Complete locally |
| Phase 1A limits/framing/failure envelopes | Complete locally |
| Phase 1A tests and dry builds | Green |
| R6 decisions and compliance extension | Complete, AV-155–AV-180 |
| Combined Current Decisions PDF | Complete, 70 pages, visually verified |

## Not completed and not authorised by this package

| Phase | Remaining work | Dependency |
| --- | --- | --- |
| Independent R6 review | Reproduce and decide Phase 1A source GO/NO-GO | This pack |
| Phase 2 | Author twelve additive Daily tables and thirteen service-only RPC owners; disposable PostgreSQL 17/18 proof | Independent R6 GO |
| Separate TEST installation gate | Review exact SQL, rollback/no-change evidence and explicitly authorise TEST install | Phase 2 source GO |
| Phase 1B | Connect broker handlers to Phase 2 authority, prove receipts/leases/concurrency/failure recovery | Phase 2 installed authority in the authorised environment |
| Phase 3 | Small Apps Script server-side coexistence adapter, mappings, projection, Emergency/Master compatibility | Phase 1B plus fresh Google Head/deployment gate and explicit Google permission |
| Phase 4 | Candidate Daily UI for responsive web, iOS and Android | Stable broker/adapter behaviour |
| TEST rollout | Activate bounded cohort, effects/soak/rollback evidence | Integrated acceptance and explicit approval |
| Production | Separately authorised gradual rollout | TEST specialist acceptance |
| Legacy decommission | Remove adapter and old client only after proven replacement | Explicit later approval |

## Current route state

```text
11 Candidate Daily routes: present in source, authenticated, globally disabled
13 Google-system routes: present in source, signed/private, business-dark
Candidate Daily SQL tables: 0 added
Candidate Daily business RPCs: 0 added
Candidate Daily UI routes/screens: 0 added
Google adapter edits: 0
```

## Current safety state

No operation in this implementation can perform Daily business work. No external system was mutated. No deployment or repository publication occurred. Existing Candidate and Office business authority is unchanged.

## Legacy coexistence state

The Availability browser remains the temporary client and was not modified. The future adapter is a small Apps Script server-side bridge; it is not a legacy browser modernization. Existing Master Rota and Emergency consumers remain protected and must be proven through Phase 3 before any cutover.

## What an R6 GO means

An R6 GO means:

> the dark Phase 1A source is an acceptable basis for Phase 2 authoring.

It does not mean:

- Daily business authority exists;
- Candidate Daily is enabled;
- Google coexistence is installed;
- app UI is complete;
- TEST mutation/deployment is authorised;
- production rollout is authorised.
