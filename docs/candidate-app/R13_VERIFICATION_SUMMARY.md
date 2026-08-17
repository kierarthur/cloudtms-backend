# Candidate Daily Phase 3 R13 verification summary

Date: 17 August 2026

## Executable results

```text
Focused Phase 1A/1B/2/3/R13: 73 passed, 0 failed, 0 skipped
R13 recovery/quota suite: 19 passed, 0 failed, 0 skipped
Complete backend JavaScript: 651 passed, 0 failed, 0 skipped
Candidate broker dry build: PASS
Private Candidate dry build: PASS
Public Candidate health/readiness: HTTP 200 / HTTP 200
Real TEST broker invalid-authority probe: HTTP 401 SYSTEM_AUTH_FAILED / DO_NOT_RETRY
```

## R12 defect closure coverage

The R13 suite proves:

1. exact `BATCH_IN_PROGRESS / STATUS_CHECK` retention;
2. exact terminal `SOURCE_EVENT_CONFLICT` and `GENERATION_INCOMPLETE` handling;
3. malformed/unknown responses remain recoverable;
4. `429`, `5xx` and transport uncertainty retain immutable operation identity;
5. a later accepted event recovers pending state before creating anything new;
6. batch UUID, key, correlation and body remain exact across at least three recovery cycles;
7. unresolved state is not replaced after seven days;
8. first-batch success plus later-batch uncertainty does not log overall completion;
9. eventual last-batch success logs completion once;
10. four and fifty minimal items persist below the conservative per-value ceiling;
11. reassembly is byte- and SHA-exact;
12. near-total store exhaustion makes no POST;
13. oversized request bodies are split under the route safety ceiling;
14. corrupted pending state fails closed without replacement identity;
15. the builder includes two eligible TEST fixture candidates, proving no candidate-specific restriction;
16. no raw public ID, phone, email or source secret enters the request/log state.

The real TEST negative probe uses a structurally valid 14-day body and deliberately invalid synthetic signing authority. It proves route/body/header parsing and fail-closed authentication before identity resolution; it performs no database mutation and is not a substitute for the later positive signed generation journey.

Read-only identity qualification also proves that Kier's exact existing global Candidate key is present and matches, while the separate `GOOGLE_CREDENTIALLY_PUBLIC_ID` Daily source-link catalogue has zero active rows for Kier and zero overall. The controlled source-link bootstrap remains a prerequisite to enabled proving.

The onboarding decision is population-safe and future-facing: a Candidate who registers only in the new app is identified by the admin-entered global key, and the derived Google source HMAC must be bound to that same existing Candidate UUID. Legacy-app participation is not required. Executable admin-onboarding proof remains a later gate and is not falsely reported as completed by R13.

## Google qualification

The R13 helper was saved in NEW MASTER ROTA Head but was not deployed. The active version remains 102, rollback 101. Availability remains active 216, rollback 215, and was not edited.

This is deliberate: the product owner requires independent GO before either Google project is deployed again. No enabled bridge journey was executed.

## Safety

```text
Bridge enabled: no
Signed route executed: no
Google deployment: no
Supabase mutation/definition install: no
Worker source deployment: no
Candidate business row or feature change: no
Emergency/provider action: no
Financial authority change: no
Production action: no
Secret value read or packaged: no
```
