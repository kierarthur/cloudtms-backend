# CloudTMS Candidate locked password-authority closure

Date: 13 August 2026  
Environment: TEST only  
Runtime commit: `7a98383fe08f559c5eb10a455e7253c2c0b3dd87`

## Outcome

The private Candidate Worker may still perform PBKDF2 verification as bounded preflight work, but a mutable REST snapshot no longer authorises login session creation, failed-login accounting or password replacement. For known-account login and authenticated password change, it sends a derived non-plaintext password digest plus a SHA-256 fingerprint of the exact verifier authority it inspected. PostgreSQL recomputes and verifies that authority after durable receipt ownership, the shared per-account session advisory lock and the account-row lock.

This closes the supported races:

- old-password login versus reset, in both orders;
- old-password login versus authenticated password change, in both orders;
- stale failed-login verdict versus reset;
- two password changes whose Workers both preverify the old password;
- wrong-current-password durable rejection and exact replay.

The obsolete internal `LOGIN_FAILURE` mutation branch was removed; the canonical receipt-owned `LOGIN_SUCCESS` action with `login_failed=true` is the only failed-login mutation owner.

## Verification

- focused Candidate backend/SQL contract: 124 passed, 0 failed;
- complete JavaScript regression: 1,123 passed, 0 failed, 17 environment-specific skips;
- local PostgreSQL 17.6: 40 SQL suites plus three real-chain and seven mixed-version tests passed;
- local PostgreSQL 18.1: same matrix passed;
- GitHub Candidate runtime workflow `31693446422`: PostgreSQL 17.6 and 18.1 passed;
- GitHub safe migration workflow `31693446413`: passed;
- Candidate and Office OpenAPI descriptions: valid;
- normal, private Candidate and public broker dry builds: passed;
- fixed JS/installed-SQL authority fingerprint vector: exact match.

## TEST rollout

- authentication repeatable ledger SHA-256: `4c32cf9f8b65303d849b01f80e6a3084e5c5aa16b1bae11f473a31f802db0227`;
- private Candidate Worker version: `e9f8ae06-3e67-48d8-836f-d7cd56504c42`;
- normal TEST backend version: `03a37a90-432b-48db-a933-132d23c99fa5`;
- public Candidate broker version: `3bc03b5c-1d61-4f2b-83c9-d432c6ba97a3`;
- normal backend health/readiness: 200/200;
- public broker health/readiness: 200/200;
- direct Candidate route on normal backend: expected 404.

## Postdeployment safety

- TEST project: `test-cloudtms` (`yakevhtttcsljosbdpov`), PostgreSQL 17.6;
- Candidate flags: 0 of 12 enabled;
- electronic auto-authorise default: false;
- all seven Candidate business tables: empty;
- Candidate App authentication/workflow-bound mail: empty;
- approved public Candidate business RPCs: exactly 14, one overload each, service-role-only;
- new helper: one private invoker overload, unavailable to `service_role`, `anon` and `authenticated`;
- new Candidate business tables: 0;
- new public Candidate RPCs: 0;
- Candidate/manager business mutations: 0;
- emails/pushes/R2 business writes: 0;
- financial, invoice, payment, Banking Pay and Policy X code changes: 0;
- production access/deployment: 0;
- secrets printed or committed: 0.

## Independent review gate

The receiving reviewer must independently reproduce the real handler/PostgreSQL races, compare the installed repeatable ledger with the exact source, inspect the private helper ACL, confirm the flags/table/mail/RPC invariants and issue GO only if no concrete supported blocker remains. Candidate and Office frontend API wiring remains blocked pending that independent GO.
