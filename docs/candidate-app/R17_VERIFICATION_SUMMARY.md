# Candidate Daily Phase 3 R17 verification summary

Date: 18 August 2026

## Current local evidence

| Gate | Result |
| --- | --- |
| Focused Candidate Daily JavaScript | **72 passed, 0 failed** |
| Complete backend JavaScript | **686 passed, 0 failed** |
| PostgreSQL 18.1 exact ordered Candidate install/runtime | **46 suites passed** |
| PostgreSQL 18.1 public-auth chain | **3 passed** |
| PostgreSQL 18.1 mixed-version authentication | **7 passed** |
| PostgreSQL 18.1 authority-transition concurrency | **2 passed** |
| PostgreSQL 18.1 first-generation concurrency | **1 passed** |
| PostgreSQL 18.1 R16 identity-integrity concurrency | **3 passed** |
| PostgreSQL 18.1 R17 transition/source concurrency | **2 passed** |
| PostgreSQL 17.6 exact ordered Candidate install/runtime | **46 suites passed** |
| PostgreSQL 17.6 public-auth chain | **3 passed** |
| PostgreSQL 17.6 mixed-version authentication | **7 passed** |
| PostgreSQL 17.6 authority-transition concurrency | **2 passed** |
| PostgreSQL 17.6 first-generation concurrency | **1 passed** |
| PostgreSQL 17.6 R16 identity-integrity concurrency | **3 passed** |
| PostgreSQL 17.6 R17 transition/source concurrency | **2 passed** |
| Workflow static dependency | **PASS** |
| Git diff whitespace check | **PASS** |

## R16 blocker closure proved on PostgreSQL 17.6 and 18.1

| R16 finding | R17 implementation/proof |
| --- | --- |
| History conflict aborted entire transition statement | `IDENTITY_LINK_CONFLICT` is contained in the existing item subtransaction and returned as indexed `REJECTED`; mixed valid/conflict batches complete and replay exactly. |
| Scope/SOURCE lock inversion | Every syntactically safe distinct source identity is sorted and locked before the existing Candidate-scope pre-lock. The R16 trigger reacquires the same transaction lock namespace. |
| Malformed pre-lock input could broaden failure | The pre-lock scan performs shape/regex/range checks as text and does not cast key-version input. Malformed items reach the item validator and return indexed `VALIDATION_FAILED`. |
| Generation versus transition deadlock | Actual generation and actual transition functions run concurrently under a test-only widened overlap; no `40P01`, timeout control flow or partial owner remains. |
| Transition versus transition input-order deadlock | Two batches with the same two source identities in opposite item order use one globally sorted lock order and complete without deadlock. |

## Engine provenance

Docker is installed but this restricted desktop process cannot open the local Docker engine. The evidence therefore does not count Docker as a test. PostgreSQL 18.1 ran through an isolated local PostgreSQL cluster. Exact PostgreSQL 17.6 was obtained from the official PostgreSQL Windows binary archive and ran through a separate isolated local cluster on its own port. Both engines executed the same saved R17 runner and source chain. Neither cluster was TEST Supabase.

## Safety

```text
Secrets printed or packaged: no
TEST Supabase changed: no
Google project changed/versioned/deployed: no
Google bridge enabled: no
Candidate feature or entitlement enabled: no
Worker deployed: no
Git commit/push: no
Production accessed/deployed: no
Finance/Banking Pay/Policy X changed: no
```
