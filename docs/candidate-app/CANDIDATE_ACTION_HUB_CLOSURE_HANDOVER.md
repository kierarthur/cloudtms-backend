# CloudTMS Candidate action-hub closure handover

Status: TEST-only runtime published, installed and deployed for independent DB/RPC/backend/API verification. Candidate feature enablement and frontend code remain prohibited pending independent GO. Office-frontend implementation planning is a separate deliverable and must proceed regardless of the audit verdict.

## Published authority

- Runtime commit: `5c6c57783e167a04046e462d0499030b104e1fb9`
- Runtime title: `Close Candidate action hub authority`
- PostgreSQL 17.6/18.1 workflow: `31503047448`
- Safe TEST migration workflow: `31503048607`
- Installed read/action repeatable LF-normalised SHA-256: `a6d107856a67d4af523658fa98add36b67aec5f6e9cdc268f13d3337c9526633`
- Normal TEST backend version: `60f29b77-db51-41b2-9b68-75702f1e6223`
- Private Candidate API version: `8be4ffff-5878-459e-acee-69b6aa11ec1b`
- Public Candidate broker version: `ee7f54c5-f034-4d06-8ce3-762653748c5c`

## Closed findings

### Immutable rejection replacement

An office-rejected workflow is immutable history. Its recovery action no longer attempts `AMEND`, which is intentionally invalid for `REJECTED`. The new service path:

```text
POST /candidate-app/v1/workflows/{workflowId}/resubmit
```

validates account, candidate, state and generation, proves the rejection remains actionable, derives the current replacement identity on the server and creates a new `WORKER_DRAFT` workflow through the existing `CREATE` authority. A UUID idempotency key returns the same replacement on lost-response replay. Client-supplied workflow kind, route, scope, contract, week and anchor values are not authoritative.

Every independent actionable rejection remains in the plural rejection contract and receives its own recovery action. A separate hours and expense rejection is not collapsed into one action.

### Exact card/detail membership

`private._candidate_workflow_maps_to_card_v1` bounds workflows, components and document state to the exact opened contract-week/card/version family. It resolves historical expense anchors through the stable current worked-row family while preventing another `additional_seq` record or unrelated same-date claim from entering the response.

### Executable action contract

Each action contains invocation version 1:

- `HTTP` — exact method/path, immutable `fixed_body`, declared `required_user_inputs`, and idempotency requirement;
- `CLIENT_DESTINATION` — exact timesheet or expense editor destination and workflow context.

`ENTER_TIMESHEET` and `ADD_EXPENSES` carry their complete server-owned create context. `CONTINUE_*` opens the correct editor rather than re-fetching the same detail. `REFUSED / REVIEW_AND_RESUBMIT` uses the supported `AMEND` transition. Rejected recovery uses the new replacement path.

### Durable PAPER readiness

`private._candidate_paper_pack_readiness_v1` classifies the exact workflow/generation/manifest/outbox/attachment contract as:

```text
NOT_APPLICABLE
PREPARING
READY
FAILED
RETIRED
STALE
RETURN_RECEIVED
```

Download and signed-return upload are enabled only for `READY`. The private backend separately proves the exact immutable R2 receipt before serving bytes. `AWAITING_PAPER_RETURN` no longer makes an unfinished pack look downloadable.

## Verification

- Complete JavaScript backend suite: 461 passed, 0 failed.
- Focused Candidate/backend suite: 64 passed, 0 failed.
- Dependency-light Candidate/DB/PAPER/DAILY suite: 50 passed, 0 failed.
- Candidate SQL runtime/concurrency suites: 28 passed on PostgreSQL 17.6 and 28 passed on PostgreSQL 18.1.
- OpenAPI validation: pass.
- JavaScript syntax: pass.
- Normal backend, private Candidate API and public Candidate broker builds: pass.
- Git diff whitespace check: pass.
- Safe TEST migration: pass.
- Runtime checks: normal health 200; direct public Candidate route on normal backend 404; broker health 200; broker readiness 200.

Post-deployment dormant-state proof:

```text
Candidate flags enabled:                    0 of 12
Candidate auto-authorisation default:       false
Candidate business-table rows:              0
Candidate-bound mail rows:                  0
Approved public Candidate business RPCs:   14
anon/authenticated execution:               none
service_role execution:                     all 14
new private helper service execution:       none
```

No Candidate workflow, email, notification, push, R2 business object or financial operation was created by deployment verification.

## Independent verification instruction

Do not restrict review to the latest audit findings. Review the complete current-decisions PDF and every adjacent installed DB/RPC/backend/API seam required to satisfy it. Report any clearly incorrect, unsafe, contradictory or incomplete server-owned behaviour within that scope. In particular, verify:

1. rejected history is never revived or amended and resubmission creates one new server-derived workflow;
2. lost-response replay returns the same replacement and conflicting key reuse fails;
3. each independent rejection scope retains a separate executable action;
4. `REFUSED` uses supported `AMEND`, while `REJECTED` uses replacement `CREATE`;
5. card/detail membership cannot mix other contract-week sequence records or unrelated claims;
6. PAPER download/upload remain disabled until the exact immutable outbox/attachment receipt is ready;
7. failed, stale and retired PAPER states remain non-actionable;
8. every returned action is executable from its typed invocation without browser inference;
9. fixed server fields cannot be overridden by client input;
10. all previously accepted upload, renderer, mail, provider-permit, route/rejection, manager and Current/History controls remain intact;
11. exactly seven Candidate tables and 14 public business RPCs remain;
12. no financial, Banking Pay, Policy X, invoice, payment, settlement, remittance or production authority changed.

Give a clear GO or NO-GO for Candidate backend/API freeze with reproducible evidence. Keep all Candidate flags false during verification.

## Separate frontend-planning rule

The independent backend verdict must not suppress or truncate the separately requested office-frontend implementation plan. A NO-GO blocks frontend code execution and API freeze only. The frontend-planning chat must still return its complete detailed plan so backend correction and frontend preparation can proceed in parallel without inventing the contract.

