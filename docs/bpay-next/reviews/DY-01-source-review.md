# DY-01 independent source review

11 October 2026. **Saved-source PASS.** No essential defect found in the route-only successor or the retained Daily resolution interaction. Static source, complete-body diff and hash review only; no SQL execution, tests, database, fixtures, hosted actions, browser, product/checklist edits or commits. This verdict does not mark the item built, installed or app tested.

## Scope and exact parent

Read the full DY-01 card `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:871–899`, all D01/D02/D03 checklist rows `IMPLEMENTATION_CHECKLIST.md:2696–2710`, dependency DY-04, sequence91 in `08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:175`, current master allocation/route chain, BD101 in `design/DY-01-scope.md`, `design/DY-01-current-investigation.md` and current final-definer registry.

`supabase/repeatable/11102026_0952_banking_pay_next_daily_resolution_admission_v1.sql:4–121` contains only the existing `private.bpay_next_approval_route_v1(uuid)` successor. Removing its one new native Daily qualification block50–69 reconstructs the entire0939 parent function statement exactly after line-ending normalization (`CompleteParentBodyExact=true`, one block). All factual current/archive/revoked checks, genuine contractless DAILY/HOURS shape checks, contracted expense routing, typed import-correction provenance, self-bill Source and resolved Source-family branches remain unchanged. No receipt, settings, rate, Source, financial, Candidate producer or public trigger is replaced.

## Processed result and captured Client

The new EXISTS point qualification51–67 reads only the actual physical Timesheet, its same-version current/nonstale SELF_REPORTED financial row and a real Client. Processing timestamp and factual Candidate are required57–59. Object/type/version/workflow checks60–62 require the native captured DAILY settings snapshot; Client must exactly match current TF63, Contract must be the explicit JSON null64, and relevant date must match the actual London worked date65–66. No display-name inference, current Contract substitution, fabricated TF/Client/HEAD, rate resolution or money calculation occurs.

Every missing or NULL predicate prevents EXISTS from succeeding; an absent/unprocessed/stale/wrong-version TF, missing Client, empty/malformed snapshot, missing/null/nonmatching Client/date or non-null/missing Contract snapshot returns the finite private `DAILY_RESOLUTION_PENDING` state68. A genuine processed matching result returns the retained DAILY_HOURS70. Processing classification itself grants no authorisation and requires no opposite-channel rate.

These operands are actual native captured authority: retained `03092026_1641_contract_settings_effective_authority_v1.sql:831–870` preserves an existing snapshot and obtains Daily Client from a processed TF; its after-financial owner893–921 captures the processed Client/nullable Contract/date. Current `_contract_settings_effective_core_v1` in `06102026_1214_self_bill_candidate_expense_separation_v1.sql:377–396` constructs the exact authority_version, Client, Contract, relevant_date and workflow members. The new branch consumes those facts without rewriting their owners. It remains STABLE under the calling MVCC snapshot, with no row locks or side effects.

## Nonpayable state cannot publish

The retained0939 authorisation trigger851–870 invokes publication only for ORDINARY_HOURS, DAILY_HOURS or EXPENSE_CARRIER. DAILY_RESOLUTION_PENDING does not call the publisher and writes no NEXT row. Existing publication187–212 still requires exact current physical TF, actual Timesheet/TF authorisation and supported native channel; the corrected0925 Daily evidence helper23–52 still positively requires current authorised physical identity, canonical provenance and factual Client/Candidate/role. A direct private publication attempt with the nonpayable route also fails before Work creation through its unchanged strict Contract/allowed-route guards. No new caller path admits the pending state.

Receipt/read/save semantics, separate booked dates, role provenance, SCX-059 Weekly Source, SCX-060 HealthRoster import and SCX-061 Candidate Daily owners are untouched. Frozen Work/Rule B selections and live locked Case money are not changed.

## D02 retained UI under the complete card

BD101 explicitly resolves the checklist's named NEXT blocker wording against the full card's Worker:none and Screen:none (`card:885–887`). The existing Daily Office maps CLIENT_UNRESOLVED to attention and **Client Required** (`FE js/main.js:116737,116776`); its resolution screen qualifies that actual status342859–342868 and presents the native **Assign client…** action342890–342897. Canonical processing continues to produce CLIENT_UNRESOLVED from an absent factual Client (`BE broker/src/daily-canonical-authority.js:203–206`). These are retained factual resolution controls, not invented payable Work.

The named NEXT Office mounts existing maintained Workbench readers (`FE js/bpay-next-office.js:1327–1338`) and the existing closed Work/Case schemas. It does not turn unresolved Timesheet receipts into Work identities or offer rows. D01's publication exclusion therefore retains the required NEXT nonpayable exclusion while preserving the actual upstream resolution control. Adding an unresolved receipt reader or artificial Workbench row would contradict the card's explicit no-screen-change scope. D02's actual joined application check remains NOT_RUN.

## Packaging and future acceptance

Signature, STABLE/security-definer posture and fixed search path are preserved0952:4–9. Existing ownerpostgres and REVOKE ALL from PUBLIC/anon/authenticated/service_role122–123 retain owner-only access; no grants, new public RPC or attachment are introduced. Registry diff changes only the exact route chain to0952 and records0939 as predecessor; publisher, trigger, helper0925, pointer0928, guards and generic Authorise0939 remain current.

D03 remains AUTHORED_NOT_RUN: genuine unresolved receipt excluded from NEXT, actual Client resolution/processing/settings snapshot, actual authorisation yielding exactly one stored ordinary Work, separate booked dates/read-save no creation, retained Source/import/Candidate paths and exact before/after-click behavior. Static review does not prove these runtime outcomes or operational installation.

Raw SHA-256 of reviewed saved bytes:

| Source | SHA-256 |
| --- | --- |
| 0952 route successor | 3eee328de610d01b1890bf4fd039001fb01ccf8dc9cb51c8a233e6f4d1406b87 |
| 0939 publication parent | 4a21ab611b90ed39fed312a5d8aaa0754e72f980da9c621ee815ec1f611c6c1a |
| final-definer registry | 9c74603ff3c4e9e1c22eede93e7fa4096c3cf9d96bb41b2279169f4643c7d65d |
| DY-01 scope | ea2951fd5e46edebe340c37de2e78a93aad0e1080e35390e2486b6886f004f9b |
| DY-01 investigation | 8af1d3c4287e64d94d38ece15efedf0db5413769269dd221e790b5b628862b8e |
| retained FE main | a65011bef9bb78bb83e4b8e2ab7481690dd724c83ef6b3716b79ff6c11292915 |
| retained FE NEXT Office | 8b9e437170c1cb015d13391447d3fb20aa39b37718de9040dc0e5051097abdf3 |
| retained FE Workbench | f01b5540b281e9ee30b45098811f6ec7c8067ddad93f9247faa4bea760990e3e |

