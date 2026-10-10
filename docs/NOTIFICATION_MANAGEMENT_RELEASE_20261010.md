# Notification inbox management — TEST release, 10 October 2026

## Reviewed scope

Latest 14 notifications with cursor-based Load More; separate accessible Open
and Delete controls; whole-inbox Mark all read and Delete all against an exact,
five-minute server snapshot; ten-minute Undo retaining the original read state.
Late arrivals are not included in an earlier whole-inbox snapshot. Scope follows
the authenticated environment, account and candidate, not a client-supplied ID.

Read notifications expire after 30 days and unread notifications after 90 days,
both measured from creation. Resolved or superseded actionable notices are hidden
using exact current lineage and committed state. Timesheets, expense claims,
queries and audit history are not deleted. Unstarted PUSH delivery rechecks
notification actionability before contacting the provider; manager EMAIL and
already-started/uncertain provider outcomes remain unchanged.

Four closed notification-management operations extend the existing 68-operation
catalogue to 72 without modifying those 68 policies. Policy SHA-256:
`9b7416efd74aef0e037f0a195f5f7afacb7799db2dd83b0617118496e2d8bb5c`.
MyTMS reviewed runtime source: `d23876c3232a7405ef4da8d0c88a8d2acfea2735`.

## Qualification before publication

- Candidate app: 632 tests, TypeScript, guards, Expo Doctor 21/21 and three-platform exports pass.
- Root contracts/control plane: 269 tests and contract lint/check pass.
- Browser: 236 tests pass, including notification paging, selection snapshots,
  late arrivals, navigation-safe Undo, read-state retention and separate 48px targets.
- Backend: 1,486 tests pass; release coordinator tests and binding registry pass.
- Disposable PostgreSQL 17 NEW rebuild: 295 migrations / 799 repeatables;
  all 156 registered portable verifiers pass. Canonical source-reader preparation
  was required by the local harness. Cold invoice fixture proof was rerun with
  JIT disabled only in the disposable process; no hosted setting or verifier changed.
- Notification rollback proofs cover paging ties at full timestamp precision,
  ownership, expiry, replay, snapshots, Undo and existing Office deletion compatibility.
- Existing PUSH regression also proves READ notices cannot be newly claimed or
  reach provider submission after an earlier lease, without any network dispatch.
- Reviewed catalogue: 169 Candidate-named RPCs / 9 service exclusions / zero
  browser-executable functions, hash `1d353a1d80ad8cb20aaa841fb4dfe948`.
  Removing only the two new notification RPCs exactly reproduces the prior
  167 / 9 / 0 seal `83ccfa8f2422ab86c38bcd48c12a2e16`.
- Generated contract SHA-256:
  `2f322476bce280e8def23f53449763fd81d3c6807815da36fdc04c8bcfc8ed69`.
  Diff is limited to the reviewed notification tables, owner policies, index,
  helpers/RPCs, Home reader and two PUSH consumers.
- Critic accepted the final reviewed source after microsecond cursor preservation
  and sibling Open/Delete accessibility corrections.

## Publication and acceptance

MyTMS rollback-only preflight `38067422009` passed; the actual protected
installation `38068411988` also passed. Independent Miget read-back matched
the migration and repeatable hashes recorded in the MyTMS device-testing report.
Backend protected full UPGRADE `38069005153` passed at source
`2c907ece65963fcd6af2806b958c51829c26bc08`. The coordinator receipt records
all four ordered runtime publications successful, with functional acceptance separate.

Fresh Cloudflare read-back on 10 October confirmed 100% traffic at that exact
successful source build for every Worker:

| TEST Worker | Active version |
|---|---|
| test-cloudtms-backend | 6c2a16e6-5e2e-41a2-a826-e39b8da144d3 |
| test-cloudtms-candidate-private-api | fa344892-b40d-4966-85a3-ade780776a54 |
| test-cloudtms-candidate-synthetic-private-api | cb9a0175-760d-4f1e-a651-ab323c564efd |
| test-cloudtms-candidate-broker | 3ca82a2f-3e71-4b3c-926d-acaaf29acf15 |

Candidate TEST web publication `38092606339` passed at deployment-repository
commit `95561f4a01805a36e38ec88e9979fbc5a986e8ef`. The sealed archive hash is
`d4640b0ace31c4980ef100ae0942abcafb87b6b5bb7c0fd1721da3a077de4906`.
The served entry bundle exactly matches the reviewed local export, SHA-256
`29867df9d576127e31896e4a4b09b555a7426f61622f1c3c2dfa86e6422d24ec`.
Post-publication focused browser qualification passed four notification viewport
tests and twelve import-approval/QR eligibility cases. These use mocked API
responses and are not claimed as live business-write acceptance.

Android 40 is built and signed; the owner now explicitly excludes uploads to
Google Play and Apple. No store action or new Apple/EAS build is part of this
continuation. Existing Android 39 store releases and iOS 0.1.5/build 24 do not
contain this notification UI; the connected phone's Play package remains 37.
Live signed-in web and current-source USB acceptance are being checked separately.

No LIVE resource, secret rotation, payment/provider execution or email drain was
used for this qualification. Disposable proof resources must be removed after use.
