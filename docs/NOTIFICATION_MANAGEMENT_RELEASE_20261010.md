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
MyTMS reviewed source: `7de6dd449f20cedaf1bccb36bd64fad57f6446e2`.

## Qualification before publication

- Candidate app: 632 tests, TypeScript, guards, Expo Doctor 21/21 and three-platform exports pass.
- Root contracts/control plane: 268 tests and contract lint/check pass.
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

MyTMS installed-state preflight: workflow run `38067422009`, pending at the time
of this source checkpoint. The prior preflight stopped at raw OpenAPI lint before
database rehearsal; its four missing 4xx declarations were corrected and reverified.
No durable installation is inferred from a preflight or a local test.

Backend and MyTMS installation, active Worker identities, Pages publication and
real TEST acceptance remain to be recorded after their protected release gates.
Existing Android 39 and iOS 0.1.5/build 24 do not contain this notification UI.
No new Apple/EAS build is authorised for this small update; iOS changes are batched.

No LIVE resource, secret rotation, payment/provider execution or email drain was
used for this qualification. Disposable proof resources must be removed after use.
