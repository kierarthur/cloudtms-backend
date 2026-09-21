# HANDOVER 2 review of the Weekly Source C1 upstream contract

Review date: 16 September 2026  
Review owner: HANDOVER 2 task `01a05db2-a696-77b2-b66c-44c3e1bb339c`  
Reviewed specification: `WEEKLY_SOURCE_C1_UPSTREAM_COMPATIBILITY_SPEC_20260916.md`  
Reviewed specification SHA-256: `3788f37d54565d4371d3de0fe5175a62005e12d486f73e695bd88a484f2003f5`  
Disposition: **COMPATIBLE — no remaining producer-to-C1 design gap**

## Confirmed boundary

HANDOVER 2 confirmed that the corrected specification matches the accepted C1 protocol for:

- complete entitlement rather than a residual or payment difference;
- the ordinary immutable Weekly Timesheet as the sole public/economic root;
- the exact closed source, component and provenance schema;
- `NHSP_WEEKLY` and generic `HEALTHROSTER_WEEKLY` source modes;
- certified zero, explicit source absence and later reappearance;
- provider authority validation before publication;
- disjoint source-expense and ordinary-expense ownership;
- no advance marker;
- unchanged dependency-closure ownership; and
- no new or changed Banking Pay, Workbench, Draft, recovery, provider, cancellation, settlement, remittance or UI owner beyond the separately approved C1 seam.

## Recovery rule confirmed

- An unknown initial START uses one deterministic exact replay bound to request ID, request sequence and digest because no operation identity yet exists for a status call.
- Later mutating calls with an operation/scope identity perform status-first receipt recovery.
- A committed call consumes its receipt without replay.
- A later call proved uncommitted may replay the identical sealed request once.
- Read-only status calls may repeat.
- Automatic mutating retry remains disabled.
- A second unknown outcome or any digest/cursor conflict refuses recovery.

## Evidence limit

This is design compatibility confirmation only. It is not implementation authority, installed-definition proof, release approval or runtime acceptance. The joined PostgreSQL/Workbench/Draft/cancellation/channel/differential matrix remains mandatory after the separately owned implementation is available.
