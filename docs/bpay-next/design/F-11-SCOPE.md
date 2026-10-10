# F-11 exact object scope — 10 October 2026

Controlling card and checklist read in full before implementation. Required objects:

- D01: existing `04102026_0560_banking_pay_next_queue_recovery_service_v1.sql` — reuse metadata enrollment, wake and expiry discovery.
- D02: shared continuation queue NEXT owner dispatch.
- D03: bounded minute recovery tick.
- D04: existing preparation/command ENROLL wakes after durable receipt.
- D05: existing PREPARE mutable-read gap — completed rule-B binding is reused.
- D06: existing PREPARE claim/PIN/order owners — retain.

Product scope is only `broker/src/index.js`: imports and dependency factory for existing transports, fresh-owner one-message dispatch, minute recovery, existing old-name metadata wake helpers, and synchronous nudge promise retention. Queue size/configuration, SQL owners and unrelated scheduled tasks remain existing. The nudge registers the actual send with waitUntil; a caller lacking that context returns a missed-wake result and relies on durable minute recovery. No detached send.

BUILDER DECISION BD020: adopt the binding scope guard for all remaining items. List exact checklist objects before building; support only their required behavior, use the narrowest reading that satisfies complete deliverables/Done, and audit scope after approximately two hours without stopping. Later accepted-record/input/captured-query/effective-cut references mean `bpay_next_work_choice.expected_revision_id`, captured `selection_candidate` columns, `run_work_query_snapshot`, and existing `run_work_query_pin_v2`. The parked 2153 tables/resolver are never a prerequisite. F-25 stays closed.

F-25 completed local commits: backend `cadf7b8e4b834059b81f76435996c9123555b945`; Office `c85a329` (full identity in subsequent checkpoint). No push or release.

One queue delivery uses the actual message, exact nonce/epoch/cursor and existing module acknowledgement rules. Non-NEXT/unknown owners retry. A malformed batch retries every actual message or throws for an invalid interface; no undisposed tail. Recovery runs once only on the minute cron, with existing four bounded lanes. SQL retains owner/epoch locks, money and revision-review semantics; F-26 handles resume validity later, F-13 qualified old-envelope retirement next.

The prior runtime design notes' producer/effective-cut paragraphs are historical and superseded by BD019/BD020. No new SQL, command kind, live trigger, proof protocol or financial owner is needed for F-11.
