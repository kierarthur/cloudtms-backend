# WB-37 sequence 74 scope

11 October 2026. Exact physical checklist objects: D06 public.bpay_next_snooze_due_page_v1(jsonb), D07 public.bpay_next_workbench_week_rollover_page_v1(jsonb). The card additionally names the two independently bounded minute lanes. All nine checklist entries, full card, R2-F01/W12-W14, current native guards/owners and dependency closures read before implementation.

BUILDER DECISION BD083: adopt the narrow native expiry/eligibility interpretation. Preserve original financial, Source, Pay Query, F-25, at-cut and projection formulas. No new command, table, financial job, Office route or screen. Snooze until-date is inclusive, using London calendar date; rollover consumes maintained eligible_from_date. Case FUTURE_WEEK coverage from the existing due lane remains through its own existing refresh owner.

Necessary support is limited to one private additive migration: native cleared_reason, an existing-event request lookup index, and two date/UUID seek pairs on existing workbench_revision id=1. Existing due indexes are reused. Cursor-only updates preserve all context/financial revisions, occur after controls/projection and compare the originally read tuple; each sweep wraps at its end. There is no discovery or proof framework.

Existing snooze metadata/receipt guards need the truthful WOKEN/null system actor branch, preserving administrator provenance on the snooze. One bounded native event validator qualifies 1..200 exact original-command events and before/after tuples, never all audit history. Historical absent nullable reason is normalized without rewriting any event. One original SNOOZE_CLEAR command binds a successful page; a wholly failed page rolls back its command. The original snooze projection trigger supplies exact scope refresh once.

The current enroller otherwise stops forever at a completed manual zero-member snooze. Its small successor advances one exact next completed current-epoch snooze only after the original manual receipt or bounded DUE event set, zero members/jobs and seal. PREPARE and every financial branch remain unchanged.

Candidate controls precede Work/Case/native snooze preholds, command clock and global projection. Non-waiting per-scope locks preserve unrelated advancement; busy/failed scope remains due and is retried on its circular sweep. The rollover wrapper's bounded linked Case preholds occur before projection. Each native discovery page visits at most 200 records; Worker requests 100, two pages per independent lane/tick. Queue and financial consumption are unchanged.

Dependencies: F-11 current runtime/minute wiring is built. WB-36 native scope/basis/guard/manual command reader is built; WB-08 maintained native refresh and WB-44 continuation/seed owners are built. Their whole ITEM boxes remain open for their separately named future WB-45 resolver/other closure references. Those future references do not block native dated expiry or maintained FUTURE_WEEK promotion, and are not prematurely ticked by WB-37.

Reviews: WB-37-current-investigation.md and WB-37-scope-review.md. Per-card joined app journeys, contention, queue delivery and installation remain AUTHORED/NOT_RUN. No database data, local/hosted timing, fixtures or rehearsal. Timing check moved to TEST after installation (owner 10/10/2026).
