# Protected WAIT witness — source contract

Owner-private `private.weekly_source_protected_wait_witness_v1(p_work_event_id uuid,p_source_cycle_id uuid) returns jsonb`. Root's report owner must resolve its actual report/cycle first; this helper accepts an actual cycle only and supplies no report or current-cycle fallback. It is read-only, sends nothing and exposes no paid/settlement flag.

NULL means no latest WAIT exists for the exact event's candidate/client/Contract-family scope and no contradictory WAIT/protected member remains in its current PUBLISHED complete vector. An unaccepted RECONCILE stage appending raw ACCEPTED_SOURCE does not clear a genuinely published WAIT: that contradiction raises unavailable. `WEEKLY_SOURCE_PROTECTED_WAIT_SCOPE_REQUIRED` is invalid input; scope mismatch raises `WEEKLY_SOURCE_PROTECTED_WAIT_SCOPE_INVALID`. Claimed WAIT with missing, ambiguous, pending, stale or contradictory proof raises `WEEKLY_SOURCE_PROTECTED_WAIT_WITNESS_UNAVAILABLE`; callers must hold admission, not coalesce this refusal into false or drop the shift.

A successful object has exactly these fields:

```text
schema_version=WEEKLY_SOURCE_PROTECTED_WAIT_WITNESS_V1
state=QUALIFIED_WAIT
work_event_id/source_cycle_id/source_group_id/candidate_id/client_id/contract_id
root_timesheet_id/family_id/family_bound_version (decimal text)
event_id/event_sequence (decimal text)/approval_id/approval_source_cycle_id
accepted_generation_id/current_generation_id/current_generation_number
protected_work_date/protected_start_at_local/protected_end_at_local/protected_break_minutes/protected_net_minutes
accepted_receipt (the unchanged native LOCAL/C1/Direct NEXT receipt DTO)
qualification_sha256 (hex)
```

The canonical root identity, actual Contract client, event/group and approval/source-group are checked. The receipt proof is extracted unchanged from `05102026_1417_weekly_source_coverage_support/qualified_coverage.inc` into one shared `private.weekly_source_protected_wait_receipt_v1(root UUID,event UUID)`. It retains exactly one nonnull financial target generation, genuine generation-less WAIT audit checks, the original native receipt helpers and every competing partial-transport count fence. The existing V3 facts reader reuses it without changing the exception/JSON/hash shape. `covered_hours_incident_v1` remains byte-identical.

The accepted approval generation may be SUPERSEDED by a sibling. Its immutable publication receipt stays relevant, while the current family generation must be PUBLISHED with its matching complete-vector hash and one exact durable event/approved-clock member. Direct NEXT uses its actual `WEEKLY_PROTECTED_NEXT_TARGET_V1` WORK/protected component; LOCAL/C1 uses the actual `WEEKLY_PROTECTED_TARGET_VECTOR_V1` target schedule's work-event/WAIT member. There is no duration-only or same-day fallback. A pending freeze does not qualify a new S25 position.

This proves protection selection, not actual payment or invoice readiness. Root clarified the report's invoice-ready arm requires a real bound finalised export plus the actual immutable pay-finalisation/projection/publication witness; it is not an external settlement gate. Protection never manufactures the required export, snapshot or source hours. Source text alone is not installed acceptance. Actual protected initial/sibling/WAIT-refresh/rotation/pending/withdrawal/partial/corrupt native fixtures, roles and NEW/UPGRADE proof remain root gates.
