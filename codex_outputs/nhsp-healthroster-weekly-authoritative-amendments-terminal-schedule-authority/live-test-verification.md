# Live TEST verification snapshot

Captured after deployment on 1 August 2026 against Supabase project `test-cloudtms`.

## Pre-install activity gate

```text
total import_apply_operations: 29
non-COMPLETE: 0
missing committed/finalised timestamp: 0
active TSFIN follow-up: 0
```

## Installed helper

```text
signature: public._import_review_effective_invoice_balance_core_v1(uuid,jsonb,integer,integer,integer,integer)
exact signature count: 1
pg_get_functiondef MD5: 4b183b4e4009189f6361223299dee287
prosrc MD5: baa981be3bdf45e8a729fcd71cb8d7c4
prosrc length: 107526
owner: postgres
security definer: false
search_path: public, extensions, pg_temp
ACL: postgres EXECUTE only
```

Local disposable PostgreSQL produced the same definition and body hashes.

## Unchanged protected functions

```text
_import_review_action_catalog_core_v1                       1623934a80bd09d263588271addd9ffd
_import_review_apply_envelope_core_v1                       3fad3522f7d3d0aaa464d8a32b20ebd9
import_review_correction_generation_transition_v1           1d3481867a4326c471812ef7c6326f76
timesheet_paid_uninvoiced_rollover_v1                       2632b3b506dcd2bd06a77dddad01e76d
hr_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid)   788e9d8926c91cf654a9d36634944d94
nhsp_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid) 5a35c7cd476e580841eb8c8a060d3992
hr_weekly_apply_transactional                               d57dea07edca42d3ae41b56b37ded723
nhsp_weekly_apply_transactional                             505bfc530f49e8e79233d1e468271e49
```

## Installed markers

```text
TERMINAL_COMPLETED_OPERATION_A_SCHEDULE: present
B_standard_schedule_authority_fingerprint: present
reconciliation-v4: present
Banking Pay/pay_workbench reference in helper: absent
```

## Catalog smoke

```text
HealthRoster latest import: 11 action rows; 0 APPLY_AMENDMENT rows
NHSP latest import:         26 action rows; 0 APPLY_AMENDMENT rows
```

These smoke calls are read-only. Because the latest live imports had no amendment actions, destructive correction cases were tested only in the disposable rollback-only fixture database.
