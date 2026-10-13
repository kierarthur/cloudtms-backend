# First-install incoming source classification review

Independent read-only source review, 13 October 2026, saved-source checkpoint 14:14 BST. Scope: classify nine reported unmatched incoming native SQL hazards without changing native producers or weakening the existing gates. The reviewer read source/schema/indices and local Git objects, calculated text hashes and wrote this document only. No tests, SQL/RPC/database connection, business invocation, network, runtime, Git mutation or product edit was performed.

**SOURCE PASS for the narrow incoming hazard classification.** No remaining mandatory source finding is open in that classifier/regression source. This is not a whole source-gate, compile, runtime, PostgREST reload, A4 or release pass. The root separately reports that the integrated source gate now passes these hazards and reaches a missing-native-RPC-reload failure; that independent gate remains active.

## Native source provenance and exact reads

All three saved incoming native files exactly equal the canonical LF text of their local saved `origin/test` Git objects at `e0a02bb5f86b1fb2134e540bbf0ccc52f6feec92`. This was a local text comparison, not a network freshness claim. The reviewed build HEAD was `93cfbe5433231be4915592e40665fde3ba3f1265`, with root integration/classifier changes saved on disk.

The root reports nine unmatched hazards: contact one, notification five, Daily three. The complete reviewed classification covers fourteen sites across the three files: one typed aggregate, five native notification point reads and eight native Daily point reads. Five Daily sites already have historical baseline admission; retaining all eight in a file-specific closed set prevents that history from being used as an incomplete substitute for current-source review.

### Contact policy: one TIMESTAMPTZ aggregate

`supabase/repeatable/10102026_0810_weekly_source_contact_policy_v1.sql`, `private.weekly_source_office_contact_policy_v1(uuid,jsonb)`, lines 78–84:

```sql
select max(generation.manual_reminder_available_at_utc) into v_reminder_at
from public.weekly_candidate_cohorts cohort
join public.weekly_candidate_outreach_generations generation on generation.candidate_cohort_id=cohort.id
  and generation.state='ACTIVE'
where cohort.source_cycle_id=p_source_cycle_id and cohort.candidate_id=v_candidate
  and cohort.client_id=v_client
  and generation.request_kind=case when jsonb_array_length(v_missing)>0 then 'SUBMIT_TIMESHEET' else 'CHECK_HOURS' end;
```

`15092026_1534_weekly_source_plan6_schema.sql` declares this native generation column `timestamptz` at 1516. The actual alias binds directly to that relation; the local target is also `timestamptz`. It is a timestamp MAX, with no UUID or unresolved economic operand. The cohort unique key at 1498 begins `(source_cycle_id,candidate_id,client_id,manager_recipient_route_key)`. `30092026_2059_weekly_source_independent_candidate_requests.sql` retains the ACTIVE generation unique index on `(candidate_cohort_id,request_kind)` at 16–17. The existing aggregate spans the native matching cohort routes; it is not a new LIMIT-based point read and this review does not claim a measured query plan.

### Notification management: five exact complete-row reads

All five use `SELECT * INTO` from exactly one relation, matching their declared `%ROWTYPE`, with no joined extra projection. This expands the relation columns. The unsafe composite-value form `SELECT alias INTO row_variable` remains refused by the unchanged general scanner.

| Target and lines in `10102026_1623` | Native signature and exact identity/scope |
| --- | --- |
| `v_component`, 53–55 | `private.candidate_notification_visible_v1(public.candidate_notifications,timestamp with time zone,text)`: expense-component UUID from notification metadata plus exact notification workflow. `06092026_1636` declares `expense_component_id` UUID primary key at 14. |
| `v_workflow`, 75–77 | Same visible routine: workflow PK `id`, notification account and candidate. Native foundation `07082026_2055` declares workflow `id` UUID PK at 298. |
| `v_operation`, 150–151 | `public.candidate_app_notifications_manage_v1(uuid,text,integer,text,jsonb,timestamp with time zone)`: exact authenticated account, selected candidate and idempotency key. `10102026_1622` declares the matching unique key `(account_id,candidate_id,idempotency_key)`. |
| `v_snapshot`, 169–171 | Same manage routine: snapshot PK `id`, account/candidate, literal action SNAPSHOT and exact requested MARK_READ/DELETE action predicate. |
| `v_source`, 203–204 | Same manage routine: deletion PK `id`, account/candidate and literal action DELETE. |

There is no added LIMIT on the five unique native lookups. Existing session/service admission, per-account/candidate advisory serialization, expiry/refusal/idempotency checks and notification mutation ownership remain unchanged. The point-row classifier does not authorize those mutations; it identifies their existing assignment shapes.

### Daily reconciliation: all eight sites and multiplicities

`supabase/repeatable/13102026_0029_candidate_daily_missing_legacy_choice_reconciliation_v1.sql`, signature `public.candidate_daily_reconciliation_apply_atomic_v1(jsonb,uuid,text,jsonb,text)`:

| Target and lines | Existing complete-row lookup |
| --- | --- |
| `v_batch`, 74–75, count 1 | Environment, literal actor SIGNED_SYSTEM, operation class and request idempotency key; FOR UPDATE. |
| `v_scope`, 116–117 and 186–187, exact key count 2 | Environment/candidate PK; FOR UPDATE. Both saved SELECT texts are identical. |
| `v_command`, 126–131, count 1 | Native source-event identity OR environment/candidate/SIGNED_SYSTEM/idempotency identity; existing source-event preference ORDER BY, LIMIT 1, FOR UPDATE. |
| `v_day`, 139–140, count 1 | Active generation UUID and native date PK. |
| `v_availability`, 188–189, count 1 | Environment/candidate/date PK. |
| `v_day`, 194–197, distinct key count 1 | Same active generation/date PK plus explicit environment/candidate predicates. |
| `v_command`, 220–225, distinct key count 1 | The second saved command SELECT retains the same native alternatives, preference, LIMIT and lock with its own exact statement text. |

All targets match the single relation's declared row type; no alias composite or multiple-relation wildcard is selected. Native `17082026_0010` defines scope PK at 21, batch scope unique key at 184, rota-day PK `(generation_id,rota_date)` at 263, availability PK at 320, command idempotency unique key at 131 and partial source-event unique index at 134–136. Each command predicate branch binds its existing unique identity; the unchanged native LIMIT retains one command result. This source review preserves the existing ordering/receipt semantics and does not claim an EXPLAIN or first-use proof. Signed-context parsing, source resolution, normal Daily mutation/absence guards and identity-only probe behavior are unchanged.

## Existing policy and smallest change

The source gate's frozen historical base is `a6f7423eb71e6e7cd173e705c2290e5408a1d077`. It already derives signature/hazard/exact-statement keys, retains historical count ceilings, and decrements each count in `assertSpecialConstructs`. Existing narrow DR04/discard/expiry/projection/DR15 policies preserve reviewed old statements while new constructs are refused. Broadly admitting every SELECT star or guessing aggregate type from a column name would exceed the task.

The root's saved `FIRST_INSTALL_INCOMING_HAZARDS`/`firstInstallIncomingHazards` uses that existing mechanism: only the three named paths, exact canonical LF whole-file hashes and literal signature/hazard/statement keys with consumed counts 1/5/8. `Math.max` merges the approved counts with historical ceilings rather than adding a second duplicate budget. Every whole-file change refuses before receiving those keys. Wrong file paths receive no incoming classification. The general detector and original write-boundary, exact numeric, frozen authority, executable product and controlling-RPC reload checks remain independent. The saved diff adds the closed map/helper and its one call site; it does not rewrite native SQL or remove any existing refusal.

The full native contact/hash pin establishes the reviewed timestamp provenance; the exact aggregate key gives no allowance for another MAX/MIN argument. The row-type keys retain exact target, declared relation, SELECT/predicate, locks and LIMITs. No new generic exception system, protocol, producer, money owner or runtime behavior is introduced.

## Regression source and closure

The new regression reads all three actual native source files. It requires LF and CRLF positives and wrong-path refusal, and mutates duplicate native SELECT, the selected hazardous statement's own WHERE, an added UUID/unknown aggregate and actual dynamic EXECUTE. Both levels are checked: altered bytes refuse the whole-file pin; the same altered source also refuses when given the original finite exact keys, proving those keys do not admit a duplicate or changed reviewed statement.

**SC-01 — closed in saved source:** the brief intermediate finite-key predicate test changed the first WHERE anywhere in the file, which could leave all reviewed hazard keys intact. The saved test now uses `source.replace(statement, () => statement.replace(/\bwhere\b/i,'where true or'))`, targeting the extracted hazardous statement itself. The root separately reports focused 1/1 PASS after this correction. The reviewer read the corrected test and did not execute it.

No unchanged-safety assertion is inferred from a test result alone. All original negative controls and production detectors remain in their existing code paths. Native RPC reload failure is outside this classifier and was not exempted.

## Final source hashes

| File | SHA-256 |
| --- | --- |
| `10102026_0810_weekly_source_contact_policy_v1.sql` | `E96DB0E91E88CAB1009C504E2ACC1BB0B03EBC57277295BED4AC6280FAC7E541` |
| `10102026_1623_candidate_notification_management_v1.sql` | `16B19EA35D6046EF66C3430B05B02288149CBBB01FBBE2EF36F1381B5232A12B` |
| `13102026_0029_candidate_daily_missing_legacy_choice_reconciliation_v1.sql` | `FA64B9AEDC712EE2938E7A0488B58FF767C2B90C2C527422F4BA699EFDC63338` |
| `scripts/bpay-next-source-gates.mjs` | `47FC928AFA04D0B040F5900CC72AAFFCB262AD66DB6D6913D040518E41AD87CA` |
| `tests/bpay-next-source-gates.test.js` | `8BEEBF712F884724B119F480B41D8E92DCCCB95E4D7B5FCD382EC1F22BCD08A3` |

The three native file hashes are also their canonical LF pins. Source verdict is limited to this exact saved classification and test source; required full gates, PostgreSQL/runtime verification and release evidence remain owned by the root.
