-- Installed-writer census verifier (Weekly Source Plan 6.2, WP-18).
--
-- HANDOVER 2 round-4 ruling 5 (OR-3) replaced the closing rule of
-- proof/32 section 5.4 with:
--
--   "Every installed writer that can void an item, cancel or terminalise a
--    batch, or alter reservation/transfer evidence consumed by the census must
--    appear in the installed writer inventory. Each must be classified as an
--    exact Binding A/B/C writer, the named section 5.2 settlement/terminality
--    owner, or an explicit structural fail-closed exclusion. Any unrecognised
--    writer, changed installed definition or unclassified evidence mutation
--    blocks release. Runtime release still requires the item's own positive
--    binding or settlement proof; inventory classification is not item-level
--    evidence."
--
-- This file is the design-time half of that rule, and of ruling 1's second part
-- ("at design/release time, every installed writer capable of changing evidence
-- relevant to these classifications must be inventoried and assigned a binding,
-- a named non-A/B/C authority class, or an exact structural fail-closed
-- exclusion"). The runtime half is the freeze census, which is a different file.
--
-- It proves, against whatever database it runs in:
--
--   1. every classified owner exists, exactly once, with its exact identity
--      arguments, SECURITY DEFINER status and classification;
--   2. every classified owner's complete installed definition still hashes to
--      its pinned SHA-256, with line endings normalised, so the same pin holds
--      on a clean local build (LF bodies) and on an installed database whose
--      bodies carry CRLF (env/ENVIRONMENT_REPORT.md section 5.4);
--   3. public.pay_batch_cancel is the five-argument validating delegator and
--      performs none of the census-relevant writes (ruling 5 correction 1);
--   4. public.pay_settle_rail is the only routine whose pay_batches.status write
--      can reach 'SETTLED' or 'FAILED' and the only routine that writes
--      pay_batches.completed_at_utc (ruling 5 correction 3), and the
--      cancellation stamp has exactly the four owners ruling 5 names. Read
--      section 6 before relying on this: sole terminality rests on the hash pin
--      on pay_settle_rail together with section 8's completeness, NOT on a
--      structural property of the scan on its own;
--   5. public.pay_batch_apply_finance_adjustments qualifies for the
--      NON_ENUMERATED_BORN_VOIDED_TEMPLATE structural exclusion of ruling 2 by
--      the exact shape of that one INSERT branch: already voided, null
--      timesheet_id, no bound transfer, no item-linked reservation;
--   6. public.pay_unpay_batch is still non-committable: the installed batch
--      status constraint still rejects 'UNPAID' (ruling 5 correction 6);
--   7. no other installed function in public or private writes any
--      census-relevant column, or deletes or truncates a census-relevant
--      evidence row, without appearing in the inventory, SO FAR AS A LEXICAL
--      SCAN OF INSTALLED DEFINITION TEXT CAN ESTABLISH IT -- see the limits
--      below, which are not decoration;
--   8. no write path that is NOT a routine -- a rewrite rule, a trigger on an
--      evidence table, a trigger function outside the scanned schemas, a
--      generated column, a census-column default, or a foreign-key action that
--      deletes or nulls evidence -- has appeared or changed since it was
--      censused (section 10).
--
-- WHAT THIS FILE DOES NOT COVER. An approver reading only the summary would
-- otherwise assume the coverage is total, and TWO ROUNDS OF INDEPENDENT REVIEW
-- HAVE NOW DEFEATED THIS SCAN, so this list is stated without softening:
--
--   a. IT IS A LEXICAL SCAN, NOT A PARSER, AND A ROUTINE THAT TOKENISES PERFECTLY
--      CLEANLY CAN STILL HIDE A WRITE FROM IT. An earlier version claimed it
--      "cannot be fail-open": false, and withdrawn. Ten ordinary SQL shapes
--      defeated the first version (WP-18-19_REVIEW.md F1) and a Unicode-escape
--      identifier defeated the second (WP-18b_REVIEW.md F1) while tokenising
--      cleanly. Every KNOWN escape shape -- twenty-two of them now, listed in
--      backend/tests/weekly-source/wp18b-installed-writer-census-fixtures.mjs --
--      is a permanent bidirectional fixture and is detected. That is not the same
--      claim as "every escape is detected", and it must not be read as one: what
--      is proved is that the shapes anyone has thought of are caught.
--   b. Identifiers this scan cannot reduce to a relation name, and SQL assembled
--      at run time, are NOT understood. They are refused: sections 8d and 8f fail
--      closed on them, so they block the release rather than passing it.
--   c. Writes made by the APPLICATION LAYER are outside this census entirely,
--      and this is the largest hole. service_role holds DELETE, INSERT,
--      REFERENCES, SELECT, TRIGGER, TRUNCATE and UPDATE on all four evidence
--      tables, and each table carries the cloudtms_miget_service_owner_all policy
--      for {postgres, service_role} with cmd = ALL. The Worker can therefore void
--      an item, cancel a batch or release a reservation through PostgREST or the
--      gateway without calling any database routine, and no such write is visible
--      to this file. Census 00 section 7.5 records the same exclusion. Whether
--      Worker-side direct writes exist is owned by the Banking Pay workstream.
--   d. Only the public and private schemas are scanned for routine text. Section
--      10 fails closed if a trigger anywhere calls a function outside them.
--   e. Nothing here executes any owner. This is a text-identity and structure
--      proof of the installed catalogue; runtime behaviour is the freeze census.
--   f. Line endings are normalised before hashing, so a change that is only
--      CRLF/LF, including inside a string literal, cannot be seen by a pin.
--
-- THE TWO HALVES ARE NOT EQUALLY STRONG, and the difference is the single most
-- important thing to understand about this file. Sections 3 and 4 -- identity and
-- the thirty-one SHA-256 pins -- are exact: any edit to any classified owner
-- fails the gate deterministically, whether or not anyone anticipated it, and
-- none of the limits above touches them. Sections 2, 8 and 8c are a lexical
-- filter over text: good, much harder to walk past than it was, backstopped where
-- it cannot resolve what it is reading, and still not a proof.
--
-- READ-ONLY. It executes no installed owner, opens no transaction of its own,
-- writes no table and calls no Banking Pay, Draft, execution, cancellation,
-- settlement, remittance, provider or payment routine. Its only objects are
-- six temporary tables, all created in and dropped from pg_temp. Every
-- "drop table if exists" below is qualified with pg_temp. because an unqualified
-- drop resolves to a PERMANENT table of the same name when no temporary one
-- exists yet: the review found that running this file against a database holding
-- a permanent public.wp18_observed silently destroyed it and still reported ok
-- (finding F5).
--
-- NOT REGISTERED in supabase/release/current-release.json by this package: the
-- seals package owns that file. Hand it off before release.
--
-- Provenance of every pinned value: compiled and compared on 17-18 September
-- 2026 against the local PostgreSQL 17.11 full build and, read-only, against the
-- installed agency_test catalogue. See
-- plan6-2-implementation/reports/WP-18-19_REPORT.md.

\set ON_ERROR_STOP on
set client_min_messages to warning;

-- ---------------------------------------------------------------------------
-- 0. The census-relevant evidence surface.
-- ---------------------------------------------------------------------------
-- Columns: exactly the evidence proof/32 sections 4.1 to 5.3 consume when it
-- decides whether an item is voided, whether a batch is terminal or cancelled,
-- whether a reservation is live, and how a transfer classifies.
drop table if exists pg_temp.wp18_census_columns;
create temporary table wp18_census_columns (
  table_name text not null,
  column_name text not null,
  primary key (table_name, column_name)
);
insert into wp18_census_columns (table_name, column_name) values
  ('pay_batch_items','is_voided'),
  ('pay_batches','status'),
  ('pay_batches','cancelled_at_utc'),
  ('pay_batches','completed_at_utc'),
  ('pay_advance_reservations','status'),
  ('pay_advance_reservations','released_reason'),
  ('pay_bank_transfers','status'),
  ('pay_bank_transfers','rail_state');

-- ---------------------------------------------------------------------------
-- 1. The inventory.
-- ---------------------------------------------------------------------------
-- classification values and what each one means:
--
--   BINDING_A   correction-bound void or cancellation completion
--               (proof/32 section 5.1 Binding A, as ruling 5 corrects it)
--   BINDING_B   whole-batch cancellation / Draft rebuild void
--   BINDING_C   positively corroborated PAYE-net reprojection void
--               (ruling 1: the runtime proof also needs the item-linked
--                RELEASED / PAYE_NET_REPROJECTION reservation)
--   SETTLEMENT_TERMINALITY_OWNER
--               the named section 5.2 / 4.1 owner; not an A/B/C void-or-cancel
--               writer (ruling 5 correction 3)
--   VALIDATING_DELEGATOR_NO_WRITE
--               ruling 5 correction 1: recorded separately, must never write
--   NON_ENUMERATED_BORN_VOIDED_TEMPLATE
--               ruling 2: the exact dormant-recovery-template INSERT site only
--   NON_BINDING_OWNER
--               ruling 5 corrections 5 and 6: installed, classified, and
--               explicitly NOT a release authority
--   NON_CENSUS_LIFECYCLE_WRITER
--               writes a census-relevant column, but no value it can write is a
--               census-relevant transition: it voids no item, cancels or
--               terminalises no batch, and releases no reservation. Pinned so
--               that any change to it blocks release and forces reclassification.
--
-- definition_sha256 is over pg_get_functiondef with CRLF normalised to LF.
drop table if exists pg_temp.wp18_inventory;
create temporary table wp18_inventory (
  schema_name text not null,
  routine_name text not null,
  identity_arguments text not null,
  classification text not null,
  definition_sha256 text not null,
  reason text not null,
  primary key (schema_name, routine_name, identity_arguments)
);

insert into wp18_inventory
  (schema_name, routine_name, identity_arguments, classification, definition_sha256, reason)
values
-- ---- Binding A: correction-bound void and cancellation-completion owners ----
  ('public','pay_pre_bank_cancel_apply_work_item',
   'p_work_item_id uuid, p_actor_user_id uuid',
   'BINDING_A','9475812b6d146f434971317d23f6b7754b666ca4403ecd8ccb6f4269c3966d10',
   'Correction work-item apply: voids the item and releases its reservation under an APPLIED pre-bank-cancel correction request.'),
  ('public','pay_no_money_unwind_apply_work_item',
   'p_work_item_id uuid, p_actor_user_id uuid',
   'BINDING_A','2285079b3a88ffa5ef6dab0a339aab5679d142853b082e867dc74ee200d3fb3f',
   'Correction work-item apply: no-money unwind; voids the item, releases its reservation and stamps its transfer.'),
  ('private','pay_pre_bank_cancel_apply_work_page_v1',
   'p_correction_request_id uuid, p_work_item_ids uuid[], p_actor_user_id uuid, p_options_json jsonb',
   'BINDING_A','ede8dfe93aca76d294b59f4ab59211020fec2ac139d6f753a4c9ce1759eb4b46',
   'Paged correction apply owner for the same Binding A evidence set.'),
  ('public','pay_payment_correction_process_chunk',
   'p_correction_request_id uuid, p_limit integer, p_worker_id text, p_actor_user_id uuid',
   'BINDING_A','d07107bbb827348183bd76c5c54347c4763835fdd0c7b4e30816c6acb5ed897f',
   'Ruling 5 correction 2. The installed owner of "CANCELLED plus cancelled_at_utc only when no active item remains, otherwise back to DRAFT"; the pack attributed this to pay_batch_cancel, which does not write.'),
  ('public','pay_payment_cancel_finalise_metadata_v1',
   'p_pay_batch_id uuid, p_correction_request_id uuid, p_actor_user_id uuid, p_reason text',
   'BINDING_A','80d8ad23c671ce70b4a0cf7a24e3e3b382f342c620d277f51ce3ac48fd7cb968',
   'Ruling 5 correction 2. Writes cancelled_at_utc and cancellation metadata only; requires status = CANCELLED already and zero non-voided non-DEBT_CREATED items.'),
-- ---- Binding B: whole-batch cancellation and Draft rebuild ----
  ('public','pay_batch_abort_failed_draft_create_partial',
   'p_operation_id uuid, p_pay_batch_id uuid, p_actor_user_id uuid, p_reason text, p_failure_json jsonb',
   'BINDING_B','1b32ac59bbe61a605e60c9cd8713808d66ec19d9d2651fefc2371e3333124dfb',
   'Aborts a partially created Draft: voids its items, cancels the batch and releases its reservations.'),
  ('private','pay_workbench_draft_overlay_remove_page_v1',
   'p_correction_request_id uuid, p_pay_batch_id uuid, p_session_id uuid, p_after_candidate_id uuid, p_candidate_limit integer, p_finance_row_limit integer, p_actor_user_id uuid, p_options_json jsonb',
   'BINDING_B','c1868e301765ce99d99e69359b99a8433a358e8742d5f835bf77cd2a7524e2c7',
   'Workbench Draft overlay removal: same void, cancel and release evidence set, paged.'),
-- ---- Binding C: PAYE-net reprojection ----
  ('public','pay_set_paye_net_manual',
   'p_pay_batch_id uuid, p_entries_json jsonb, p_actor_user_id uuid',
   'BINDING_C','67bd40c49284fad3c22177987829506b1e36c1edcd392826c000bdbabab68cec',
   'Manual PAYE-net entry: reprojects a Candidate, voiding superseded items and releasing their reservations with released_reason PAYE_NET_REPROJECTION. Ruling 1 requires that item-linked artefact at runtime; membership here is not item-level proof.'),
-- ---- the named settlement and terminality owner ----
  ('public','pay_settle_rail',
   'p_pay_batch_id uuid, p_settlement_json jsonb, p_actor_user_id uuid, p_operation_id uuid, p_settlement_scope_ids jsonb',
   'SETTLEMENT_TERMINALITY_OWNER','0e5c3e38a314945f67ca716c5e6897ca9ed9730d7bbfd8e39342e31bdf26a83f',
   'Ruling 5 correction 3. Sole installed writer of pay_batches.status SETTLED or FAILED and of completed_at_utc; terminality by settlement is proved under proof/32 section 5.2, not under A, B or C.'),
-- ---- the delegator that performs no write ----
  ('public','pay_batch_cancel',
   'p_pay_batch_id uuid, p_actor_user_id uuid, p_reason text, p_correction_request_id uuid, p_work_item_id uuid',
   'VALIDATING_DELEGATOR_NO_WRITE','10c3ffcaab90dddc256d29ab3df82b1d01d6eaf3a302688a6ba82f04234fb573',
   'Ruling 5 correction 1. Five installed arguments. Validates, then delegates to pay_payment_correction_request_start with requested_action DRAFT_CANCEL. Reads is_voided; writes none of the census-relevant columns. Section 8 proves the absence of writes, not merely their omission here.'),
-- ---- the structural fail-closed exclusion ----
  ('public','pay_batch_apply_finance_adjustments',
   'p_pay_batch_id uuid, p_pay_channel_scope text, p_actor_user_id uuid, p_vat_rate_pct numeric, p_week_start date, p_operation_id uuid, p_candidate_scope_ids jsonb',
   'NON_ENUMERATED_BORN_VOIDED_TEMPLATE','a2720729aa856c3846d18aa38aaa1fac6a37c6c7dcc3e9d69911a09f63e64b91',
   'Ruling 2. Classified ONLY through the exact dormant-recovery-template INSERT site, whose shape section 7 asserts. It supplies no release proof and no source-family terminal evidence. Any change giving that branch a Timesheet identity, a transfer, a reservation or a prior payable state invalidates the exclusion and blocks release pending a new ruling.'),
-- ---- non-binding owners ----
  ('public','pay_finance_case_write_off',
   'p_finance_case_id uuid, p_actor_user_id uuid, p_write_off_reason text',
   'NON_BINDING_OWNER','0113b09b0b2fcd060105aa0deac7fbb64db5fc4b6e31fbfeb1b039705258b30c',
   'Ruling 5 correction 5. Releases every RESERVED or COMMITTED reservation of a finance case on a finance_case_id predicate alone, with no batch-state guard. A reservation released with released_reason WRITE_OFF is NOT positive reservation evidence for any binding. Kept visible for the later R01-R12 reconciliation.'),
  ('public','pay_unpay_batch',
   'p_pay_batch_id uuid, p_actor_user_id uuid, p_reason text, p_force boolean',
   'NON_BINDING_OWNER','e777388b90cf884912e69641cd4bebc165046c17deef4a3778fc9211c04db10f',
   'Ruling 5 correction 6. Latent and presently non-committable: its batch status write is UNPAID, which the installed status constraint rejects (section 9). Not a durable safety contract; any change that makes it committable requires Banking Pay design review, recensus and new acceptance evidence.'),
-- ---- census-relevant column writers that are not census writers ----
  ('public','pay_bank_event_ingest',
   'p_event_json jsonb, p_actor_user_id uuid, p_ingest_options_json jsonb',
   'NON_CENSUS_LIFECYCLE_WRITER','323a3171e74129a459f0467e546f51fe738895c600b1168bee3b8e6ecb1b5225',
   'Provider bank-event ingest. Writes transfer status and rail_state from provider events, and re-opens authorisation with status = CASE WHEN status IN (AUTHORISED_FOR_PAYMENT, SCHEDULED, EXECUTING) THEN AWAITING_AUTHORISATION ELSE status END. Never CANCELLED, never SETTLED or FAILED, never a terminality stamp; its settlement effect is by calling pay_settle_rail in full-batch mode.'),
  ('public','pay_bank_transfers_apply_rail_updates',
   'p_pay_batch_id uuid, p_updates jsonb, p_actor_user_id uuid, p_operation_id uuid, p_transfer_ids jsonb, p_chunk_id uuid',
   'NON_CENSUS_LIFECYCLE_WRITER','be5de841a24d5fb68b59a258613ea36a4a52428fc06bbb0c29675acabab1e4b1',
   'Rail outcome applier. Moves transfer status and rail_state between provider outcomes (COMPLETED, FAILED, RETURNED, CANCELLED, SUBMITTED, REVIEW_REQUIRED). Alters transfer evidence the census classifies through _pay_rail_state_money_movement_classify, so it must be inventoried; it voids no item, cancels or terminalises no batch and releases no reservation.'),
  ('public','pay_batch_auth_apply_action',
   'p_auth_request_id uuid, p_actor_user_id uuid, p_action text, p_note text',
   'NON_CENSUS_LIFECYCLE_WRITER','42157edf5d4754dd6b22625c40b233ef682abbf30bcd24bc5b2853ff94b331fb',
   'Authorisation state machine. Writes only READY, AWAITING_AUTHORISATION and AUTHORISED_FOR_PAYMENT.'),
  ('public','pay_batch_finalize_reservations_and_markers',
   'p_pay_batch_id uuid, p_pay_channel_scope text, p_actor_user_id uuid, p_pay_date date, p_week_start date, p_operation_id uuid, p_candidate_scope_ids jsonb',
   'NON_CENSUS_LIFECYCLE_WRITER','0449fe0daa2d6e1b23aa7d991bfce84d84fce30d7d123dff6804cbe528ddc4a4',
   'Creates reservations: status and released_reason appear in an INSERT column list. Creation, never release.'),
  ('public','pay_batch_mark_blocked_funds',
   'p_pay_batch_id uuid, p_actor_user_id uuid, p_funds_check_json jsonb',
   'NON_CENSUS_LIFECYCLE_WRITER','834a12c2c9c1ac75f2ccf2cc2806fafa9a4f0742f509bde4109930ff0d20db43',
   'Marks a batch BLOCKED_FUNDS and returns its COMMITTED reservations to RESERVED. A reservation that moves COMMITTED to RESERVED stays active, so proof/32 section 4.3 C2 still freezes the item; it is never released. Also deletes unbound transfers, covered by section 8.'),
  ('public','pay_batch_schedule',
   'p_pay_batch_id uuid, p_schedule_kind text, p_scheduled_at_utc timestamp with time zone, p_funding_account_ref text, p_warning_hours_json jsonb, p_actor_user_id uuid, p_operation_id uuid, p_freshness_result_hash text',
   'NON_CENSUS_LIFECYCLE_WRITER','ce09e32e1ca706d8b426d15b323c8c7727b9f7d6c40b4a8b1999df0e10b50f4c',
   'Schedules a batch: status SCHEDULED, reservations RESERVED to COMMITTED, plus a rollback branch that restores the values it read. Never a terminal or cancelled state.'),
  ('public','pay_batch_shell_ensure_from_operation',
   'p_operation_id uuid, p_workbench_session_id uuid, p_actor_user_id uuid, p_batch_kind text, p_pay_channel text, p_input_json jsonb',
   'NON_CENSUS_LIFECYCLE_WRITER','f7f065bc97fb61bf4cbc052bbfa623ed0977fe3db726ee19c93fa41eda9d1ae6',
   'Creates the batch shell; status appears in an INSERT column list. Creation, never a transition of an existing batch.'),
  ('public','pay_batches_claim_due_scheduled',
   'p_limit integer, p_now_utc timestamp with time zone',
   'NON_CENSUS_LIFECYCLE_WRITER','51440c38e2552c1c75907c537ca38d337ec99b241f9de510445fc421a9205073',
   'Claims due scheduled batches: SCHEDULED to EXECUTING, guarded on status = SCHEDULED and an uncommitted execution state.'),
  ('public','pay_execute_bank_transfer_chunk_prepare',
   'p_operation_id uuid, p_pay_batch_id uuid, p_transfer_scope_ids jsonb, p_actor_user_id uuid',
   'NON_CENSUS_LIFECYCLE_WRITER','9b276ea60596c256380ad7d8d97ad9c223b66c05069e1b9943703a55d752b67c',
   'Creates bank transfers; its ON CONFLICT branch resets PENDING, BLOCKED or FAILED back to PENDING. Creation and re-preparation, never a money-moved or terminal-no-money classification.'),
  ('public','pay_execute_provider_submit_review_resolve',
   'p_pay_batch_id uuid, p_operation_id uuid, p_resolution_action text, p_confirmation_json jsonb, p_actor_user_id uuid',
   'NON_CENSUS_LIFECYCLE_WRITER','8bdeb4dfe6405fcda5e8bae372cfa79c781bfa4f174393123a77b9e327f6ba86',
   'Resolves a provider submit review by returning a transfer to PENDING with a null rail_state for re-submission.'),
  ('public','pay_payment_advance_create',
   'p_candidate_id uuid, p_principal_amount numeric, p_weekly_due numeric, p_weeks_total integer, p_start_week_start date, p_actor_user_id uuid, p_note text, p_minimum_earnings_threshold numeric, p_take_home_floor_override numeric',
   'NON_CENSUS_LIFECYCLE_WRITER','188d6a5c25331a5ddf24fe3e7efda07462c9f6b28bf67e29abf7c328e7210d7c',
   'Nine-argument overload. Creates a LOANS payout batch with status DRAFT and its items, with is_voided in the INSERT column list. Creation only. The thirteen-argument overload writes no pay_batches row at all and is therefore not in this inventory.'),
  ('public','pay_payment_correction_authorise',
   'p_correction_request_id uuid, p_actor_user_id uuid, p_action text, p_note text',
   'NON_CENSUS_LIFECYCLE_WRITER','452f8286f3d13a99211fac1c389aac0c4eb59a9f9e3befbdcfbd3bba96eab903',
   'Authorises a correction request: pay_batches.status = CASE WHEN requested_action = DRAFT_CANCEL THEN status ELSE AWAITING_AUTHORISATION END. Its CANCELLED literals belong to banking_pay_operations, not to pay_batches; section 6 is statement-scoped and does not confuse the two.'),
  ('public','pay_payment_correction_reauthorisation_overlay_reset_v1',
   'p_correction_request_id uuid, p_operation_id uuid, p_pay_batch_id uuid, p_actor_user_id uuid, p_dry_run boolean',
   'NON_CENSUS_LIFECYCLE_WRITER','2fb24b7b2ffc372fe7530c219b7d656d422e644ec4ccf027ca00afb19a2a8a83',
   'A SECOND installed producer of VOIDED transfers, with amount zeroed, failed_reason CANCELLATION_REAUTHORISATION_OVERLAY_VOIDED and rail_meta_json.cancellation_reauthorisation_overlay_voided = true. These are NOT the pre-bank-cancel markers ruling 4 requires, so such a transfer stays UNKNOWN and cannot satisfy a binding. Inventoried so ruling 4 cannot be satisfied by the wrong producer.'),
  ('public','pay_payment_correction_request_start',
   'p_pay_batch_id uuid, p_selection_json jsonb, p_reason text, p_actor_user_id uuid, p_source_bank_event_id uuid, p_auto_requested boolean, p_accepted_resolution_json jsonb',
   'NON_CENSUS_LIFECYCLE_WRITER','a51e432461f584d5677fc38dcdaeb17fecadf04f0f4a7e72cd297bfeec7efd97',
   'Starts a correction request; same batch-status CASE as pay_payment_correction_authorise. Its CANCELLED literals belong to pay_payment_correction_requests and banking_pay_operations.'),
  ('public','pay_provider_submit_chunk_stage_record',
   'p_operation_id uuid, p_pay_batch_id uuid, p_chunk_id uuid, p_transfer_ids jsonb, p_stage text, p_provider_submit_diagnostic jsonb, p_actor_user_id uuid',
   'NON_CENSUS_LIFECYCLE_WRITER','3c6b04c2f090bd64b9c008de73bd7264172bbe6b6f299f9605b6f3edf881cdd0',
   'Records a provider submit stage on the transfer rail_state only.'),
  ('public','pay_set_paye_net_from_sage',
   'p_pay_batch_id uuid, p_csv_raw text, p_actor_user_id uuid, p_source_filename text',
   'NON_CENSUS_LIFECYCLE_WRITER','c6683bfe3f716382a7d76038f8e4976a7dfccdabfe089cecef1227f400bb67f2',
   'Sage PAYE-net import. Inserts fresh items with false as is_voided and fresh reservations with RESERVED as status and a null released_reason. It reshapes LOAN_REPAYMENT rows by hard delete, which section 8 covers; it never voids an existing item and never releases a reservation.'),
  ('public','pay_settle_manual_confirm',
   'p_pay_batch_id uuid, p_scope text, p_bank_confirm_ref text, p_payment_date date, p_actor_user_id uuid, p_settlement_mode text, p_auth_request_id uuid, p_csv_uploaded_confirmed boolean, p_external_settlement_comment text, p_suppress_remittances boolean, p_operation_id uuid, p_settlement_scope_ids jsonb',
   'NON_CENSUS_LIFECYCLE_WRITER','c7a2033700bdae82e8dc8e6487b283ba854bddb34c6d140083d7db35d1c551ef',
   'Manual settlement confirmation: commits reservations and completes transfers, then calls pay_settle_rail for batch terminality. It writes no batch status of its own.'),
  ('public','_pay_execute_operation_cleanup_failed_local_artifacts_base',
   'p_operation_id uuid, p_actor_user_id uuid, p_failure_phase text, p_failure_error_json jsonb, p_dry_run boolean',
   'NON_CENSUS_LIFECYCLE_WRITER','99411a31ff02fc9f09fc79cab741cd6ce025e18a8c2a177b0571ceaa0a1ca188',
   'Deletes local artefacts of a failed execution. Reaches pay_bank_transfers only through a hard delete guarded on status PENDING, null rail_tx_id, null completed_at_utc, null failed_reason, no remaining operation scope, no referencing pay_batch_items row and no transfer event. Writes no census-relevant column; inventoried for the section 8 delete scan.');

-- ---------------------------------------------------------------------------
-- 2. Observe every census-relevant write, statement-scoped, over MASKED text.
-- ---------------------------------------------------------------------------
-- WHAT THIS SECTION DOES AND, JUST AS IMPORTANTLY, WHAT IT CANNOT DO.
--
-- This is a LEXICAL scan of installed definition text. It is not a parser. Its
-- job is to enumerate, for every routine in public and private, the
-- census-relevant columns that routine ASSIGNS and the evidence rows it REMOVES,
-- so that section 8 can fail the release when a routine does either without
-- being classified in section 1.
--
-- TWO ROUNDS OF INDEPENDENT REVIEW HAVE DEFEATED EARLIER VERSIONS OF THIS SCAN.
-- Both are recorded here because the shape of the mistake matters more than the
-- individual escapes.
--
--   Round 1 (reports/WP-18-19_REVIEW.md F1). The scan sliced RAW definition text
--   with [^;]* and took the SET list as everything between the first "set" and
--   the first "where"/"from". Ten ordinary shapes defeated it: a semicolon inside
--   a literal or a comment truncated the slice; a subquery in an earlier SET
--   expression truncated the assignment list; quoted identifiers matched nothing;
--   TRUNCATE was not scanned; a run-time-built table name cannot be resolved.
--   The fix masked comments and literals before slicing and flattened parentheses
--   before reading the SET list.
--
--   Round 2 (reports/WP-18b_REVIEW.md F1). That fix still required the table name
--   to sit IMMEDIATELY AFTER the verb. PostgreSQL's Unicode-escape identifier
--   U&"pay_batch_items" survived masking as U&pay_batch_items -- the quotes
--   stripped, the U& prefix left glued to the name -- which broke that adjacency,
--   so no slice was produced and a plain function that voids items passed the
--   gate. The same bypass worked for DELETE and TRUNCATE.
--
-- The second failure is the instructive one: the defect was not the U& prefix, it
-- was REQUIRING ADJACENCY AT ALL. Any token that can sit against an identifier and
-- survive masking defeats an adjacency test, and a fix that special-cased U& would
-- have left the class open. So the adjacency requirement is gone. Statements are
-- now sliced by VERB alone, and a statement is treated as targeting an evidence
-- relation when that relation's name appears ANYWHERE IN THE STATEMENT'S TARGET
-- REGION -- the text between the verb and the point where the target list
-- demonstrably ends (SET for UPDATE, the column list or VALUES/SELECT for INSERT,
-- USING for MERGE, WHERE/USING for DELETE, the whole statement for TRUNCATE).
-- U&"..." is then caught without naming it, because "&" is not a word character
-- and \mpay_batch_items\M still matches inside U&pay_batch_items.
--
-- The four stages:
--
--   Stage 0 (targets). The write targets are the four evidence tables, plus any
--   view in public or private that is auto-updatable and whose base relations
--   include one of them: a write through such a view is a write to the table.
--
--   Stage 1 (masking). Each definition is reduced to a text in which every
--   semicolon, verb and identifier that remains is real code:
--     a. dollar-quote DELIMITERS are stripped, not their contents. Contents are
--        kept deliberately: dynamic SQL that names an evidence table is exactly
--        what must be scanned, and blanking it reopens the hole (fixture X02);
--     b. block comments, line comments, E'' literals and '' literals are masked
--        in ONE left-to-right pass, so a quote inside a comment and a comment
--        marker inside a quote each behave correctly;
--     c. identifier double-quotes are stripped ALONG WITH an optional Unicode-
--        escape prefix, so "public"."pay_batch_items" and U&"pay_batch_items"
--        both read as the bare name. Case-folding differences make this
--        over-inclusive, never under-inclusive.
--
--   Stage 2 (slicing). Statements are sliced from the verb to the terminating
--   semicolon. After masking, no semicolon inside a literal or a comment can
--   truncate a slice.
--
--   Stage 3 (targeting and assignment). The target region is tested for each
--   write target by name, with no adjacency requirement. Then, inside the slice:
--     UPDATE - parentheses are flattened to a token, innermost first, until
--       stable, so no subquery or function call in an earlier SET expression can
--       truncate the SET list. The multi-column "set (a, b) = (...)" form is
--       tested on the UNflattened slice, and an ON CONFLICT ... DO UPDATE SET
--       fragment of an INSERT is tested the same way.
--     INSERT - the column is written if it is named in the explicit column list,
--       or produced as an output alias in the feeding SELECT (the born-voided
--       branch uses "true as is_voided"), or the statement has NO explicit column
--       list at all, in which case a positional INSERT can set any column and the
--       scan fails closed.
--     MERGE - treated as a write of every census column.
--     DELETE and TRUNCATE - recorded as row removals for section 8c, from the
--       same masked, sliced text rather than from a separate raw-text regex as
--       before. The old raw-text delete scan was U&-vulnerable in the same way.
--
-- WHAT CAN STILL DEFEAT IT, stated because the previous two versions each claimed
-- more than they delivered. A U&"..." identifier whose BODY carries backslash
-- Unicode escapes (U&"pay\0062atch_items") never spells the relation name in the
-- text at all, and no lexical scan can resolve it; likewise an identifier
-- assembled at run time. Section 8f fails closed on the residual tokens those
-- forms leave behind, and section 8d on run-time-built SQL, so they block the
-- release rather than passing it -- but they are blocked, not understood. Section
-- 10 covers the write paths that are not routines at all. The guarantee paragraph
-- in this file's header states the remaining gaps without softening them.
drop table if exists pg_temp.wp18_write_targets;
create temporary table wp18_write_targets as
select distinct c.table_name, c.table_name as target_relname
from wp18_census_columns as c
union
select distinct c.table_name, v.relname
from wp18_census_columns as c
join pg_catalog.pg_class as base
  on base.relname = c.table_name
join pg_catalog.pg_namespace as bn
  on bn.oid = base.relnamespace and bn.nspname in ('public','private')
join pg_catalog.pg_depend as d
  on d.refobjid = base.oid and d.refclassid = 'pg_class'::regclass
join pg_catalog.pg_rewrite as rw
  on rw.oid = d.objid
join pg_catalog.pg_class as v
  on v.oid = rw.ev_class and v.relkind in ('v','m')
join pg_catalog.pg_namespace as vn
  on vn.oid = v.relnamespace
-- No schema restriction on the VIEW. An earlier version required the view to be
-- in public or private, which meant an auto-updatable view in any other schema
-- passed writes straight through to the evidence table while the routine naming
-- it matched no target and was skipped by the pre-filter entirely
-- (WP-18b_REVIEW_2.md G2). The BASE relation is still required to be an evidence
-- table in public. On the 18 September 2026 build no updatable view over an
-- evidence table exists in any schema, so this widening adds no target today.
where pg_catalog.pg_relation_is_updatable(v.oid, true) <> 0
union
-- G3: inheritance and partition ANCESTORS. A write to a parent affects the
-- child's rows, and the child's name never appears in the statement. Nothing
-- censused this before (WP-18b_REVIEW_2.md G3). Resolved transitively, so a
-- grandparent counts too. None exists on the 18 September 2026 build.
select distinct c.table_name, ancestor.relname
from wp18_census_columns as c
join pg_catalog.pg_class as base
  on base.relname = c.table_name
join pg_catalog.pg_namespace as bn
  on bn.oid = base.relnamespace and bn.nspname in ('public','private')
cross join lateral (
  with recursive up as (
    select inhparent as oid from pg_catalog.pg_inherits where inhrelid = base.oid
    union
    select i.inhparent from pg_catalog.pg_inherits as i join up on i.inhrelid = up.oid
  )
  select up.oid from up
) as anc
join pg_catalog.pg_class as ancestor on ancestor.oid = anc.oid;

drop table if exists pg_temp.wp18_defs;
create temporary table wp18_defs as
with raw as (
  select p.oid,
         n.nspname as schema_name,
         p.proname as routine_name,
         pg_catalog.pg_get_function_identity_arguments(p.oid) as identity_arguments,
         pg_catalog.replace(pg_catalog.pg_get_functiondef(p.oid), chr(13) || chr(10), chr(10)) as definition
  from pg_catalog.pg_proc as p
  join pg_catalog.pg_namespace as n on n.oid = p.pronamespace
  where n.nspname in ('public','private')
    and p.prokind in ('f','p')
)
select oid, schema_name, routine_name, identity_arguments, definition,
       null::text as masked,
       -- c. identifier double-quotes, with an optional Unicode-escape prefix
       pg_catalog.regexp_replace(
         -- b. comments and string literals, one left-to-right pass
         pg_catalog.regexp_replace(
           -- a. dollar-quote delimiters only, contents preserved
           pg_catalog.regexp_replace(definition, '\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$', ' ', 'g'),
           $mask$(/\*(?:[^*]|\*+[^*/])*\*+/)|(--[^\n]*)|([Ee]'(?:[^'\\]|\\.|'')*')|('(?:[^']|'')*')$mask$,
           ' ', 'g'),
         '(?:[Uu]&)?"([A-Za-z_][A-Za-z0-9_]*)"', '\1', 'g') as masked_raw
from raw;

-- Stage 1d. Glue "DO UPDATE" into a single token.
--
-- Statements are sliced by verb, and a slice must stop at the NEXT verb so that a
-- data-modifying CTE ("with x as (insert into a ...) insert into b ...") yields
-- one slice per clause rather than one slice for the whole statement. That cut
-- must not fire on an upsert's ON CONFLICT DO UPDATE, which is part of its INSERT,
-- not a new statement. Gluing the two words means \mupdate\M can no longer match
-- there, because the preceding character is now a word character.
update wp18_defs
set masked = pg_catalog.regexp_replace(masked_raw, '\mdo\s+update\M', 'do_update', 'gi');

drop table if exists pg_temp.wp18_observed;
create temporary table wp18_observed (
  schema_name text,
  routine_name text,
  identity_arguments text,
  table_name text,
  column_name text,
  verb text,
  assignment_fragment text
);

-- Row removals (DELETE, TRUNCATE) are recorded separately for section 8c, from
-- the same masked and sliced text the column scan uses.
drop table if exists pg_temp.wp18_dynamic_sql_acknowledged;
drop table if exists pg_temp.wp18_observed_removals;
create temporary table wp18_observed_removals (
  schema_name text,
  routine_name text,
  identity_arguments text,
  table_name text,
  verb text,
  statement_fragment text
);

do $observe$
declare
  def record;
  col record;
  tgt record;
  chunk text;
  slice text;
  verb text;
  target_region text;
  remainder text;
  flattened text;
  previous text;
  set_list text;
  multi_column text;
  conflict_set text;
  column_list text;
  is_write boolean;
  target_pattern text;
begin
  -- Cheap pre-filter, built from the resolved target list rather than from the
  -- four table names: a routine that writes only through an updatable view never
  -- names a base table, and a hard-coded pre-filter would skip it (fixture X03).
  select '\m(' || pg_catalog.string_agg(distinct target_relname, '|') || ')\M'
  into strict target_pattern
  from wp18_write_targets;

  for def in select * from wp18_defs loop
    if def.masked !~* target_pattern then
      continue;
    end if;

    -- CHUNKING. A single semicolon-terminated statement can hold several write
    -- clauses -- a data-modifying CTE is the common case:
    --
    --   with ins as (insert into a (...) returning id)
    --   insert into b (...) select ... from ins;
    --
    -- Matching the verb with a global regex does NOT work here: the match for the
    -- first verb consumes text up to the semicolon, so the regex engine resumes
    -- past the second clause and it is never seen. That is a silent loss of
    -- coverage, and it cost two real write sites when this scan was rewritten.
    --
    -- So a sentinel is inserted before every write verb and the text is split on
    -- it. Each chunk then begins with exactly one verb, and each chunk is
    -- truncated at its first semicolon. Nothing can be swallowed.
    for chunk in
      select pg_catalog.split_part(part, ';', 1)
      from pg_catalog.unnest(
             pg_catalog.string_to_array(
               pg_catalog.regexp_replace(
                 def.masked,
                 '\m(insert\s+into|update|merge\s+into|delete\s+from|truncate)\M',
                 chr(1) || '\1', 'gi'),
               chr(1))) as part
    loop
      -- Which verb does this chunk open with, and what follows it?
      if chunk ~* '^insert\s+into\M' then
        verb := 'INSERT';
        slice := pg_catalog.regexp_replace(chunk, '(?is)^insert\s+into\M', '');
      elsif chunk ~* '^merge\s+into\M' then
        verb := 'MERGE';
        slice := pg_catalog.regexp_replace(chunk, '(?is)^merge\s+into\M', '');
      elsif chunk ~* '^delete\s+from\M' then
        verb := 'DELETE';
        slice := pg_catalog.regexp_replace(chunk, '(?is)^delete\s+from\M', '');
      elsif chunk ~* '^truncate\M' then
        verb := 'TRUNCATE';
        slice := pg_catalog.regexp_replace(chunk, '(?is)^truncate\M', '');
      elsif chunk ~* '^update\M' then
        verb := 'UPDATE';
        slice := pg_catalog.regexp_replace(chunk, '(?is)^update\M', '');
      else
        continue;
      end if;

      if slice !~* target_pattern then
        continue;
      end if;

      if verb = 'UPDATE' then
        target_region := coalesce(pg_catalog.substring(slice, '(?is)^(.*?)\mset\M'), '');
        if target_region = '' then continue; end if;
        flattened := slice;
        loop
          previous := flattened;
          flattened := pg_catalog.regexp_replace(flattened, '\([^()]*\)', ' _P_ ', 'g');
          exit when flattened = previous;
        end loop;
        set_list := coalesce(
          pg_catalog.substring(flattened, '(?is)\mset\M(.*?)(?:\mwhere\M|\mfrom\M|\mreturning\M|$)'), '');
        multi_column := coalesce(
          pg_catalog.substring(slice, '(?is)\mset\M\s*\(([^)]*)\)\s*='), '');
        for col in select * from wp18_census_columns loop
          if not exists (select 1 from wp18_write_targets t
                         where t.table_name = col.table_name
                           and target_region ~* ('\m' || t.target_relname || '\M')) then
            continue;
          end if;
          if set_list ~* ('(^|[^[:alnum:]_.])' || col.column_name || '\s*=')
             or multi_column ~* ('\m' || col.column_name || '\M') then
            insert into wp18_observed
            values (def.schema_name, def.routine_name, def.identity_arguments,
                    col.table_name, col.column_name, 'UPDATE', pg_catalog.left(set_list, 4000));
          end if;
        end loop;

      elsif verb = 'INSERT' then
        -- The slice begins at the TARGET, not after it, because the target is no
        -- longer consumed by the slice pattern (that adjacency requirement is what
        -- U&"..." defeated). So the column list is read from the text AFTER the
        -- target region, not from the head of the slice.
        target_region := coalesce(
          pg_catalog.substring(slice, '(?is)^(.*?)(?:\(|\mvalues\M|\mselect\M|\mdefault\s+values\M|\moverriding\M|\mwith\M)'),
          slice);
        remainder := pg_catalog.substr(slice, pg_catalog.length(target_region) + 1);
        column_list := coalesce(
          pg_catalog.substring(remainder, '(?is)^\s*\(([^;]*?)\)\s*(?:values|select|overriding|default\s+values|with)'),
          '');
        conflict_set := coalesce(
          pg_catalog.substring(slice, '(?is)\mon\s+conflict\M.*?\mdo_update\M\s*\mset\M(.*)$'), '');
        if conflict_set <> '' then
          flattened := conflict_set;
          loop
            previous := flattened;
            flattened := pg_catalog.regexp_replace(flattened, '\([^()]*\)', ' _P_ ', 'g');
            exit when flattened = previous;
          end loop;
          conflict_set := coalesce(
            pg_catalog.substring(flattened, '(?is)^(.*?)(?:\mwhere\M|\mreturning\M|$)'), flattened);
        end if;
        for col in select * from wp18_census_columns loop
          if not exists (select 1 from wp18_write_targets t
                         where t.table_name = col.table_name
                           and target_region ~* ('\m' || t.target_relname || '\M')) then
            continue;
          end if;
          is_write := column_list ~* ('\m' || col.column_name || '\M')
                   or slice ~* ('(?is)\mas\M\s+' || col.column_name || '\M');
          -- no explicit column list: a positional INSERT can set any column
          if column_list = '' and remainder !~ '^\s*\(' then
            is_write := true;
          end if;
          if conflict_set ~* ('(^|[^[:alnum:]_.])' || col.column_name || '\s*=') then
            is_write := true;
          end if;
          if is_write then
            insert into wp18_observed
            values (def.schema_name, def.routine_name, def.identity_arguments,
                    col.table_name, col.column_name, 'INSERT', pg_catalog.left(slice, 4000));
          end if;
        end loop;

      elsif verb = 'MERGE' then
        target_region := coalesce(pg_catalog.substring(slice, '(?is)^(.*?)\musing\M'), slice);
        for col in select * from wp18_census_columns loop
          if exists (select 1 from wp18_write_targets t
                     where t.table_name = col.table_name
                       and target_region ~* ('\m' || t.target_relname || '\M')) then
            insert into wp18_observed
            values (def.schema_name, def.routine_name, def.identity_arguments,
                    col.table_name, col.column_name, 'MERGE', pg_catalog.left(slice, 4000));
          end if;
        end loop;

      elsif verb = 'DELETE' then
        target_region := coalesce(
          pg_catalog.substring(slice, '(?is)^(.*?)(?:\mwhere\M|\musing\M|\mreturning\M|$)'), slice);
        for tgt in select distinct table_name, target_relname from wp18_write_targets loop
          if target_region ~* ('\m' || tgt.target_relname || '\M') then
            insert into wp18_observed_removals
            values (def.schema_name, def.routine_name, def.identity_arguments,
                    tgt.table_name, 'DELETE', pg_catalog.left(slice, 2000));
          end if;
        end loop;

      elsif verb = 'TRUNCATE' then
        for tgt in select distinct table_name, target_relname from wp18_write_targets loop
          if slice ~* ('\m' || tgt.target_relname || '\M') then
            insert into wp18_observed_removals
            values (def.schema_name, def.routine_name, def.identity_arguments,
                    tgt.table_name, 'TRUNCATE', pg_catalog.left(slice, 2000));
          end if;
        end loop;
      end if;
    end loop;
  end loop;
end
$observe$;
-- ---------------------------------------------------------------------------
-- 3. Every classified owner exists exactly once, with its exact identity.
-- ---------------------------------------------------------------------------
do $identity$
declare
  inventory_row record;
  installed_count integer;
begin
  for inventory_row in select * from wp18_inventory order by schema_name, routine_name loop
    select pg_catalog.count(*)
    into installed_count
    from pg_catalog.pg_proc as p
    join pg_catalog.pg_namespace as n on n.oid = p.pronamespace
    where n.nspname = inventory_row.schema_name
      and p.proname = inventory_row.routine_name
      and p.prokind in ('f','p')
      and pg_catalog.pg_get_function_identity_arguments(p.oid) = inventory_row.identity_arguments;

    if installed_count = 0 then
      raise exception 'WEEKLY_SOURCE_INSTALLED_WRITER_MISSING:%.%(%) classified %',
        inventory_row.schema_name, inventory_row.routine_name,
        inventory_row.identity_arguments, inventory_row.classification;
    end if;
    if installed_count > 1 then
      raise exception 'WEEKLY_SOURCE_INSTALLED_WRITER_DUPLICATE_IDENTITY:%.%(%) has % installed routines with the same identity arguments',
        inventory_row.schema_name, inventory_row.routine_name,
        inventory_row.identity_arguments, installed_count;
    end if;
  end loop;
end
$identity$;

-- Every inventoried owner is SECURITY DEFINER, as census part 1 established.
do $security$
declare
  offending text;
begin
  select pg_catalog.string_agg(
           inventory_row.schema_name || '.' || inventory_row.routine_name, ', '
           order by inventory_row.schema_name, inventory_row.routine_name)
  into offending
  from wp18_inventory as inventory_row
  join pg_catalog.pg_proc as p on p.proname = inventory_row.routine_name
  join pg_catalog.pg_namespace as n
    on n.oid = p.pronamespace and n.nspname = inventory_row.schema_name
  where pg_catalog.pg_get_function_identity_arguments(p.oid) = inventory_row.identity_arguments
    and p.prosecdef is not true;

  if offending is not null then
    raise exception 'WEEKLY_SOURCE_INSTALLED_WRITER_SECURITY_CHANGED:% is no longer SECURITY DEFINER', offending;
  end if;
end
$security$;

-- ---------------------------------------------------------------------------
-- 4. Every classified owner still hashes to its pin, line endings normalised.
-- ---------------------------------------------------------------------------
-- The pin is sha256 over pg_get_functiondef with CRLF collapsed to LF. A clean
-- local build installs LF bodies and an installed TEST database carries CRLF
-- bodies for the older families; normalising makes the one pin hold for both.
do $pins$
declare
  inventory_row record;
  observed_sha text;
  observed_length integer;
begin
  for inventory_row in select * from wp18_inventory order by schema_name, routine_name loop
    select pg_catalog.encode(
             pg_catalog.sha256(
               pg_catalog.convert_to(
                 pg_catalog.replace(pg_catalog.pg_get_functiondef(p.oid), chr(13) || chr(10), chr(10)),
                 'UTF8')),
             'hex'),
           pg_catalog.length(
             pg_catalog.replace(pg_catalog.pg_get_functiondef(p.oid), chr(13) || chr(10), chr(10)))
    into strict observed_sha, observed_length
    from pg_catalog.pg_proc as p
    join pg_catalog.pg_namespace as n on n.oid = p.pronamespace
    where n.nspname = inventory_row.schema_name
      and p.proname = inventory_row.routine_name
      and pg_catalog.pg_get_function_identity_arguments(p.oid) = inventory_row.identity_arguments;

    if observed_sha <> inventory_row.definition_sha256 then
      raise exception
        'WEEKLY_SOURCE_INSTALLED_WRITER_DEFINITION_CHANGED:%.%(%) classified % expected % observed % (% characters, line endings normalised)',
        inventory_row.schema_name, inventory_row.routine_name, inventory_row.identity_arguments,
        inventory_row.classification, inventory_row.definition_sha256, observed_sha, observed_length;
    end if;
  end loop;
end
$pins$;

-- ---------------------------------------------------------------------------
-- 5. pay_batch_cancel is the five-argument delegator and writes nothing.
-- ---------------------------------------------------------------------------
do $delegator$
declare
  argument_count integer;
  write_targets text;
  delegates boolean;
begin
  select pg_catalog.array_length(p.proargtypes, 1),
         pg_catalog.pg_get_functiondef(p.oid) ~* '\mpay_payment_correction_request_start\M'
  into strict argument_count, delegates
  from pg_catalog.pg_proc as p
  join pg_catalog.pg_namespace as n on n.oid = p.pronamespace
  where n.nspname = 'public' and p.proname = 'pay_batch_cancel';

  if argument_count <> 5 then
    raise exception 'WEEKLY_SOURCE_DELEGATOR_SIGNATURE_CHANGED:public.pay_batch_cancel has % arguments, expected 5', argument_count;
  end if;
  if delegates is not true then
    raise exception 'WEEKLY_SOURCE_DELEGATOR_NO_LONGER_DELEGATES:public.pay_batch_cancel no longer reaches pay_payment_correction_request_start';
  end if;

  select pg_catalog.string_agg(distinct observed.table_name || '.' || observed.column_name, ', ' order by observed.table_name || '.' || observed.column_name)
  into write_targets
  from wp18_observed as observed
  where observed.schema_name = 'public' and observed.routine_name = 'pay_batch_cancel';

  if write_targets is not null then
    raise exception 'WEEKLY_SOURCE_DELEGATOR_PERFORMS_WRITE:public.pay_batch_cancel now writes %', write_targets;
  end if;
end
$delegator$;

-- ---------------------------------------------------------------------------
-- 6. Terminality and the cancellation stamp have exactly the owners ruling 5 names.
-- ---------------------------------------------------------------------------
-- HOW MUCH OF SOLE TERMINALITY THIS SECTION ACTUALLY PROVES, precisely, because
-- the first version of this file and its report overstated it (review finding
-- F2).
--
-- 6a keys on a TERMINAL STATUS LITERAL appearing in another routine's
-- pay_batches.status assignment. The named owner itself does not assign a
-- literal: its observed fragment is
--
--   status = v_batch_status, completed_at_utc = CASE WHEN v_batch_status IN
--   ('SETTLED', 'FAILED') THEN ...
--
-- so it assigns a VARIABLE, and the literals 6a keys on are a read inside the
-- neighbouring completed_at_utc CASE. 6a is therefore satisfied by accident for
-- pay_settle_rail, and a second terminality writer that also assigns a variable
-- (v := 'SETTLED'; ... set status = v) would be invisible to 6a.
--
-- Sole terminality is consequently NOT asserted structurally by 6a alone. It
-- rests on three things together: the section 4 hash pin on pay_settle_rail,
-- which fails on any edit to it; 6b, which admits no other writer of
-- completed_at_utc at all and is not literal-dependent; and section 8's
-- completeness, which is only as good as section 2's scan. The review's mutation
-- M06 -- a second terminality owner whose status literal sits after a subquery in
-- the SET list -- escaped 6a, 6b and 8 simultaneously against the first version
-- of this file. It is caught now, by 6b, because section 2 no longer lets a
-- subquery truncate the SET list.
do $terminality$
declare
  offending text;
  expected_cancel_owners text[] := array[
    'private.pay_workbench_draft_overlay_remove_page_v1',
    'public.pay_batch_abort_failed_draft_create_partial',
    'public.pay_payment_cancel_finalise_metadata_v1',
    'public.pay_payment_correction_process_chunk'
  ];
  observed_cancel_owners text[];
begin
  -- 6a. Only pay_settle_rail may assign a terminal batch status literal.
  select pg_catalog.string_agg(distinct observed.schema_name || '.' || observed.routine_name, ', ')
  into offending
  from wp18_observed as observed
  where observed.table_name = 'pay_batches'
    and observed.column_name = 'status'
    and observed.assignment_fragment ~* '''(SETTLED|FAILED)'''
    and not (observed.schema_name = 'public' and observed.routine_name = 'pay_settle_rail');

  if offending is not null then
    raise exception 'WEEKLY_SOURCE_TERMINALITY_OWNER_NOT_SOLE:% assigns a terminal batch status literal; proof/32 section 5.2 names pay_settle_rail as the only settlement and terminality owner', offending;
  end if;

  -- 6b. Only pay_settle_rail may write completed_at_utc.
  select pg_catalog.string_agg(distinct observed.schema_name || '.' || observed.routine_name, ', ')
  into offending
  from wp18_observed as observed
  where observed.table_name = 'pay_batches'
    and observed.column_name = 'completed_at_utc'
    and not (observed.schema_name = 'public' and observed.routine_name = 'pay_settle_rail');

  if offending is not null then
    raise exception 'WEEKLY_SOURCE_TERMINALITY_STAMP_NOT_SOLE:% writes pay_batches.completed_at_utc', offending;
  end if;

  -- 6c. The cancellation stamp has exactly the four owners ruling 5 recognises.
  select pg_catalog.array_agg(distinct observed.schema_name || '.' || observed.routine_name order by observed.schema_name || '.' || observed.routine_name)
  into observed_cancel_owners
  from wp18_observed as observed
  where observed.table_name = 'pay_batches'
    and observed.column_name = 'cancelled_at_utc';

  if observed_cancel_owners is distinct from expected_cancel_owners then
    raise exception 'WEEKLY_SOURCE_CANCELLATION_STAMP_OWNERS_CHANGED:expected % observed %',
      pg_catalog.array_to_string(expected_cancel_owners, ', '),
      pg_catalog.array_to_string(coalesce(observed_cancel_owners, array[]::text[]), ', ');
  end if;
end
$terminality$;

-- ---------------------------------------------------------------------------
-- 7. The born-voided structural exclusion still has ruling 2's exact shape.
-- ---------------------------------------------------------------------------
-- Ruling 2: "The exclusion applies only to the registered installed
-- statement/site that creates the row already voided, with timesheet_id IS NULL,
-- no bound transfer, no item-linked reservation, and no prior payable
-- incarnation ... The exclusion must be keyed to the exact statement/site and
-- shape, not merely to a broad item type that could also describe a payable row."
do $born_voided$
declare
  routine_definition text;
  insert_slice text;
  born_voided_slices integer := 0;
  total_item_inserts integer := 0;
  born_voided_slice text;
  update_assigns_is_voided boolean;
begin
  select pg_catalog.replace(pg_catalog.pg_get_functiondef(p.oid), chr(13) || chr(10), chr(10))
  into strict routine_definition
  from pg_catalog.pg_proc as p
  join pg_catalog.pg_namespace as n on n.oid = p.pronamespace
  where n.nspname = 'public'
    and p.proname = 'pay_batch_apply_finance_adjustments'
    and pg_catalog.pg_get_function_identity_arguments(p.oid)
        = 'p_pay_batch_id uuid, p_pay_channel_scope text, p_actor_user_id uuid, p_vat_rate_pct numeric, p_week_start date, p_operation_id uuid, p_candidate_scope_ids jsonb';

  for insert_slice in
    select m[1]
    from pg_catalog.regexp_matches(
      routine_definition,
      '\minsert\M\s+into\s+public\.pay_batch_items\M([^;]*)', 'g') as m
  loop
    total_item_inserts := total_item_inserts + 1;
    if insert_slice ~* '\mtrue\M\s+as\s+is_voided\M' then
      born_voided_slices := born_voided_slices + 1;
      born_voided_slice := insert_slice;
    end if;
  end loop;

  if total_item_inserts = 0 then
    raise exception 'WEEKLY_SOURCE_BORN_VOIDED_EXCLUSION_SHAPE_CHANGED:no INSERT INTO public.pay_batch_items remains in pay_batch_apply_finance_adjustments';
  end if;
  if born_voided_slices <> 1 then
    raise exception 'WEEKLY_SOURCE_BORN_VOIDED_EXCLUSION_SHAPE_CHANGED:expected exactly 1 born-voided INSERT site, found % of % INSERT INTO public.pay_batch_items statements',
      born_voided_slices, total_item_inserts;
  end if;

  -- the registered site: the dormant recovery template stage
  if born_voided_slice !~* '\mfrom\s+pg_temp\.tmp_pay_build_dormant_recovery_template_stage\M' then
    raise exception 'WEEKLY_SOURCE_BORN_VOIDED_EXCLUSION_SHAPE_CHANGED:the born-voided INSERT no longer reads pg_temp.tmp_pay_build_dormant_recovery_template_stage; the exclusion is keyed to that exact site';
  end if;
  -- already voided
  if born_voided_slice !~* '\mtrue\M\s+as\s+is_voided\M' then
    raise exception 'WEEKLY_SOURCE_BORN_VOIDED_EXCLUSION_SHAPE_CHANGED:the registered site no longer creates the row already voided';
  end if;
  -- null timesheet_id: no Timesheet or root identity
  if born_voided_slice !~* 'null::uuid\s+as\s+timesheet_id\M' then
    raise exception 'WEEKLY_SOURCE_BORN_VOIDED_EXCLUSION_SHAPE_CHANGED:the registered site no longer supplies null::uuid as timesheet_id; a Timesheet identity invalidates the exclusion (ruling 2)';
  end if;
  -- no item-linked reservation
  if born_voided_slice !~* 'null::uuid\s+as\s+reservation_id\M' then
    raise exception 'WEEKLY_SOURCE_BORN_VOIDED_EXCLUSION_SHAPE_CHANGED:the registered site no longer supplies null::uuid as reservation_id; an item-linked reservation invalidates the exclusion (ruling 2)';
  end if;
  -- no bound transfer: pay_bank_transfer_id is absent from the INSERT column list
  if coalesce(
       pg_catalog.substring(born_voided_slice, '(?is)^\s*\(([^;]*?)\)\s*(?:values|select)'), ''
     ) ~* '\mpay_bank_transfer_id\M' then
    raise exception 'WEEKLY_SOURCE_BORN_VOIDED_EXCLUSION_SHAPE_CHANGED:the registered site now names pay_bank_transfer_id; a bound transfer invalidates the exclusion (ruling 2)';
  end if;

  -- no second born-voided route: no UPDATE in this owner assigns is_voided
  select pg_catalog.count(*) > 0
  into update_assigns_is_voided
  from wp18_observed as observed
  where observed.schema_name = 'public'
    and observed.routine_name = 'pay_batch_apply_finance_adjustments'
    and observed.table_name = 'pay_batch_items'
    and observed.column_name = 'is_voided'
    and observed.verb = 'UPDATE';

  if update_assigns_is_voided then
    raise exception 'WEEKLY_SOURCE_BORN_VOIDED_EXCLUSION_SHAPE_CHANGED:pay_batch_apply_finance_adjustments now UPDATEs pay_batch_items.is_voided; the exclusion covers an INSERT site only';
  end if;
end
$born_voided$;

-- ---------------------------------------------------------------------------
-- 8. No unclassified evidence mutation anywhere in public or private.
-- ---------------------------------------------------------------------------
do $unclassified_writes$
declare
  offending text;
begin
  select pg_catalog.string_agg(
           observed.schema_name || '.' || observed.routine_name
           || '(' || observed.identity_arguments || ') writes '
           || observed.table_name || '.' || observed.column_name
           || ' by ' || observed.verb,
           E'\n  ' order by observed.schema_name, observed.routine_name, observed.table_name, observed.column_name)
  into offending
  from (
    select distinct schema_name, routine_name, identity_arguments, table_name, column_name, verb
    from wp18_observed
  ) as observed
  where not exists (
    select 1 from wp18_inventory as inventory_row
    where inventory_row.schema_name = observed.schema_name
      and inventory_row.routine_name = observed.routine_name
      and inventory_row.identity_arguments = observed.identity_arguments
  );

  if offending is not null then
    raise exception E'WEEKLY_SOURCE_UNCLASSIFIED_EVIDENCE_WRITER:\n  %\nRuling 5: every installed writer that can void an item, cancel or terminalise a batch, or alter reservation/transfer evidence consumed by the census must appear in the installed writer inventory with a binding, a named non-A/B/C authority class, or an exact structural fail-closed exclusion.', offending;
  end if;
end
$unclassified_writes$;

-- A hard delete or truncate of an evidence row is an evidence mutation too: a
-- deleted Draft item is invisible to proof/32 section 4.2 enumeration rather
-- than ACTIVE. Ruling 5's "alter reservation/transfer evidence consumed by the
-- census" is read to include it, so the same inventory must cover every deleter.
--
-- That check used to live here and scanned RAW definition text with its own
-- regex. It has moved to section 8c and now consumes section 2's masked, sliced,
-- target-region scan instead. The reason is WP-18b_REVIEW.md F1: a raw-text scan
-- was defeated by U&"pay_advance_reservations" in exactly the way the UPDATE scan
-- was, and two scans of the same thing drift apart. There is now one mechanism.



-- 8c. Row removals: DELETE and TRUNCATE against an evidence relation.
--
-- TRUNCATE removes every evidence row at once and was not scanned at all by the
-- first version of this file (WP-18-19_REVIEW.md F1, fixture M07). Both verbs are
-- now taken from section 2's masked, sliced, target-region scan rather than from
-- a separate raw-text regex: the second review (WP-18b_REVIEW.md F1) showed that
-- the raw-text delete scan was defeated by U&"..." in exactly the way the UPDATE
-- scan was, so the two must share one mechanism or they drift apart again.
do $unclassified_removals$
declare
  offending text;
begin
  select pg_catalog.string_agg(distinct
           removal.schema_name || '.' || removal.routine_name
           || '(' || removal.identity_arguments || ') ' || removal.verb || 's rows from '
           || removal.table_name,
           E'\n  ')
  into offending
  from wp18_observed_removals as removal
  where not exists (
    select 1 from wp18_inventory as inventory_row
    where inventory_row.schema_name = removal.schema_name
      and inventory_row.routine_name = removal.routine_name
      and inventory_row.identity_arguments = removal.identity_arguments
  );

  if offending is not null then
    raise exception E'WEEKLY_SOURCE_UNCLASSIFIED_EVIDENCE_DELETER:\n  %\nA hard delete or truncate of a census evidence row is an unclassified evidence mutation under ruling 5.', offending;
  end if;
end
$unclassified_removals$;

-- 8d. Dynamic SQL: the release REFUSES what it cannot read.
--
-- A text scan cannot see inside SQL that is assembled at run time. The first
-- version of this check fired only on a format() placeholder (%I/%s), which left
-- the commonest form of all wide open: an ordinary single-quoted
-- EXECUTE 'update public.pay_batch_items set is_voided = true ...'. Its string is
-- masked away by stage 1b before the scan runs, so section 2 never sees the
-- write, and 8e and 8f stay silent because the literal is balanced and leaves no
-- identifier artefact. An independent review proved a genuine void through that
-- form, and through concatenated and variable-built variants, passing this file
-- with ok:true (WP-18b_REVIEW_2.md G1).
--
-- There is no text-based fix for that, and this check does not pretend otherwise.
-- What it does instead is REFUSE: any routine in public or private that executes
-- dynamically-constructed SQL must either be a classified owner in section 1, or
-- appear below with its exact identity and a pinned SHA-256 of its installed
-- definition. A NEW dynamic-SQL routine fails the release; an EDIT to an
-- acknowledged one fails the release.
--
-- READ THIS BEFORE RELYING ON IT. Acknowledgement is drift detection, not proof.
-- The twenty-two routines below are recorded because they execute dynamic SQL, not
-- because anyone has proved that what they build cannot touch evidence. Three of
-- them -- public.codex_debug_exec_sql, codex_debug_query_sql and
-- codex_debug_select_sql -- take the SQL to run AS A PARAMETER, are SECURITY
-- DEFINER, and are granted EXECUTE to service_role. On 18 September 2026 it was
-- executed on a disposable clone, inside a rolled-back transaction:
-- codex_debug_exec_sql was handed 'update public.pay_batch_items set is_voided =
-- true where id = ...' and the item's is_voided went false -> true. So at least
-- one installed, service_role-callable routine will perform any evidence write it
-- is asked to. No scan of installed text can bound that, and the guarantee
-- paragraph in this file's header says so rather than implying otherwise.
drop table if exists pg_temp.wp18_dynamic_sql_acknowledged;
create temporary table wp18_dynamic_sql_acknowledged (
  schema_name text not null,
  routine_name text not null,
  identity_arguments text not null,
  definition_sha256 text not null,
  primary key (schema_name, routine_name, identity_arguments)
);
insert into wp18_dynamic_sql_acknowledged
  (schema_name, routine_name, identity_arguments, definition_sha256) values
  ('private','_invoice_candidate_revision_trigger_v2','','bfd729a5eb25bc5fef8c6ed100d0b54f99af4e3e27ece84c1c1680acd55ec8b9'),
  ('private','_invoice_candidate_triggers_install_v2','','c9d4486ef51f8e8e42813a34bcaf378438c77dc620c95457d1e2559bae6fb16a'),
  ('private','_invoice_generation_advance_batch_legacy_20260726','p_claims jsonb, p_now_utc timestamp with time zone','25865e0a1371419207009b54706654934ca476ab6ee2d159cd93367741d112ef'),
  ('private','_invoice_generation_advance_core_v8','p_claims jsonb, p_now_utc timestamp with time zone','bb727a6d5f24e5ec137186e30f547743ffdcf05feb131287c918da2ab90896da'),
  ('private','pay_workbench_correction_held_dirty_job_resolve_v1','p_correction_request_id uuid, p_operation_id uuid, p_session_id uuid, p_route_results_json jsonb, p_options_json jsonb','ba11e89120decf5cfabca5ec2cfdfdc2a416cb9fb2c426548e25d6e4adbd6337'),
  ('private','pay_workbench_financial_scope_dirty_transition_v1','','1add84673ed8baed0023f39af4b7d6cd3346a16d6f5ff7a2aae3066d53e0e6eb'),
  ('public','candidate_workflow_transition_atomic_v1','p_session_id uuid, p_environment text, p_workflow_id uuid, p_action text, p_expected_generation integer, p_payload jsonb, p_idempotency_key text, p_now_utc timestamp with time zone','617c4f658a4e9d99ded9dbca54cc4f90b5ad3ce8621bf2fcdb8de4b7672bc464'),
  ('public','codex_debug_exec_sql','p_sql text, p_statement_timeout_ms integer, p_lock_timeout_ms integer','8366f2d4db00a039928e39fd53876b6dea6f471e51c7b21c160d7e0ed4e44a70'),
  ('public','codex_debug_explain_sql','p_sql text, p_analyze boolean, p_statement_timeout_ms integer, p_lock_timeout_ms integer','561524dc2ea5ce6138561b85162dc479777dc0a6f7802f70551535fbe4c284e6'),
  ('public','codex_debug_pg_stat_statements_snapshot','p_terms text[], p_limit integer','db679b5746ebe9ebddcc3333c21795705944f7a545e903d20c6b9184da58f500'),
  ('public','codex_debug_query_sql','p_sql text, p_limit integer, p_statement_timeout_ms integer, p_lock_timeout_ms integer','fb5a37cf5a1b94f05c5e4e7051100502da9138022dd1afd0232fccc8f90e84d4'),
  ('public','codex_debug_select_sql','p_sql text, p_limit integer','b736ba46a7acc3f1419e8f66695672c0321d4c8173941041856f384c96592a11'),
  ('public','id_consolidation_run_draft_commit','p_id_ref text, p_bank_upload_code text, p_actor_user_id uuid','e930d13cc8fe35cebe82e9e851e83f770bc55176f1cd49b368ab0c319ea308e3'),
  ('public','invoice_issue_one','p_invoice_id uuid, p_actor_user_id uuid','c49c6056de2d20dbf45e5be231a10ad2e5fd91fefc37b5c431e94e355a9b817c'),
  ('public','pay_remittance_maybe_queue_for_trigger','p_pay_batch_id uuid, p_trigger text, p_scope text, p_actor_user_id uuid, p_only_confirmed boolean, p_root_operation_id uuid, p_operation_mode boolean','1745985bd2a722750514f77a5ce5f4ae5744b23dc2c4f0c37f3a8e53faad54db'),
  ('public','pay_timesheet_summary_pay_state_refresh_trigger','','ffcee44c5e17076fbb2764dea5c7b23dd50236a81e402bf0d16d7d3629821ccb'),
  ('public','pay_workbench_candidate_dirty_apply_job_process','p_job_id uuid, p_limit integer','1f03edcfb744072a75aea55a6d2accde8ec446c0b167c8fe152054e257f5200e'),
  ('public','pay_workbench_claim_due_jobs','p_limit integer, p_now_utc timestamp with time zone, p_session_id uuid, p_candidate_id uuid, p_allowed_job_types text[]','8849ce5f2ba63b8fb68fbb82496993cc3a338e994cddcf9f012340db250648c5'),
  ('public','pay_workbench_enqueue_candidate_refresh','p_snapshot_run_id uuid, p_candidate_id uuid, p_reason text, p_actor_user_id uuid, p_payload_json jsonb','afff514075f85f88642783e6b72db24b64e922b4112274473f67e33c92694d79'),
  ('public','pay_workbench_mark_candidate_dirty','','62e25d7548b7c2488b76aa2d189bc07fc986364de5837f775bed3a3a364243e7'),
  ('public','pay_workbench_patch_preview_after_batch_mutation_cancel_safe_v1','p_session_id uuid, p_pay_batch_id uuid, p_operation_type text, p_actor_user_id uuid, p_options_json jsonb','5a7aabeaf72d13d040e05e7b1179f2e10d40587dd553f54a23c04cc08bd66b18'),
  ('public','pay_workbench_preview_rows_materialise_chunk','p_session_id uuid, p_candidate_id uuid, p_cursor_json jsonb, p_limit integer','a956d26311d7eb018b6c55de20049ecca5edb85e1a48e16dd386b10b8d400add');
do $dynamic_sql$
declare
  offending text;
begin
  -- new, or edited since acknowledgement
  select pg_catalog.string_agg(distinct
           def.schema_name || '.' || def.routine_name || '(' || def.identity_arguments || ')'
           || case when ack.schema_name is null then ' [not acknowledged]' else ' [definition changed]' end,
           E'\n  ')
  into offending
  from wp18_defs as def
  left join wp18_dynamic_sql_acknowledged as ack
    on ack.schema_name = def.schema_name
   and ack.routine_name = def.routine_name
   and ack.identity_arguments = def.identity_arguments
  where def.masked ~* '\mexecute\M'
    and not exists (
      select 1 from wp18_inventory as inventory_row
      where inventory_row.schema_name = def.schema_name
        and inventory_row.routine_name = def.routine_name
        and inventory_row.identity_arguments = def.identity_arguments)
    and (ack.schema_name is null
         or ack.definition_sha256 <> pg_catalog.encode(
              pg_catalog.sha256(pg_catalog.convert_to(def.definition, 'UTF8')), 'hex'));

  if offending is not null then
    raise exception E'WEEKLY_SOURCE_UNACKNOWLEDGED_DYNAMIC_SQL:\n  %\nThis routine executes SQL assembled at run time, which no scan of installed text can read. It must be classified in section 1 or acknowledged in section 8d with a pinned definition hash. Acknowledgement records that the routine builds SQL; it does not prove the SQL cannot touch evidence.', offending;
  end if;
end
$dynamic_sql$;
-- 8e. Masking integrity: the backstop for the mask's one fail-open direction.
--
-- Section 2 strips dollar-quote delimiters and keeps their contents, so that
-- dynamic SQL is scanned rather than blanked. The cost is that a dollar-quoted
-- STRING whose contents contain an odd number of apostrophes -- $$don't$$ -- can
-- leave an unbalanced quote behind, and an unbalanced quote makes the literal
-- mask swallow the code that follows it.
--
-- A clean mask consumes every apostrophe. This check therefore fails closed on
-- any routine that names an evidence relation, is not inventoried, and still has
-- an apostrophe left in its masked text. On the 18 September 2026 build the mask
-- closes cleanly on all 1,874 routines in public and private, so this check has
-- no candidates and adds no release block.
do $mask_integrity$
declare
  offending text;
begin
  select pg_catalog.string_agg(distinct
           def.schema_name || '.' || def.routine_name || '(' || def.identity_arguments || ')',
           E'\n  ')
  into offending
  from wp18_defs as def
  where pg_catalog.strpos(def.masked, '''') > 0
    and exists (
      select 1 from (select distinct target_relname from wp18_write_targets) as tgt
      where def.masked ~* ('\m' || tgt.target_relname || '\M')
    )
    and not exists (
      select 1 from wp18_inventory as inventory_row
      where inventory_row.schema_name = def.schema_name
        and inventory_row.routine_name = def.routine_name
        and inventory_row.identity_arguments = def.identity_arguments
    );

  if offending is not null then
    raise exception E'WEEKLY_SOURCE_EVIDENCE_TEXT_NOT_SCANNABLE:\n  %\nThe comment and literal mask did not close cleanly on this routine, so section 2 cannot be trusted to have seen every statement in it. Classify it by hand in section 1 or restructure its quoting.', offending;
  end if;
end
$mask_integrity$;

-- 8f. Identifier-canonicalisation integrity: the backstop for the escape that
-- defeated the previous version of this file.
--
-- Section 2 no longer requires a relation name to sit adjacent to its verb, so
-- U&"pay_batch_items" is caught on its merits. But a Unicode-escape identifier
-- may also carry BACKSLASH ESCAPES IN ITS BODY -- U&"pay\0062atch_items" with an
-- optional UESCAPE clause -- and such a form never spells the relation name in
-- the text at all. No lexical scan can resolve it, and the honest response is to
-- refuse rather than to pretend.
--
-- This therefore fails closed on any non-inventoried routine whose masked text
-- still carries an identifier-quoting artefact: a Unicode-escape introducer, or a
-- double-quote close enough to a write verb to be part of its target list. Stage
-- 1c consumes the ordinary forms, so anything left here is a form this scan does
-- not understand.
--
-- On the 18 September 2026 build no routine in public or private matches either
-- test, so the backstop is armed and silent. Measured, not assumed: of 1,874
-- routines exactly one retains any double-quote at all after masking, and it
-- names no evidence relation.
do $identifier_integrity$
declare
  offending text;
begin
  select pg_catalog.string_agg(distinct
           def.schema_name || '.' || def.routine_name || '(' || def.identity_arguments || ')',
           E'\n  ')
  into offending
  from wp18_defs as def
  where (
      def.masked ~ '[Uu]&'
      or def.masked ~* '\m(update|insert\s+into|merge\s+into|delete\s+from|truncate)\M[^;]{0,40}"'
    )
    and not exists (
      select 1 from wp18_inventory as inventory_row
      where inventory_row.schema_name = def.schema_name
        and inventory_row.routine_name = def.routine_name
        and inventory_row.identity_arguments = def.identity_arguments
    );

  if offending is not null then
    raise exception E'WEEKLY_SOURCE_EVIDENCE_IDENTIFIER_NOT_CANONICAL:\n  %\nThis routine carries a Unicode-escape identifier introducer, or a quoted identifier in a write target list, that section 2 could not reduce to a bare relation name. A lexical scan cannot prove what relation it targets. Classify it by hand in section 1 or rewrite the identifier plainly.', offending;
  end if;
end
$identifier_integrity$;

-- ---------------------------------------------------------------------------
-- 9. pay_unpay_batch is still non-committable.
-- ---------------------------------------------------------------------------
-- Ruling 5 correction 6: "Its current constraint failure is not a durable safety
-- contract. Any change that makes it committable, changes 'UNPAID', relaxes the
-- constraint or introduces a caller requires Banking Pay design review,
-- installed-writer recensus and new acceptance evidence before release."
do $unpay$
declare
  constraint_definition text;
  writes_unpaid boolean;
  installed_callers text;
begin
  select pg_catalog.pg_get_constraintdef(c.oid, true)
  into constraint_definition
  from pg_catalog.pg_constraint as c
  join pg_catalog.pg_class as r on r.oid = c.conrelid
  join pg_catalog.pg_namespace as n on n.oid = r.relnamespace
  where n.nspname = 'public' and r.relname = 'pay_batches'
    and c.contype = 'c' and c.conname = 'pay_batches_status_chk_v2';

  if constraint_definition is null then
    raise exception 'WEEKLY_SOURCE_BATCH_STATUS_CONSTRAINT_MISSING:public.pay_batches_status_chk_v2 is absent; pay_unpay_batch is no longer held non-committable by it';
  end if;
  if constraint_definition ~* '''UNPAID''' then
    raise exception 'WEEKLY_SOURCE_UNPAY_BATCH_BECAME_COMMITTABLE:pay_batches_status_chk_v2 now admits UNPAID: %', constraint_definition;
  end if;

  select pg_catalog.pg_get_functiondef(p.oid) ~* '''UNPAID'''
  into strict writes_unpaid
  from pg_catalog.pg_proc as p
  join pg_catalog.pg_namespace as n on n.oid = p.pronamespace
  where n.nspname = 'public' and p.proname = 'pay_unpay_batch';

  if writes_unpaid is not true then
    raise exception 'WEEKLY_SOURCE_UNPAY_BATCH_BECAME_COMMITTABLE:public.pay_unpay_batch no longer writes the UNPAID status the constraint rejects; its latency is no longer the reason it cannot commit and it needs reclassification';
  end if;

  -- It must also remain caller-less: a new caller is a design change under ruling 5.
  select pg_catalog.string_agg(n.nspname || '.' || p.proname, ', ')
  into installed_callers
  from pg_catalog.pg_proc as p
  join pg_catalog.pg_namespace as n on n.oid = p.pronamespace
  where n.nspname in ('public','private')
    and p.prokind in ('f','p')
    and p.proname <> 'pay_unpay_batch'
    and pg_catalog.pg_get_functiondef(p.oid) ~* '\mpay_unpay_batch\M';

  if installed_callers is not null then
    raise exception 'WEEKLY_SOURCE_UNPAY_BATCH_ACQUIRED_CALLER:% now references public.pay_unpay_batch', installed_callers;
  end if;
end
$unpay$;

-- ---------------------------------------------------------------------------
-- 10. Write paths that are not routines at all.
-- ---------------------------------------------------------------------------
-- WHY THIS SECTION EXISTS. Sections 2 and 8 read pg_proc. An independent review
-- (reports/WP-18b_REVIEW.md F2) installed a rewrite RULE whose action voids items
-- -- a genuine, installed evidence writer -- and this file passed it with ok:true,
-- because a rule's action lives in pg_rewrite.ev_action and is not a routine.
-- Worse than the miss, the class was disclosed nowhere: an approver reading "no
-- routine outside the inventory assigns a census-relevant column" would have
-- taken it to mean no in-database writer at all.
--
-- The response is not to add rules and stop. It is to census EVERY way this
-- database can change evidence without a routine that section 2 scans, and to
-- pin each one so that a new one blocks the release. Six such paths exist in
-- PostgreSQL and all six are enumerated below, with what was measured on the
-- 18 September 2026 build:
--
--   a. REWRITE RULES whose action writes, deletes or truncates evidence.
--      Measured: 0 non-_RETURN rules of any kind in public or private.
--   b. TRIGGERS on the evidence tables. Measured: 19. These are not themselves a
--      gap -- every one of their functions lives in public or private and is
--      therefore scanned by section 2 -- but the BINDING is pinned here, because
--      a new trigger is a new write path even when its function is inventoried.
--   c. TRIGGER FUNCTIONS OUTSIDE public/private. A trigger on any relation whose
--      function lives in another schema executes code section 2 never reads.
--      Measured: 0.
--   d. GENERATED COLUMNS on the evidence tables. A generated column computes its
--      own value on every write. Measured: 0.
--   e. COLUMN DEFAULTS on the eight census columns. A default of true on
--      pay_batch_items.is_voided would create born-voided rows with no writer at
--      all. Measured: 2, both benign, both pinned below by exact expression.
--   f. FOREIGN KEY ACTIONS that delete or rewrite evidence rows. ON DELETE
--      CASCADE declared ON an evidence table means deleting some OTHER table's
--      row removes evidence with no routine involved. Measured: 11 non-default
--      actions on the four evidence tables, of which 2 are CASCADE deletes --
--      pay_bank_transfers via pay_bank_transfers_batch_fkey, and pay_batch_items
--      via pay_batch_items_candidate_fkey. Both are pre-existing Banking Pay
--      schema, neither is a defect, and both are pinned so that a third cannot
--      appear silently.
--
-- This section is a structural census, not a text scan, so none of section 2's
-- lexical limits apply to it. It fails closed on any addition, removal or change.
do $non_routine_write_paths$
declare
  evidence_tables text[] := array['pay_batch_items','pay_batches','pay_advance_reservations','pay_bank_transfers'];
  offending text;
  observed text[];
  expected text[];
begin
  -- 11a. Rules that write, delete or truncate an evidence relation.
  select pg_catalog.string_agg(distinct n.nspname || '.' || c.relname || ' rule ' || r.rulename, E'\n  ')
  into offending
  from pg_catalog.pg_rewrite as r
  join pg_catalog.pg_class as c on c.oid = r.ev_class
  join pg_catalog.pg_namespace as n on n.oid = c.relnamespace
  where n.nspname in ('public','private')
    and r.rulename <> '_RETURN'
    and exists (
      select 1 from (select distinct target_relname from wp18_write_targets) as tgt
      where pg_catalog.pg_get_ruledef(r.oid) ~* (
        '\m(update|insert\s+into|merge\s+into|delete\s+from|truncate)\M[^;]*\m' || tgt.target_relname || '\M')
    );

  if offending is not null then
    raise exception E'WEEKLY_SOURCE_EVIDENCE_WRITING_RULE:\n  %\nA rewrite rule whose action writes, deletes or truncates a census evidence relation is an installed writer under ruling 5, and it is not a routine, so sections 2 and 8 cannot see it. Classify it or remove it.', offending;
  end if;

  -- 11b. Triggers on the four evidence tables, pinned as name -> function.
  select pg_catalog.array_agg(entry order by entry)
  into observed
  from (
    select c.relname || '|' || t.tgname || '|' || tn.nspname || '.' || tp.proname as entry
    from pg_catalog.pg_trigger as t
    join pg_catalog.pg_class as c on c.oid = t.tgrelid
    join pg_catalog.pg_namespace as n on n.oid = c.relnamespace
    join pg_catalog.pg_proc as tp on tp.oid = t.tgfoid
    join pg_catalog.pg_namespace as tn on tn.oid = tp.pronamespace
    where not t.tgisinternal
      and n.nspname = 'public'
      and c.relname = any (evidence_tables)
  ) as triggers;

  expected := array[
    'pay_advance_reservations|trg_bpay_wb_reservations_delete_dirty_v1|private.pay_workbench_financial_scope_dirty_transition_v1',
    'pay_advance_reservations|trg_bpay_wb_reservations_insert_dirty_v1|private.pay_workbench_financial_scope_dirty_transition_v1',
    'pay_advance_reservations|trg_bpay_wb_reservations_update_dirty_v1|private.pay_workbench_financial_scope_dirty_transition_v1',
    'pay_bank_transfers|trg_bpay_wb_transfers_delete_dirty_v1|private.pay_workbench_financial_scope_dirty_transition_v1',
    'pay_bank_transfers|trg_pay_bank_transfers_normalise_status_biu|public._pay_bank_transfers_normalise_status_biu',
    'pay_bank_transfers|trg_ts_summary_pay_cache_transfers_au|public.pay_timesheet_summary_pay_state_refresh_trigger',
    'pay_batch_items|trg_bpay_wb_batch_items_delete_dirty_v1|private.pay_workbench_financial_scope_dirty_transition_v1',
    'pay_batch_items|trg_bpay_wb_batch_items_insert_dirty_v1|private.pay_workbench_financial_scope_dirty_transition_v1',
    'pay_batch_items|trg_bpay_wb_batch_items_update_dirty_v1|private.pay_workbench_financial_scope_dirty_transition_v1',
    'pay_batch_items|trg_retention_capture_pay_batch_items_insert|public.timesheet_financial_retention_capture_trigger_v1',
    'pay_batch_items|trg_retention_capture_pay_batch_items_update|public.timesheet_financial_retention_capture_trigger_v1',
    'pay_batch_items|trg_ts_summary_pay_cache_items_ad|public.pay_timesheet_summary_pay_state_refresh_trigger',
    'pay_batch_items|trg_ts_summary_pay_cache_items_au|public.pay_timesheet_summary_pay_state_refresh_trigger',
    'pay_batches|trg_banking_alert_success_events_pay_batches_insert|public.banking_alert_success_event_capture_pay_batch',
    'pay_batches|trg_banking_alert_success_events_pay_batches_update|public.banking_alert_success_event_capture_pay_batch',
    'pay_batches|trg_bpay_wb_batches_delete_dirty_v1|private.pay_workbench_financial_scope_dirty_transition_v1',
    'pay_batches|trg_retention_capture_pay_batches_insert|public.timesheet_financial_retention_capture_trigger_v1',
    'pay_batches|trg_retention_capture_pay_batches_update|public.timesheet_financial_retention_capture_trigger_v1',
    'pay_batches|trg_ts_summary_pay_cache_batches_au|public.pay_timesheet_summary_pay_state_refresh_trigger'
  ];

  if observed is distinct from expected then
    raise exception E'WEEKLY_SOURCE_EVIDENCE_TRIGGER_SET_CHANGED:\nexpected %\nobserved %\nA trigger on an evidence table is a write path. Re-census it and update this pin.',
      pg_catalog.array_to_string(expected, E'\n  '),
      pg_catalog.array_to_string(coalesce(observed, array[]::text[]), E'\n  ');
  end if;

  -- 11c. Trigger functions outside public and private: code section 2 never reads.
  select pg_catalog.string_agg(distinct tn.nspname || '.' || tp.proname || ' (trigger ' || t.tgname || ')', E'\n  ')
  into offending
  from pg_catalog.pg_trigger as t
  join pg_catalog.pg_proc as tp on tp.oid = t.tgfoid
  join pg_catalog.pg_namespace as tn on tn.oid = tp.pronamespace
  where not t.tgisinternal
    and tn.nspname not in ('public','private');

  if offending is not null then
    raise exception E'WEEKLY_SOURCE_TRIGGER_FUNCTION_OUT_OF_SCOPE:\n  %\nThis trigger executes a function outside public and private, which section 2 does not scan. Bring it into scope or classify it by hand.', offending;
  end if;

  -- 11d. Generated columns on the evidence tables.
  select pg_catalog.string_agg(distinct c.relname || '.' || a.attname, E'\n  ')
  into offending
  from pg_catalog.pg_attribute as a
  join pg_catalog.pg_class as c on c.oid = a.attrelid
  join pg_catalog.pg_namespace as n on n.oid = c.relnamespace
  where n.nspname = 'public'
    and c.relname = any (evidence_tables)
    and a.attnum > 0
    and not a.attisdropped
    and a.attgenerated <> '';

  if offending is not null then
    raise exception E'WEEKLY_SOURCE_EVIDENCE_GENERATED_COLUMN:\n  %\nA generated column computes its own value on every write, with no routine involved. Classify it or remove it.', offending;
  end if;

  -- 11e. Defaults on the eight census columns, pinned by exact expression.
  select pg_catalog.array_agg(entry order by entry)
  into observed
  from (
    select c.relname || '.' || a.attname || ' = ' || pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) as entry
    from pg_catalog.pg_attrdef as ad
    join pg_catalog.pg_class as c on c.oid = ad.adrelid
    join pg_catalog.pg_attribute as a on a.attrelid = ad.adrelid and a.attnum = ad.adnum
    join pg_catalog.pg_namespace as n on n.oid = c.relnamespace
    join wp18_census_columns as cc on cc.table_name = c.relname and cc.column_name = a.attname
    where n.nspname = 'public'
  ) as defaults;

  expected := array[
    'pay_bank_transfers.status = ''PENDING''::text',
    'pay_batch_items.is_voided = false'
  ];

  if coalesce(observed, array[]::text[]) is distinct from expected then
    raise exception E'WEEKLY_SOURCE_CENSUS_COLUMN_DEFAULT_CHANGED:\nexpected %\nobserved %\nA column default writes a census-relevant value with no writer at all; a default of true on is_voided would create born-voided rows outside ruling 2''s exclusion.',
      pg_catalog.array_to_string(expected, E'\n  '),
      pg_catalog.array_to_string(coalesce(observed, array[]::text[]), E'\n  ');
  end if;

  -- 11f. Foreign-key actions declared ON the evidence tables. A CASCADE delete
  -- here removes evidence rows when some other table's row is deleted.
  select pg_catalog.array_agg(entry order by entry)
  into observed
  from (
    select c.relname || '.' || con.conname || ' del=' || con.confdeltype::text || ' upd=' || con.confupdtype::text as entry
    from pg_catalog.pg_constraint as con
    join pg_catalog.pg_class as c on c.oid = con.conrelid
    join pg_catalog.pg_namespace as n on n.oid = c.relnamespace
    where con.contype = 'f'
      and n.nspname = 'public'
      and c.relname = any (evidence_tables)
      and (con.confdeltype::text <> 'a' or con.confupdtype::text <> 'a')
  ) as fks;

  expected := array[
    'pay_advance_reservations.pay_advance_reservations_finance_component_id_fkey del=n upd=a',
    'pay_bank_transfers.pay_bank_transfers_batch_fkey del=c upd=a',
    'pay_bank_transfers.pay_bank_transfers_candidate_fkey del=n upd=a',
    'pay_bank_transfers.pay_bank_transfers_umbrella_fkey del=n upd=a',
    'pay_batch_items.pay_batch_items_candidate_fkey del=c upd=a',
    'pay_batch_items.pay_batch_items_finance_component_id_fkey del=n upd=a',
    'pay_batch_items.pay_batch_items_timesheet_id_fkey del=n upd=a',
    'pay_batch_items.pay_batch_items_umbrella_id_fkey del=n upd=a',
    'pay_batches.pay_batches_created_by_user_id_fkey del=n upd=a',
    'pay_batches.pay_batches_freshness_operation_id_fkey del=n upd=a',
    'pay_batches.pay_batches_monzo_confirmed_by_user_id_fkey del=n upd=a'
  ];

  if coalesce(observed, array[]::text[]) is distinct from expected then
    raise exception E'WEEKLY_SOURCE_EVIDENCE_FK_ACTION_CHANGED:\nexpected %\nobserved %\nON DELETE CASCADE declared on an evidence table removes evidence rows with no routine involved; ON DELETE SET NULL rewrites them. Re-census and update this pin.',
      pg_catalog.array_to_string(expected, E'\n  '),
      pg_catalog.array_to_string(coalesce(observed, array[]::text[]), E'\n  ');
  end if;
end
$non_routine_write_paths$;
-- ---------------------------------------------------------------------------
-- 11. Summary.
-- ---------------------------------------------------------------------------
select jsonb_build_object(
  'ok', true,
  'verification', 'weekly_source_installed_writer_census_v1',
  'inventory_rows', (select pg_catalog.count(*) from wp18_inventory),
  'classifications', (
    select jsonb_object_agg(classification, routine_count)
    from (
      select classification, pg_catalog.count(*) as routine_count
      from wp18_inventory group by classification
    ) as counts),
  'census_columns_scanned', (select pg_catalog.count(*) from wp18_census_columns),
  'routines_scanned', (
    select pg_catalog.count(*)
    from pg_catalog.pg_proc as p
    join pg_catalog.pg_namespace as n on n.oid = p.pronamespace
    where n.nspname in ('public','private') and p.prokind in ('f','p')),
  'observed_write_sites', (select pg_catalog.count(*) from wp18_observed),
  'observed_writer_routines', (
    select pg_catalog.count(*) from (
      select distinct schema_name, routine_name, identity_arguments from wp18_observed) as writers),
  -- write targets = the four evidence tables plus any auto-updatable view over
  -- them. More than four means a view now exposes an evidence table to writes.
  'write_targets_resolved', (select pg_catalog.count(distinct target_relname) from wp18_write_targets),
  'observed_removal_sites', (select pg_catalog.count(*) from wp18_observed_removals),
  'unclassified_writers', 0,
  'unclassified_deleters', 0,
  'scan_kind', 'lexical_text_scan_over_masked_definitions',
  'non_routine_write_paths_censused', 'rules, triggers on evidence tables, trigger functions out of scope, generated columns, census-column defaults, foreign-key actions',
  'forward_guarantee', 'detects an unclassified KNOWN-SHAPE writer whose statement text names a write target, and fails closed on identifiers and dynamic SQL it cannot resolve; it is a lexical scan, not a parser. See section 2 and the header limits for what it cannot detect.'
);

drop table if exists pg_temp.wp18_observed_removals;
drop table if exists pg_temp.wp18_observed;
drop table if exists pg_temp.wp18_defs;
drop table if exists pg_temp.wp18_write_targets;
drop table if exists pg_temp.wp18_inventory;
drop table if exists pg_temp.wp18_census_columns;
reset client_min_messages;
