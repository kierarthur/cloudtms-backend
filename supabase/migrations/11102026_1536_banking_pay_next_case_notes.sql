-- FC-03: additive metadata on the existing immutable receipt. No row rewrites.
\set ON_ERROR_STOP on
begin;
alter table private.bpay_next_case_create_request
  add column note text,
  add column original_liability_reference text,
  add column preview_token jsonb,
  add constraint bpay_next_case_create_note_check check(note is null or octet_length(note)<=2048),
  add constraint bpay_next_case_create_liability_reference_check check(original_liability_reference is null
    or (case_kind='MANUAL_DEBT' and octet_length(original_liability_reference) between 1 and 2048 and btrim(original_liability_reference)<>'')),
  add constraint bpay_next_case_create_preview_token_check check(preview_token is null
    or (jsonb_typeof(preview_token)='object' and octet_length(preview_token::text)<=24576));
commit;
