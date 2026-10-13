-- PROPOSAL: root must integrate the complete controlling Money/Report union first.
\set ON_ERROR_STOP on
begin;
\ir source/00_adjustment_owners.sqlinc
\ir source/01_invoice_apply_edits.sqlinc
\ir source/02_invoice_allocation_assert.sqlinc
\ir source/03_invoice_issue_validate.sqlinc
\ir source/04_invoice_move_atomic.sqlinc
\ir source/05_invoice_edit_context.sqlinc
\ir source/06_native_acl.sqlinc
\ir source/07_new_owner_acl.sqlinc
commit;
