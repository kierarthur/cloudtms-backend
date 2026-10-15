\set ON_ERROR_STOP on
\ir healthroster_checklist_current/central_acl_inventory.sqlinc
begin;
\ir healthroster_checklist_current/final_acl_union.sqlinc
commit;
