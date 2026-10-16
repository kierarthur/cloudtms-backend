-- Proposed source only. Root must reserve/register this slot after Contact1210.
\set ON_ERROR_STOP on
begin;
\ir includes/weekly_contact_manual_enquiry_office_rows_v1.sqlinc
commit;
