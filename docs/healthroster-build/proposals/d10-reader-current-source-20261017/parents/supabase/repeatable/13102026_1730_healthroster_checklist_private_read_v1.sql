-- Adjacent owner-private facts for the single native Checklist; no public route.
\set ON_ERROR_STOP on
begin;
\ir healthroster_wp_u3_source_proposals/wp_u3_submission_context.sqlinc
\ir healthroster_wp_u3_source_proposals/wp_u3_signed_submission_read_context.sqlinc
\ir healthroster_wp_u3_source_proposals/wp_u3_check_only_query_rows.sqlinc
\ir healthroster_wp_u3_source_proposals/wp_u3_client_calendar.sqlinc
\ir healthroster_wp_u3_source_proposals/wp_u3_client_week_context.sqlinc
commit;
