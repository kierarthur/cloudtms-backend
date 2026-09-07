-- Final Weekly Candidate break-entry authority.
--
-- Some earlier repeatables also own the shared break-entry function and can
-- legitimately be replayed during an UPGRADE when their separate authorities
-- change. Reassert the reviewed 3 September definition last so an upgrade and
-- a clean installation finish with identical pre-route, Electronic, Paper and
-- QR behaviour while import-authoritative records remain protected.

\set ON_ERROR_STOP on

\ir 03092026_1215_candidate_weekly_preroute_break_entry_v1.sql
