-- Final Weekly Candidate break-entry authority.
--
-- Some earlier repeatables also own the shared break-entry function and can
-- legitimately be replayed during an UPGRADE when their separate authorities
-- change. Reassert the Daily-aware public readers followed by the reviewed
-- 3 September private definition so an upgrade and a clean installation finish
-- with identical Daily, pre-route, Electronic, Paper and QR behaviour while
-- import-authoritative records remain protected.

\set ON_ERROR_STOP on

\ir 02092026_0325_candidate_paper_break_entry_v1.sql
\ir 03092026_1215_candidate_weekly_preroute_break_entry_v1.sql
