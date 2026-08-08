-- Reassert the latest targeted enqueue authority after the older consolidated
-- Banking Pay repeatables.  The included function establishes the exact
-- pending session-candidate state required by certified source publication.
-- Reasserted after the final removal of the monolith's obsolete prepare drop.
\ir 07082026_1017_pay_workbench_enqueue_candidate_refresh.sql
