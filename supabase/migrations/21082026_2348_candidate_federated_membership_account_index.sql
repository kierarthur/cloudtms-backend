-- Cover the agency-local membership link's account foreign key so account
-- state changes and referential checks remain bounded.

create index if not exists candidate_app_global_membership_links_account_idx
  on public.candidate_app_global_membership_links(account_id);
