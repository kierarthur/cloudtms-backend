begin;

-- CloudTMS authenticates application users from public.tms_users. The original
-- preferences table incorrectly referenced auth.users, which is not populated
-- by the custom CloudTMS session system and made every preference save fail.
alter table public.banking_alert_user_preferences
    drop constraint if exists banking_alert_user_preferences_user_id_fkey;

alter table public.banking_alert_user_preferences
    drop constraint if exists banking_alert_user_preferences_user_fkey;

alter table public.banking_alert_user_preferences
    add constraint banking_alert_user_preferences_user_fkey
    foreign key (user_id)
    references public.tms_users(id)
    on delete cascade;

commit;
