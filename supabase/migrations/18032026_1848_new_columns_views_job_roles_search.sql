-- ============================================================================
-- Candidate job-title exact-filter support
-- Safe to rerun
-- - appends new columns to the RIGHT of existing view columns only
-- - preserves existing column order and existing labels
-- - adds reverse lookup index for job_title_id -> candidate_id
-- ============================================================================

create or replace view public.candidates_summary as
select
    c.id,
    c.tms_ref,
    c.first_name,
    c.last_name,
    c.display_name,
    c.email,
    c.phone,
    c.pay_method,
    c.umbrella_id,
    u.name as umbrella_name,
    c.active,
    c.created_at,
    c.updated_at,
    c.key_norm,
    c.mileage_pay_rate,
    c.account_holder,
    c.bank_name,
    c.sort_code,
    c.account_number,
    c.roles,
    c.notes,
    c.rev,
    c.job_title_id,
    c.prof_reg_number,
    c.prof_reg_type,
    c.ni_number,
    c.date_of_birth,
    c.gender,
    c.address_line1,
    c.address_line2,
    c.address_line3,
    c.town_city,
    c.county,
    c.postcode,
    c.country,
    coalesce(jt_primary.label, jt_legacy.label) as primary_job_title,
    string_agg(
        jt_all.label,
        '; '::text
        order by
            case when cjt_all.is_primary then 0 else 1 end,
            lower(jt_all.label)
    ) as job_titles_display,
    c.tms_ref_num,
    coalesce(
        array_agg(distinct cjt_all.job_title_id order by cjt_all.job_title_id)
            filter (where cjt_all.job_title_id is not null),
        case
            when c.job_title_id is not null then array[c.job_title_id]
            else '{}'::uuid[]
        end
    ) as job_title_ids,
    coalesce(
        (
            array_agg(distinct cjt_primary.job_title_id order by cjt_primary.job_title_id)
                filter (where cjt_primary.job_title_id is not null)
        )[1],
        c.job_title_id
    ) as primary_job_title_id
from public.candidates c
left join public.umbrellas u
    on u.id = c.umbrella_id
left join public.candidate_job_titles cjt_all
    on cjt_all.candidate_id = c.id
left join public.default_job_titles jt_all
    on jt_all.id = cjt_all.job_title_id
left join public.candidate_job_titles cjt_primary
    on cjt_primary.candidate_id = c.id
   and cjt_primary.is_primary = true
left join public.default_job_titles jt_primary
    on jt_primary.id = cjt_primary.job_title_id
left join public.default_job_titles jt_legacy
    on jt_legacy.id = c.job_title_id
group by
    c.id,
    c.tms_ref,
    c.first_name,
    c.last_name,
    c.display_name,
    c.email,
    c.phone,
    c.pay_method,
    c.umbrella_id,
    u.name,
    c.active,
    c.created_at,
    c.updated_at,
    c.key_norm,
    c.mileage_pay_rate,
    c.account_holder,
    c.bank_name,
    c.sort_code,
    c.account_number,
    c.roles,
    c.notes,
    c.rev,
    c.job_title_id,
    c.prof_reg_number,
    c.prof_reg_type,
    c.ni_number,
    c.date_of_birth,
    c.gender,
    c.address_line1,
    c.address_line2,
    c.address_line3,
    c.town_city,
    c.county,
    c.postcode,
    c.country,
    jt_primary.label,
    jt_legacy.label,
    c.tms_ref_num;

create or replace view public.candidates_summary_activity as
select
    cs.id,
    cs.tms_ref,
    cs.first_name,
    cs.last_name,
    cs.display_name,
    cs.email,
    cs.phone,
    cs.pay_method,
    cs.umbrella_id,
    cs.umbrella_name,
    cs.active,
    cs.created_at,
    cs.updated_at,
    cs.key_norm,
    cs.mileage_pay_rate,
    cs.account_holder,
    cs.bank_name,
    cs.sort_code,
    cs.account_number,
    cs.roles,
    cs.notes,
    cs.rev,
    cs.job_title_id,
    cs.prof_reg_number,
    cs.prof_reg_type,
    cs.ni_number,
    cs.date_of_birth,
    cs.gender,
    cs.address_line1,
    cs.address_line2,
    cs.address_line3,
    cs.town_city,
    cs.county,
    cs.postcode,
    cs.country,
    cs.primary_job_title,
    cs.job_titles_display,
    car.is_currently_working,
    car.last_timesheet_week_ending,
    cs.tms_ref_num,
    cs.job_title_ids,
    cs.primary_job_title_id
from public.candidates_summary cs
left join public.candidate_activity_rollup car
    on car.candidate_id = cs.id;

create index if not exists idx_candidate_job_titles_job_title_candidate
    on public.candidate_job_titles (job_title_id, candidate_id);
