begin;

create index if not exists import_review_weekly_validation_resolution_hr_row_idx
  on public.import_review_weekly_validation_resolutions(hr_row_id);

commit;
