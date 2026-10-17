-- SOURCE-ONLY, NOT_RUN. Pure census vectors for the 011 prebody predicate.
-- These rows are not paid evidence, admitted choices, fixture producers or a
-- finalisation proof. Root must separately prove the actual complete owner,
-- genuine choice/audit/I-1 reproof and invoice/pay outcomes in its native lane.
with cases(name,expected_refusal,expected_rows,rows_json,choices_json) as(values
 ('one-normal-one-none',false,2,
  '[{"row":"old","event":"paid","effect":"NORMAL"},{"row":"new1","event":"paid","effect":"NONE","choice":"c1"}]'::jsonb,
  '[{"id":"c1","row":"new1","event":"paid","effect":"NONE","separate":false}]'::jsonb),
 ('one-normal-two-none',false,3,
  '[{"row":"old","event":"paid","effect":"NORMAL"},{"row":"new1","event":"paid","effect":"NONE","choice":"c1"},{"row":"new2","event":"paid","effect":"NONE","choice":"c2"}]'::jsonb,
  '[{"id":"c1","row":"new1","event":"paid","effect":"NONE","separate":false},{"id":"c2","row":"new2","event":"paid","effect":"NONE","separate":false}]'::jsonb),
 ('duplicate-normal',true,2,
  '[{"row":"old","event":"paid","effect":"NORMAL"},{"row":"other","event":"paid","effect":"NORMAL"}]'::jsonb,'[]'::jsonb),
 ('none-cannot-reuse-another-rows-choice',true,3,
  '[{"row":"old","event":"paid","effect":"NORMAL"},{"row":"new1","event":"paid","effect":"NONE","choice":"c1"},{"row":"new2","event":"paid","effect":"NONE","choice":"c1"}]'::jsonb,
  '[{"id":"c1","row":"new1","event":"paid","effect":"NONE","separate":false}]'::jsonb),
 ('none-target-mismatch',true,2,
  '[{"row":"old","event":"paid","effect":"NORMAL"},{"row":"new1","event":"paid","effect":"NONE","choice":"c1"}]'::jsonb,
  '[{"id":"c1","row":"new1","event":"foreign","effect":"NONE","separate":false}]'::jsonb),
 ('none-missing-choice',true,2,
  '[{"row":"old","event":"paid","effect":"NORMAL"},{"row":"new1","event":"paid","effect":"NONE","choice":"c1"}]'::jsonb,'[]'::jsonb),
 ('resolution-total-still-required',true,3,
  '[{"row":"old","event":"paid","effect":"NORMAL"},{"row":"new1","event":"paid","effect":"NONE","choice":"c1"}]'::jsonb,
  '[{"id":"c1","row":"new1","event":"paid","effect":"NONE","separate":false}]'::jsonb),
 ('distinct-separate-normal-remains-payable',false,2,
  '[{"row":"old","event":"paid","effect":"NORMAL"},{"row":"separate","event":"new-event","effect":"NORMAL"}]'::jsonb,'[]'::jsonb)
), evaluated as(
 select cases.name,cases.expected_refusal,
   jsonb_array_length(rows_json)<>expected_rows
   or exists(select 1 from jsonb_array_elements(rows_json) r where r->>'effect'='NORMAL'
     group by r->>'event' having count(*)<>1)
   or exists(select 1 from jsonb_array_elements(rows_json) r where r->>'effect'='NONE'
     and not exists(select 1 from jsonb_array_elements(choices_json) c
       where c->>'id'=r->>'choice' and c->>'row'=r->>'row' and c->>'event'=r->>'event'
         and c->>'effect'='NONE' and c->'separate'='false'::jsonb)) as actual_refusal
 from cases
)
select name,expected_refusal,actual_refusal,actual_refusal=expected_refusal as census_vector_pass
from evaluated order by name;
