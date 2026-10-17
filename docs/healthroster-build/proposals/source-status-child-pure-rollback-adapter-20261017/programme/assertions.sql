-- SYNTHETIC JSON boundary vectors only. No business fixtures or Banking success/economics.
do $source_pure_boundaries$
declare
 f constant uuid:='c9234701-4bed-40ae-a01d-795a27562823';
 r constant uuid:='0fe0a2df-4a2b-47d9-9bf4-13955806677f';
 d constant uuid:='44949c19-688b-454e-a58a-668a2f545521';
 q constant uuid:='bf2504ee-6851-44a3-a04c-3e47fd58e934';
 other_id constant uuid:='1f215dec-1164-4c76-bc41-4d0fd9b617ec';
 j jsonb; got jsonb; reason text; v record; refused boolean; positives integer:=0; negatives integer:=0;
begin
 got:=private.weekly_source_pay_decision_native_status_child_v1(null,f,r,null,null);
 if got is not null then raise exception 'SOURCE_PURE_NULL_RESULT_FAILED';end if;positives:=positives+1;
 foreach reason in array array['NOT_APPLIED_DEPENDENCY','APPLICATION_INCOMPLETE','NATIVE_RESULT_UNKNOWN'] loop
  j:=jsonb_build_object('kind','SOURCE_RESULT_REQUIRED','decision_id',d::text,'request_id',q::text,'reason',reason);
  got:=private.weekly_source_pay_decision_native_status_child_v1(j,f,r,d,q);
  if got is distinct from j then raise exception 'SOURCE_PURE_CLOSED_REQUIRED_ROUNDTRIP_FAILED';end if;positives:=positives+1;
 end loop;
 j:=jsonb_build_object('kind','SOURCE_RESULT_REQUIRED','decision_id',d::text,'request_id',q::text,'reason','NATIVE_RESULT_UNKNOWN');
 for v in select * from (values
  (null::jsonb,f,r,d,null::uuid,'SOURCE_NATIVE_OWNERSHIP_CONTRADICTION'),
  (null::jsonb,f,r,null::uuid,q,'SOURCE_NATIVE_OWNERSHIP_CONTRADICTION'),
  ('null'::jsonb,f,r,d,q,'SOURCE_NATIVE_STATUS_INVALID'),
  ('[]'::jsonb,f,r,d,q,'SOURCE_NATIVE_STATUS_INVALID'),
  (j-'request_id',f,r,d,q,'SOURCE_NATIVE_STATUS_INVALID'),
  (j||'{"extra":true}'::jsonb,f,r,d,q,'SOURCE_NATIVE_STATUS_INVALID'),
  (j,f,r,other_id,q,'SOURCE_NATIVE_STATUS_INVALID'),
  (j,f,r,d,other_id,'SOURCE_NATIVE_STATUS_INVALID'),
  (jsonb_set(j,'{reason}','"APPROVED"'::jsonb),f,r,d,q,'SOURCE_NATIVE_STATUS_INVALID'),
  (j,null::uuid,r,d,q,'SOURCE_NATIVE_STATUS_INVALID'),
  (jsonb_set(j,'{decision_id}','123'::jsonb),f,r,d,q,'SOURCE_NATIVE_STATUS_INVALID'),
  (jsonb_set(j,'{request_id}','"BF2504EE-6851-44A3-A04C-3E47FD58E934"'::jsonb),f,r,d,q,'SOURCE_NATIVE_STATUS_INVALID')
 ) cases(source,family_id,run_id,decision_id,request_id,expected_message) loop
  refused:=false;
  begin
   perform private.weekly_source_pay_decision_native_status_child_v1(v.source,v.family_id,v.run_id,v.decision_id,v.request_id);
  exception when sqlstate '55000' then
   if sqlerrm is distinct from v.expected_message then raise exception 'SOURCE_PURE_WRONG_REFUSAL';end if;
   refused:=true;
  end;
  if not refused then raise exception 'SOURCE_PURE_MALFORMED_BOUNDARY_ACCEPTED';end if;
  negatives:=negatives+1;
 end loop;
 if positives<>4 or negatives<>12 then raise exception 'SOURCE_PURE_VECTOR_CENSUS_CHANGED';end if;
end;$source_pure_boundaries$;
select 'HR_SOURCE_STATUS_CHILD_PURE_PASS_4_12'::text as result;
