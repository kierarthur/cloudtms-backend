-- Rollback-only first-use controls for privacy-safe financial failure details.
create temporary table ws_verify_drift_control(id integer primary key, label text, amount numeric, payload jsonb) on commit drop;
insert into ws_verify_drift_control values(1,'SENSITIVE_BASELINE',12.5,'{"private":"DO_NOT_RETURN"}'),(2,null,0,null);
do $control$
declare b jsonb;a jsonb;d jsonb;r jsonb;name text:=('pg_temp.ws_verify_drift_control'::regclass)::text;
begin
 b:=jsonb_build_object(name,pg_temp.ws_verify_full_relation_fingerprint('pg_temp.ws_verify_drift_control'::regclass));
 if pg_temp.ws_verify_financial_drift_capture('pg_temp.ws_verify_drift_control'::regclass,'BEFORE') is distinct from b->>name then
   raise exception 'CONTROL_COMPLETE_ROW_HASH_MISMATCH';
 end if;
 d:=pg_temp.ws_verify_financial_drift_detail(b,b,1)::jsonb;
 if d->'relations' is distinct from '[]'::jsonb then raise exception 'CONTROL_UNCHANGED_NOT_EMPTY';end if;
 update pg_temp.ws_verify_drift_control set amount=13.5,label='SENSITIVE_NEW' where id=1;
 delete from pg_temp.ws_verify_drift_control where id=2;
 insert into pg_temp.ws_verify_drift_control values(3,'SENSITIVE_INSERT',25,'{"private":"NEW_PRIVATE"}');
 a:=jsonb_build_object(name,pg_temp.ws_verify_full_relation_fingerprint('pg_temp.ws_verify_drift_control'::regclass));
 d:=pg_temp.ws_verify_financial_drift_detail(b,a,2)::jsonb;r:=d->'relations'->0;
 if jsonb_array_length(d->'relations')<>1 or d->>'worker_call'<>'2'
    or r->>'added_rows'<>'1' or r->>'removed_rows'<>'1' or r->>'changed_rows'<>'1'
    or r->'changed_fields_row_counts' is distinct from '{"amount":1,"label":1}'::jsonb
    or r->>'before_matches_guard'<>'true' or r->>'after_matches_guard'<>'true'
    or d::text like '%SENSITIVE%' or d::text like '%PRIVATE%' or d::text like '%13.5%' then
   raise exception 'CONTROL_DETAIL_COUNTS_OR_PRIVACY_FAILED';
 end if;
 -- A changed read-back is flagged, never misrepresented as the original guard.
 update pg_temp.ws_verify_drift_control set payload='{"private":"SECOND_PRIVATE"}' where id=1;
 d:=pg_temp.ws_verify_financial_drift_detail(b,a,2)::jsonb;
 if d->'relations'->0->>'after_matches_guard'<>'false' then
   raise exception 'CONTROL_LATER_SNAPSHOT_MISMATCH_NOT_REPORTED';
 end if;
 -- Composite keys, NULL and row-associated value swaps all remain visible.
 create temporary table ws_verify_drift_composite(a int,b int,val text,primary key(a,b)) on commit drop;
 insert into pg_temp.ws_verify_drift_composite values(1,1,null),(1,2,'');
 name:=('pg_temp.ws_verify_drift_composite'::regclass)::text;
 b:=jsonb_build_object(name,pg_temp.ws_verify_financial_drift_capture('pg_temp.ws_verify_drift_composite'::regclass,'BEFORE'));
 update pg_temp.ws_verify_drift_composite t set val=case t.b when 1 then '' else null end;
 a:=jsonb_build_object(name,pg_temp.ws_verify_full_relation_fingerprint('pg_temp.ws_verify_drift_composite'::regclass));
 d:=pg_temp.ws_verify_financial_drift_detail(b,a,3)::jsonb;r:=d->'relations'->0;
 if r->>'changed_rows'<>'2' or r->'changed_fields_row_counts' is distinct from '{"val":2}'::jsonb
    or r->>'added_rows'<>'0' or r->>'removed_rows'<>'0' then
   raise exception 'CONTROL_COMPOSITE_NULL_SWAP_NOT_EXACT';
 end if;
end $control$;
