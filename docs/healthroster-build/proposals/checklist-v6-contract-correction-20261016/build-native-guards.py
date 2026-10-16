"""Exact bounded native body amendment; never imports another builder's body."""
from pathlib import Path
import hashlib,json
base=Path('\\\\?\\'+str(Path(__file__).resolve().parent))
root=base.parents[3]
canonical=root.parent/'healthroster-install2-14102026'
source=canonical/'supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql'
s=source.read_text(encoding='utf-8')
receipts=[];blocks=[]
prior_file=base/'NATIVE_GUARD_INPUTS.json'
prior={x['signature']:x['inputSha256'] for x in json.loads(prior_file.read_text(encoding='utf-8'))['functions']} if prior_file.exists() else {}
for name in ['timesheet_archive_row_guard_v1','timesheet_archived_evidence_guard_v1']:
 start=s.index('CREATE OR REPLACE FUNCTION public.'+name+'()')
 end=s.index('$function$;',start)+len('$function$;')
 original=s[start:end]
 signature='public.'+name+'()'
 if signature in prior and hashlib.sha256(original.encode()).hexdigest()!=prior[signature]:
  raise RuntimeError('Native owner changed; focused reconciliation required: '+signature)
 if original.count('\nBEGIN\n')!=1:raise RuntimeError('Native guard anchor ambiguous: '+name)
 condition="TG_OP IN ('INSERT','UPDATE')"
 if name=='timesheet_archived_evidence_guard_v1':
  condition+=" AND EXISTS (SELECT 1 FROM public.timesheets t WHERE t.timesheet_id=ANY(v_timesheet_ids) AND t.archived_at_utc IS NOT NULL)"
 body=original.replace('\nBEGIN\n','''
  v_move_capability uuid;
BEGIN
  -- V6 exact one-use acceptance arm. All original ordinary refusal logic follows unchanged.
  IF '''+condition+''' THEN
    v_move_capability:=private.timesheet_contract_move_capability_resolve_v1(
      'public.'''+name+'''',TG_RELID,TG_OP,CASE WHEN TG_OP='UPDATE' THEN to_jsonb(OLD) ELSE NULL END,to_jsonb(NEW));
    IF v_move_capability IS NOT NULL THEN
      PERFORM private.timesheet_contract_move_capability_consume_v1(v_move_capability,
        'public.'''+name+'''',TG_RELID,TG_OP,CASE WHEN TG_OP='UPDATE' THEN to_jsonb(OLD) ELSE NULL END,to_jsonb(NEW));
      RETURN NEW;
    END IF;
  END IF;
''',1)
 blocks.append(body+'\nREVOKE ALL ON FUNCTION public.'+name+'() FROM PUBLIC,anon,authenticated,service_role;\n')
 receipts.append({'signature':'public.'+name+'()','inputPath':str(source).removeprefix('\\\\?\\'),
  'inputSha256':hashlib.sha256(original.encode()).hexdigest(),'outputSha256':hashlib.sha256(body.encode()).hexdigest(),
  'exactChange':'One DECLARE variable and exact resolver/consume arm before original BEGIN body; evidence arm applies only to native archived rows, leaving normal B admission intact; all original body bytes retained.'})
blocks.append('''-- New Health correction-event guard. The existing evidence guard is attached
-- only to timesheet_evidence; it is never misrepresented as a native TF trigger.
-- Outside exact live correction-context rows, ordinary native admission is unchanged.
create or replace function private.timesheet_contract_move_financial_row_guard_v1()
returns trigger language plpgsql security definer set search_path='pg_catalog','public','private','pg_temp'
as $function$
declare c private.timesheet_contract_move_execution_contexts%rowtype; cap_id uuid; named boolean:=false;
begin
 c:=private.timesheet_contract_move_context_v1();if c.id is null then return new;end if;
 if c.phase in('ACCEPT_PLACEMENT','DAILY_RATE_REVISION') then
  named:=exists(select 1 from jsonb_array_elements(c.native_manifest_json->'members') m
   where (tg_op='INSERT' and (m->>'to_timesheet_id')::uuid=new.timesheet_id)
    or (tg_op='UPDATE' and (m->>'from_financial_id')::uuid=old.id));
 elsif c.phase in('INVOICE_SELECTED_EDIT','INVOICE_SELECTED_RELOCATE') and tg_op='UPDATE' then
  named:=exists(select 1 from jsonb_array_elements(c.native_manifest_json->'members') m
   where (m->>'from_financial_id')::uuid=old.id);
 end if;
 if not named then return new;end if;
 cap_id:=private.timesheet_contract_move_capability_resolve_v1(
  'private.timesheet_contract_move_financial_row_guard_v1',tg_relid,tg_op,
  case when tg_op='UPDATE' then to_jsonb(old) else null end,to_jsonb(new));
 if cap_id is null then raise exception 'CONTRACT_MOVE_FINANCIAL_CAPABILITY_REQUIRED' using errcode='42501';end if;
 perform private.timesheet_contract_move_capability_consume_v1(cap_id,
  'private.timesheet_contract_move_financial_row_guard_v1',tg_relid,tg_op,
  case when tg_op='UPDATE' then to_jsonb(old) else null end,to_jsonb(new));
 return new;
end;
$function$;
-- Runs after the native trg_set_updated_at_tsfin so full NEW hashes seal its
-- actual transaction_timestamp() value. No original timestamp trigger is edited.
create or replace trigger zz_timesheet_contract_move_financial_row_guard_v1 before insert or update on public.timesheets_financials
 for each row execute function private.timesheet_contract_move_financial_row_guard_v1();
''')
(base/'forward/sql/03_native_guards.sqlinc').write_text('\n'.join(blocks),encoding='utf-8',newline='\n')
(base/'NATIVE_GUARD_INPUTS.json').write_text(json.dumps({'canonicalI2':'7f5a5a876a9b485beee2b040409233eef70448c7','functions':receipts},indent=2)+'\n',encoding='utf-8')
print('Native guard bodies generated: '+str(len(blocks)))
