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
 body=original.replace('\nBEGIN\n','''
  v_move_capability uuid;
BEGIN
  -- V6 exact one-use acceptance arm. All original ordinary refusal logic follows unchanged.
  IF TG_OP IN ('INSERT','UPDATE') THEN
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
  'exactChange':'One DECLARE variable and exact resolver/consume arm before original BEGIN body; all original body bytes retained.'})
(base/'forward/sql/03_native_guards.sqlinc').write_text('\n'.join(blocks),encoding='utf-8',newline='\n')
(base/'NATIVE_GUARD_INPUTS.json').write_text(json.dumps({'canonicalI2':'a934ee1d460b444939234e812babccb90dc3ddff','functions':receipts},indent=2)+'\n',encoding='utf-8')
print('Native guard bodies generated: '+str(len(blocks)))
