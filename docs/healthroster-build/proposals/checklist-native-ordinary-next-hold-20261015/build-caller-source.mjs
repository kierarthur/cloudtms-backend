// Same financial truth before retention and paging; no new query classifier.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import crypto from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),commit='9b44412d7fdda7f8e099036e7c8a7297a8eeb795',records=[];
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
function full(file,output,hunks){const b=execFileSync('git',['cat-file','blob',commit+':'+file],{cwd:repo,maxBuffer:4e6}),eol=b.includes(Buffer.from('\r\n'))?'\r\n':'\n';let next=b.toString();const applied=[];
 for(const h of hunks){const before=h.before.replaceAll('\n',eol),after=h.after.replaceAll('\n',eol);assert.equal(next.split(before).length,2,h.label);next=next.replace(before,()=>after);applied.push({...h,before,after});}
 let reverse=next;for(const h of [...applied].reverse()){assert.equal(reverse.split(h.after).length,2,h.label);reverse=reverse.replace(h.after,()=>h.before);}assert.equal(reverse,b.toString());
 const saved='input/before_'+output;fs.writeFileSync(path.join(here,saved),b);fs.writeFileSync(path.join(here,'forward',output),next);
 records.push({source_commit:commit,source_path:file,saved,output:'forward/'+output,before_sha256:sha(b),after_sha256:sha(next),hunks:applied});}
const financialRead=`      v_money_scope:=jsonb_build_object('source_group_id',g.id,'source_cycle_id',v_read_scope.cycle_id,
        'client_id',v_client,'week_ending',v_read_scope.week_ending_date);
      v_scope_money:='[]'::jsonb;v_scope_money_facts:='[]'::jsonb;
      for v_money_row in select r from jsonb_array_elements(v_rows) r
        where r->>'item_kind'='ACTUAL_TIMESHEET' and r#>'{submission_witness,actual_submitted}'='true'::jsonb loop
        v_money:=private.weekly_source_checklist_ordinary_next_hold_money_v1((v_money_row->>'timesheet_id')::uuid,v_money_scope,v_actor);
        v_scope_money_facts:=v_scope_money_facts||jsonb_build_array(v_money->'native_facts');
        if v_money->'known'='true'::jsonb then
          v_scope_money:=v_scope_money||jsonb_build_array(v_money-'native_facts');
          -- Retain the week using the SAME native financial truth. No R254
          -- information row can enter this authentic counted-Timesheet arm.
          v_scope_open:=v_scope_open+1;
        end if;
      end loop;
      if v_read_scope.week_ending_date=v_week then
        v_financial_items:=v_scope_money;v_financial_facts:=v_scope_money_facts;
      end if;
`;
full('supabase/repeatable/healthroster_wp_u3_source_proposals/wp_u3_client_week_context.sqlinc','wp_u3_client_week_context.sqlinc',[
 {label:'read-local native financial frames',before:"  v_scope_count integer; v_scope_open integer; v_retention_reads jsonb:='[]'::jsonb;",after:"  v_scope_count integer; v_scope_open integer; v_retention_reads jsonb:='[]'::jsonb;\n  v_money jsonb;v_money_row jsonb;v_money_scope jsonb;v_scope_money jsonb;v_scope_money_facts jsonb;\n  v_financial_items jsonb:='[]'::jsonb;v_financial_facts jsonb:='[]'::jsonb;"},
 {label:'same finance owner before signed old-week admission',before:"      if v_scope_count>0 then v_latest_timesheet_week:=v_read_scope.week_ending_date; end if;",after:financialRead+"      if v_scope_count>0 then v_latest_timesheet_week:=v_read_scope.week_ending_date; end if;"},
 {label:'every signed retention frame binds native finance observations',before:"'source_cycle_id',v_read_scope.cycle_id,'read_revision',v_revision,'actual_timesheet_count',v_scope_count,'open_read_items',v_scope_open));",after:"'source_cycle_id',v_read_scope.cycle_id,'read_revision',v_revision,'actual_timesheet_count',v_scope_count,'open_read_items',v_scope_open,\n        'native_financial_facts',v_scope_money_facts));"},
 {label:'agency actual Timesheet arm shares one native finance read',before:"  if v_route='SIGNED_TIMESHEETS' and (v_count=0 or (v_week<>v_latest_week and v_open=0)) then",after:`  if v_route='AGENCY_INVOICE' then
    v_money_scope:=jsonb_build_object('source_group_id',g.id,'source_cycle_id',c.id,'client_id',v_client,'week_ending',v_week);
    for v_money_row in select value from jsonb_array_elements(v_actual) loop
      v_money:=private.weekly_source_checklist_ordinary_next_hold_money_v1((v_money_row->>'timesheet_id')::uuid,v_money_scope,v_actor);
      v_financial_facts:=v_financial_facts||jsonb_build_array(v_money->'native_facts');
      if v_money->'known'='true'::jsonb then v_financial_items:=v_financial_items||jsonb_build_array(v_money-'native_facts'); end if;
    end loop;
  end if;
  if v_route='SIGNED_TIMESHEETS' and (v_count=0 or (v_week<>v_latest_week and v_open=0)) then`},
 {label:'private leaf returns current-week fact subject, not money classifications or new raw query states',before:"    'qualified_waiting_count',v_wait,'engineering_gates',v_gate,",after:"    'qualified_waiting_count',v_wait,'engineering_gates',v_gate,\n    'native_financial_items',v_financial_items,'native_financial_facts',v_financial_facts,"},
 {label:'same actor-wide leaf revision includes all financial frames',before:"'wait_count',v_wait,'gates',v_gate)),'hex'));",after:"'wait_count',v_wait,'gates',v_gate,'native_financial_items',v_financial_items,'native_financial_facts',v_financial_facts)),'hex'));"}
]);
full('docs/healthroster-build/proposals/checklist-native-invoice-decision-transport-20261015/forward/wp_u3_native_checklist.sqlinc','wp_u3_native_checklist.sqlinc',[
 {label:'append the SAME already read native financial subjects before page/revision, no reclassification of invoice queries',before:"    fingerprints:=fingerprints||jsonb_build_array(jsonb_build_object('row_id',row_id,'native_financial_facts',financial_facts));",after:`    if route in('SIGNED_TIMESHEETS','AGENCY_INVOICE') then
      if jsonb_typeof(leaf->'native_financial_items') is distinct from 'array' or jsonb_typeof(leaf->'native_financial_facts') is distinct from 'array' then
        raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_FINANCIAL_FRAME_UNAVAILABLE' using errcode='55000'; end if;
      financial_facts:=financial_facts||leaf->'native_financial_facts';
      for raw in select value from jsonb_array_elements(leaf->'native_financial_items') loop
        if raw->'known' is distinct from 'true'::jsonb or raw#>>'{native_ordinary_money_witness,scope,source_group_id}' is distinct from row_scope->>'source_group_id'
          or raw#>>'{native_ordinary_money_witness,scope,source_cycle_id}' is distinct from row_scope->>'source_cycle_id'
          or raw#>>'{native_ordinary_money_witness,scope,client_id}' is distinct from row_scope->>'client_id'
          or raw#>>'{native_ordinary_money_witness,scope,week_ending}' is distinct from row_scope->>'week_ending' then
          raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_FINANCIAL_FRAME_UNAVAILABLE' using errcode='55000'; end if;
        -- A separate native TSFIN hold is not a HealthRoster invoice query.
        -- The genuine Timesheet was already counted by its submission owner.
        row_items:=row_items||jsonb_build_array(jsonb_build_object('item_id','native-financial:'||(raw#>>'{native_ordinary_money_witness,work_id}'),
          'item_kind','NATIVE_ORDINARY_FINANCIAL','scope',row_scope,'route',route,'worker',raw->'worker','work_date',raw->'work_date',
          'sheet_scope','WEEKLY','reason_code','PAY_ON_HOLD','status',raw->'status','hold',raw->'hold','subject','PAY','permitted_pay_subject',true,
          'action',case when view='ADMIN' then jsonb_build_object('kind','TIMESHEET','label','View pay hold in Timesheet ›','native_allowed',true,
            'target',jsonb_build_object('timesheet_id',raw#>>'{native_ordinary_money_witness,timesheet_id}')) end,
          'counted_timesheet_id',raw#>>'{native_ordinary_money_witness,timesheet_id}','correction_binding',null,'submission_read',null,
          'source_information_witness',null,'native_source_witness',null,'native_invoice_witness',null,'native_invoice_decision_witness',null,
          'queries_parent_witness',null,'native_ordinary_money_witness',raw->'native_ordinary_money_witness','_rank',35));
        open_count:=open_count+1;pay_count:=pay_count+1;completed:=false;pay_complete:=false;complete_text:=open_count::text||' items';
      end loop;
    end if;
    fingerprints:=fingerprints||jsonb_build_array(jsonb_build_object('row_id',row_id,'native_financial_facts',financial_facts));`}
]);
fs.writeFileSync(path.join(here,'COMPLETE_CALLER_REVERSES.json'),JSON.stringify({status:'UNMOUNTED_SAME_NATIVE_FINANCIAL_OWNER_RETENTION_AND_PUBLIC_ITEM_COMPOSITION',native:false,records},null,2)+'\n');
console.log(JSON.stringify({source_only:true,whole_owners:records.length,hunks:records.reduce((n,r)=>n+r.hunks.length,0),native:false}));
