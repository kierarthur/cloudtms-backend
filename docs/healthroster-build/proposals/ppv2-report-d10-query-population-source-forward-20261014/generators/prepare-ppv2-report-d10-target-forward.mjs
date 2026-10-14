import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import crypto from 'node:crypto';import {spawnSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),dir=path.join(here,'ppv2-final-workspace-construction'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
assert.equal(fs.existsSync(path.join(dir,'REVIEW_MANIFEST.json')),false);
if(fs.existsSync(path.join(dir,'workspace.report-d10-target-forward.sqlinc'))){assert.ok(process.argv.includes('--refresh-unfrozen'));const previous=JSON.parse(fs.readFileSync(path.join(dir,'PPV2_REPORT_D10_TARGET_FULL_REVERSE.json')));assert.equal(sha(fs.readFileSync(path.join(dir,'workspace.report-d10-target-forward.sqlinc'))),previous.forward_definition_sha256);}
const before=fs.readFileSync(path.join(dir,'workspace.current-before.sqlinc'),'utf8'),overlay=fs.readFileSync(path.join(dir,'workspace.root-report-overlay.sqlinc'),'utf8');
const oldLines=before.match(/[^\n]*\n|[^\n]+$/g)||[],newLines=overlay.match(/[^\n]*\n|[^\n]+$/g)||[];
const diff=spawnSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','diff','--no-index','--unified=0','--',path.join(dir,'workspace.current-before.sqlinc'),path.join(dir,'workspace.root-report-overlay.sqlinc')],{encoding:'utf8'});assert.equal(diff.status,1);
const overlayHunks=[...diff.stdout.matchAll(/^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@/gm)].map(m=>{const oc=m[2]===undefined?1:Number(m[2]),nc=m[4]===undefined?1:Number(m[4]),oi=Number(m[1])-(oc?1:0),ni=Number(m[3])-(nc?1:0);return{before:oldLines.slice(oi,oi+oc).join(''),after:newLines.slice(ni,ni+nc).join(''),before_line:oi+1,after_line:ni+1,after_byte_offset:Buffer.byteLength(newLines.slice(0,ni).join(''))};});
let baseRestore=Buffer.from(overlay);for(const h of [...overlayHunks].reverse()){const b=Buffer.from(h.after),at=h.after_byte_offset;assert.ok(baseRestore.subarray(at,at+b.length).equals(b));baseRestore=Buffer.concat([baseRestore.subarray(0,at),Buffer.from(h.before),baseRestore.subarray(at+b.length)]);}assert.ok(baseRestore.equals(Buffer.from(before)));
const eol=overlay.includes('\r\n')?'\r\n':'\n',native=s=>s.replace(/\r?\n/g,eol),hunks=[];let current=overlay;
function replace(a,b){a=native(a);b=native(b);assert.equal(current.split(a).length,2);hunks.push({before:a,after:b});current=current.replace(a,()=>b);}
replace('  v_query_row_key text;','  v_query_row_key text;\n  v_d10_target boolean:=false;\n  v_d10_groups jsonb;\n  v_d10_rows jsonb;\n  v_d10_count integer;');
replace("  v_query_row_key:=p_request->>'row_key';", "  v_query_row_key:=p_request->>'row_key';\n  v_d10_target:=coalesce(v_query_row_key~'^d10g_[0-9a-f]{64}$',false);" );
replace("    or v_query_row_key='' or length(v_query_row_key)>512 or v_query_row_key~'[[:cntrl:]]'", "    or v_query_row_key!~'^(qg|d10g)_[0-9a-f]{64}$'" );
replace("  if v_publication_id is not null then\n    select pg_catalog.count(*)::integer,", "  if v_publication_id is not null and not v_d10_target then\n    select pg_catalog.count(*)::integer," );
replace("  if v_tab='queries' and v_publication_id is not null then\n    v_total:=v_query_count;", "  if v_tab='queries' and v_publication_id is not null and not v_d10_target then\n    v_total:=v_query_count;" );
replace("  if v_query_row_key is not null then\n    -- The exact scoped target comes from the unchanged native query universe", `  if v_d10_target then
    -- The one shared native D10 census supplies the literal parent key. It is
    -- independent of Source publication and never enters Source bulk selection.
    if v_group.source_family<>'ROSTER' or v_pay_view or v_filters<>'{}'::jsonb then
      raise exception 'WEEKLY_SOURCE_WORKSPACE_REQUEST_INVALID' using errcode='22023';end if;
    v_d10_groups:=private.weekly_source_check_only_query_groups_v1(v_cycle.id,v_client_id,
      jsonb_build_object('actor_user_id',v_actor,'source_group_id',v_group.id,'week_ending',v_cycle.finalisation_week_ending));
    if v_d10_groups->>'contract' is distinct from 'WEEKLY_SOURCE_CHECK_ONLY_QUERY_GROUPS_V1'
      or v_d10_groups->>'actor_user_id' is distinct from v_actor::text
      or v_d10_groups->>'permission_view' is distinct from 'ADMIN'
      or v_d10_groups#>>'{scope,source_group_id}' is distinct from v_group.id::text
      or v_d10_groups#>>'{scope,source_cycle_id}' is distinct from v_cycle.id::text
      or v_d10_groups#>>'{scope,client_id}' is distinct from v_client_id::text
      or v_d10_groups#>>'{scope,week_ending}' is distinct from v_cycle.finalisation_week_ending::text
      or jsonb_typeof(v_d10_groups->'groups') is distinct from 'array'
      or v_d10_groups->'source_outreach_eligible' is distinct from 'false'::jsonb
      or v_d10_groups->'bulk_actions' is distinct from '{}'::jsonb
      or coalesce(v_d10_groups->>'read_revision','')!~'^[0-9a-f]{64}$' then
      raise exception 'WEEKLY_SOURCE_CHECK_ONLY_NATIVE_GROUP_READ_UNAVAILABLE' using errcode='55000';end if;
    select count(*)::integer,coalesce(jsonb_agg(group_row order by group_row->>'group_key'),'[]'::jsonb)
      into v_d10_count,v_d10_rows from jsonb_array_elements(v_d10_groups->'groups') group_row
      where group_row->>'group_key'=v_query_row_key and group_row->>'client_id'=v_client_id::text;
    if v_d10_count<>1 then
      raise exception 'WEEKLY_SOURCE_CHECKLIST_QUERY_TARGET_STALE' using errcode='40001';end if;
    v_query_count:=v_d10_count;
    v_queries:=jsonb_build_object('rows',v_d10_rows,'total_count',v_d10_count,'next_cursor','','has_more',false,
      'record_version',v_d10_groups->>'read_revision','stale',false,'origin','CHECK_ONLY_D10');
    v_bulk_actions:='{}'::jsonb;
  elsif v_query_row_key is not null then
    -- The exact scoped target comes from the unchanged native query universe`);
let restored=current;for(const h of [...hunks].reverse()){assert.equal(restored.split(h.after).length,2);restored=restored.replace(h.after,()=>h.before);}assert.equal(restored,overlay);
fs.writeFileSync(path.join(dir,'workspace.report-d10-target-forward.sqlinc'),current);
fs.writeFileSync(path.join(dir,'PPV2_REPORT_D10_TARGET_FULL_REVERSE.json'),JSON.stringify({source_only:true,native:false,current_definition_sha256:sha(before),root_report_overlay_definition_sha256:sha(overlay),forward_definition_sha256:sha(current),current_to_report_overlay_hunks:overlayHunks,report_overlay_to_d10_hunks:hunks,complete_reverse_to_current_ppv2:true,shared_d10_adapter_sha256:'7c6b3a3aad2d6a7b734996dc2ffc94efaf156039b459ac8b1e5da5886e329722',normal_d10_population_union_complete:false,independent_review_pending:true,mount_ready:false},null,2)+'\n');
console.log(JSON.stringify({source_only:true,native:false,current_sha256:sha(before),forward_sha256:sha(current),root_hunks:overlayHunks.length,d10_hunks:hunks.length,complete_reverse:true,normal_d10_population_union_complete:false,mount_ready:false},null,2));
