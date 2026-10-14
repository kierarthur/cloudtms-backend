import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),dir=path.join(here,'ppv2-final-workspace-construction');
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
assert.equal(fs.existsSync(path.join(dir,'REVIEW_MANIFEST.json')),false,'Never edit a frozen packet');
const input='scopes.root-report-overlay.sqlinc',output='scopes.report-d10-population-forward.sqlinc',reverse='PPV2_D10_SCOPES_FULL_REVERSE.json';
if(fs.existsSync(path.join(dir,output))){assert.ok(process.argv.includes('--refresh-unfrozen'));assert.equal(sha(fs.readFileSync(path.join(dir,output))),JSON.parse(fs.readFileSync(path.join(dir,reverse))).forward_sha256);}
const before=fs.readFileSync(path.join(dir,input),'utf8'),eol=before.includes('\r\n')?'\r\n':'\n',native=s=>s.replace(/\r?\n/g,eol);let current=before;const hunks=[];
function replace(a,b){a=native(a);b=native(b);assert.equal(current.split(a).length,2,a.slice(0,150));current=current.replace(a,()=>b);hunks.push({before:a,after:b});}
replace('  v_cutoffs jsonb;',`  v_cutoffs jsonb;
  v_admin boolean;
  v_d10_scope record;
  v_d10_frame jsonb;
  v_d10_scopes jsonb:='[]'::jsonb;`);
replace("      if v_offset is null or v_offset<0 then raise exception 'invalid offset'; end if;", "      if v_offset is null or v_offset<0 or v_offset>2147483547 then raise exception 'invalid offset'; end if;");
replace('  with candidates as (',fs.readFileSync(path.join(dir,'d10-scope-census.fragment.sql.txt'),'utf8')+'  with candidates as (');
replace('      coalesce(report.cutoff_at_utc,candidate.cutoff_at_utc) actual_cutoff',"      coalesce(report.cutoff_at_utc,candidate.cutoff_at_utc) actual_cutoff,\n      'SOURCE'::text scope_kind,null::text query_read_revision");
replace("      fr.id,case when fr.id is not null then 'FINAL_SOURCE' end,fr.finalised_at_utc,encode(fr.manifest_hash,'hex'),rs.cutoff_at_utc", "      fr.id,case when fr.id is not null then 'FINAL_SOURCE' end,fr.finalised_at_utc,encode(fr.manifest_hash,'hex'),rs.cutoff_at_utc,\n      'SELF_BILL_REPORT'::text,null::text");
replace('  ), facts as (\n    select scopes.*,private.weekly_source_import_is_prepared_v1(scopes.upload_id) prepared',`    union all
    select q.group_id,q.source_name,'ROSTER'::text,q.client_id,q.client_name,q.cycle_id,q.scope_client_id,
      q.finalisation_week_ending,q.cutoff_at_utc,q.cycle_version,null::uuid,null::uuid,
      null::uuid,null::bigint,null::uuid,null::uuid,null::uuid,null::text,null::timestamptz,null::text,
      q.cutoff_at_utc,'CHECK_ONLY_D10'::text,q.query_read_revision
    from jsonb_to_recordset(v_d10_scopes) q(group_id uuid,source_name text,client_id uuid,client_name text,
      cycle_id uuid,scope_client_id uuid,finalisation_week_ending date,cutoff_at_utc timestamptz,
      cycle_version bigint,query_read_revision text)
  ), facts as (
    select scopes.*,case when scopes.scope_kind='CHECK_ONLY_D10' then null::boolean
      else private.weekly_source_import_is_prepared_v1(scopes.upload_id) end prepared`);
replace("count(distinct (cycle_id,client_id)) filter(where completion_id is null and not client_has_prepared", "count(distinct (cycle_id,client_id)) filter(where scope_kind<>'CHECK_ONLY_D10' and completion_id is null and not client_has_prepared");
replace("count(distinct (cycle_id,client_id)) filter(where completion_id is null and prepared)", "count(distinct (cycle_id,client_id)) filter(where scope_kind<>'CHECK_ONLY_D10' and completion_id is null and prepared)");
replace('cycle_id,client_id,cycle_version,report_scope_id,report_version,upload_id,publication_id,completion_hash,prepared)', 'cycle_id,client_id,cycle_version,report_scope_id,report_version,upload_id,publication_id,completion_hash,prepared,scope_kind,query_read_revision)');
replace("      'prepared',prepared,'completed',completion_id is not null,'completion_kind',completion_kind,", "      'scope_kind',scope_kind,'query_read_revision',query_read_revision,\n      'prepared',prepared,'completed',case when scope_kind='CHECK_ONLY_D10' then null::boolean else completion_id is not null end,'completion_kind',completion_kind,");
replace("'missing_previous_report',completion_id is null and not client_has_prepared", "'missing_previous_report',scope_kind<>'CHECK_ONLY_D10' and completion_id is null and not client_has_prepared");
let restored=current;for(const h of [...hunks].reverse()){assert.equal(restored.split(h.after).length,2);restored=restored.replace(h.after,()=>h.before);}assert.equal(restored,before);
fs.writeFileSync(path.join(dir,output),current);fs.writeFileSync(path.join(dir,reverse),JSON.stringify({schema:'PPV2_D10_SCOPES_FULL_REVERSE_V1',source_only:true,native:false,input,output,input_sha256:sha(before),forward_sha256:sha(current),hunks,full_reverse:true,shared_adapter_current_commit:'5804a7d0be822e2f133d86d934fca8a2897ff33f',shared_adapter_current_sha256:'5fe27bf0723ed51f28952177aa10987760619b3b6b0c08d97a2a58098011f7cc',independent_review_pending:true,mount_ready:false},null,2)+'\n');
console.log(JSON.stringify({source_only:true,native:false,input_sha256:sha(before),forward_sha256:sha(current),hunks:hunks.length,full_reverse:true,mount_ready:false}));
