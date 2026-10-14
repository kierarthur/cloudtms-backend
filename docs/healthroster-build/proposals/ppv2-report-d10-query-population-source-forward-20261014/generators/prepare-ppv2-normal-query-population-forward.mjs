import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),dir=path.join(here,'ppv2-final-workspace-construction'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
assert.equal(fs.existsSync(path.join(dir,'REVIEW_MANIFEST.json')),false,'Preserve frozen inputs');
const input='workspace.report-d10-target-forward.sqlinc',output='workspace.report-d10-population-forward.sqlinc',reverse='PPV2_D10_POPULATION_FULL_REVERSE.json';
if(fs.existsSync(path.join(dir,output))){assert.ok(process.argv.includes('--refresh-unfrozen'));assert.equal(sha(fs.readFileSync(path.join(dir,output))),JSON.parse(fs.readFileSync(path.join(dir,reverse))).forward_sha256);}
const before=fs.readFileSync(path.join(dir,input),'utf8'),eol=before.includes('\r\n')?'\r\n':'\n',native=s=>s.replace(/\r?\n/g,eol);let current=before;const hunks=[];
function replace(a,b){a=native(a);b=native(b);assert.equal(current.split(a).length,2,a.slice(0,120));current=current.replace(a,()=>b);hunks.push({before:a,after:b});}
replace('  v_d10_rows jsonb;\n  v_d10_count integer;',"  v_d10_rows jsonb:='[]'::jsonb;\n  v_d10_keys jsonb:='[]'::jsonb;\n  v_d10_revisions jsonb:='[]'::jsonb;\n  v_d10_scope record;\n  v_d10_row jsonb;\n  v_query_population jsonb;");
const cursor="  v_cursor:=coalesce(nullif(p_request->>'cursor',''),'');";
replace(cursor,fs.readFileSync(path.join(dir,'d10-census.fragment.sql.txt'),'utf8')+cursor);
replace("       or split_part(v_cursor,':',2)!~ '^[0-9]+$' then", "       or v_cursor!~'^[0-9a-f]{64}:[0-9]{1,10}$'\n       or split_part(v_cursor,':',2)::numeric>2147483547 then");
replace("        'candidate',query.candidate_name,'client',query.client_name,'issues',query.issue_count,", "        '_query_first_seen_at_utc',query.first_seen_at_utc,\n        'candidate',query.candidate_name,'client',query.client_name,'issues',query.issue_count,");
// Only the Queries page is collected completely for the one mixed population.
// Source render/actions and the earlier Source-only bulk census are unchanged.
const start=current.indexOf(native("  if v_tab='queries' and v_publication_id is not null and not v_d10_target then"));assert.ok(start>=0);
const end=current.indexOf(native("  if v_d10_target then\n"),start);assert.ok(end>start);
const page=current.slice(start,end);assert.equal(page.split(native('      offset v_offset limit v_limit\n')).length,2);
replace(page,page.replace(native('      offset v_offset limit v_limit\n'),''));
const targetStart=current.indexOf(native('  if v_d10_target then\n')),targetEnd=current.indexOf(native("  v_queries:=v_queries||pg_catalog.jsonb_build_object('bulk_actions',v_bulk_actions);"),targetStart);assert.ok(targetStart>=0&&targetEnd>targetStart);
const old=current.slice(targetStart,targetEnd+native("  v_queries:=v_queries||pg_catalog.jsonb_build_object('bulk_actions',v_bulk_actions);\n").length);
replace(old,fs.readFileSync(path.join(dir,'d10-population-page.fragment.sql.txt'),'utf8'));
let restored=current;for(const h of [...hunks].reverse()){assert.equal(restored.split(h.after).length,2);restored=restored.replace(h.after,()=>h.before);}assert.equal(restored,before);
fs.writeFileSync(path.join(dir,output),current);fs.writeFileSync(path.join(dir,reverse),JSON.stringify({schema:'PPV2_D10_NORMAL_POPULATION_FULL_REVERSE_V1',source_only:true,native:false,input,output,input_sha256:sha(before),forward_sha256:sha(current),hunks,full_reverse:true,
 shared_adapter_current_commit:'5804a7d0be822e2f133d86d934fca8a2897ff33f',shared_adapter_current_sha256:'5fe27bf0723ed51f28952177aa10987760619b3b6b0c08d97a2a58098011f7cc',
 issue_filter_authority_successor_required:false,normal_population_composed:true,independent_review_pending:true,mount_ready:false},null,2)+'\n');
console.log(JSON.stringify({source_only:true,native:false,input_sha256:sha(before),forward_sha256:sha(current),hunks:hunks.length,full_reverse:true,issue_filter_authority_successor_required:false,mount_ready:false},null,2));
