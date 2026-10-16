import fs from 'node:fs';
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';

const baseline='b192e0433cd24c4e7027ec1ca91470a715e57584';
const input='supabase/repeatable/14102026_0044_weekly_source_protected_pay_authority_v2.sql';
const output='supabase/repeatable/16102026_0319_weekly_contact_office_projection_v1.sql';
const args=process.argv.slice(2);assert(args.length<=1&&args.every(x=>x==='--write'));
const source=execFileSync('git',['show',`${baseline}:${input}`],{encoding:'utf8',maxBuffer:16*1024*1024});
const functions=[...source.matchAll(/CREATE OR REPLACE FUNCTION public\.weekly_source_combined_review_workspace_v1\(p_request jsonb\)[\s\S]*?\$function\$;/g)];
assert.equal(functions.length,1,'exact current native read owner');
const original=functions[0][0],anchor="  v_result:=jsonb_build_object('ok',true,'contract','WEEKLY_SOURCE_COMBINED_REVIEW_V1'";
assert.equal(original.split(anchor).length,2,'exact result construction anchor');
const body=original.replace(anchor,"  -- Pause controls enrich only the permitted current page. Payment-only\n  -- projections never expose contact controls; their current guards remain.\n  if not v_pay_view then\n    v_page:=private.weekly_contact_office_rows_v1(v_page,v_owners,(p_request->>'actor_user_id')::uuid);\n  end if;\n"+anchor);
const sha=x=>createHash('sha256').update(x).digest('hex');
const result=`-- Complete native combined read owner from ${baseline}.\n-- Original function SHA256 ${sha(original)}; one bounded read-projection call.\n\\set ON_ERROR_STOP on\nbegin;\n\\ir includes/weekly_contact_office_rows_v1.sqlinc\n${body}\nalter function public.weekly_source_combined_review_workspace_v1(jsonb) owner to postgres;\nrevoke all on function public.weekly_source_combined_review_workspace_v1(jsonb) from public,anon,authenticated,service_role;\ngrant execute on function public.weekly_source_combined_review_workspace_v1(jsonb) to service_role;\nnotify pgrst,'reload schema';\ncommit;\n`;
if(args.includes('--write')){
 const saved=fs.readFileSync(output,'utf8');
 assert(saved.includes('Implement the complete replacement definition')||saved===result,'preserve concurrent saved source');
 fs.writeFileSync(output,result);
}else assert.equal(fs.readFileSync(output,'utf8'),result,'projection composition drift');
console.log(JSON.stringify({input,baseline,original_function_sha256:sha(original),output,output_sha256:sha(result),database_execution:false}));
