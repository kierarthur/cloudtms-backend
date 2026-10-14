import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {scanManagedSql} from '../../../../scripts/cloudtms-managed-release-sql.mjs';
const out=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(out,'../../../..');
const sourceCommit='2151af656561ad4d68e61e242769ed80a631fcc9',file='supabase/repeatable/13102026_1134_weekly_source_rule_sets_v1.sql';
const sha=value=>createHash('sha256').update(value).digest('hex'),proofs=[];
const original=execFileSync('git',['show',sourceCommit+':'+file],{cwd:own,maxBuffer:4_000_000}).toString('utf8');
function emit(name,before,hunks){let value=before;
 for(const h of hunks){assert.equal(value.split(h.before).length-1,1,h.before);value=value.replace(h.before,()=>h.after);}
 let reversed=value;for(const h of [...hunks].reverse()){assert.equal(reversed.split(h.after).length-1,1);reversed=reversed.replace(h.after,()=>h.before);}
 assert.equal(reversed,before);fs.writeFileSync(path.join(out,name),value.trimEnd()+'\n');
 proofs.push({file:name,input_sha256:sha(before),output_sha256:sha(value.trimEnd()+'\n'),reverse_hunk_matches_entire_input_definition:true,
  hunks:hunks.map(h=>({before_sha256:sha(h.before),after_sha256:sha(h.after)}))});}
const definitions=scanManagedSql(original).statements.map(row=>original.slice(row.start,row.end))
 .filter(text=>/^\s*create or replace function public\.weekly_source_group_rule_assign_atomic_v1\(p_request jsonb\)/i.test(text));
assert.equal(definitions.length,1);
emit('11_complete_historical_fixture_native_assignment.sqlinc',definitions[0],[{
 before:"  if v_from<(transaction_timestamp() at time zone 'Europe/London')::date then\n    raise exception 'WEEKLY_SOURCE_RULE_FUTURE_WEEK_REQUIRED' using errcode='22023'; end if;",
 after:"  if v_from<(transaction_timestamp() at time zone 'Europe/London')::date then\n    -- Only the owner-XID audit of a fresh disabled historical TEST fixture\n    -- substitutes this date assertion. All native guards below remain.\n    perform private.weekly_source_historical_fixture_assignment_assert_v1(p_request);\n  end if;"}]);
const priorCommit='3a4f1a7c7e8e08fb171068a2d0e3bc2259d1bc32';
const input=name=>execFileSync('git',['show',priorCommit+':'+path.relative(own,path.join(out,name)).replaceAll('\\','/')],
 {cwd:own,maxBuffer:4_000_000}).toString('utf8');
let source=input('OFF_RULE_AUTHORITY.sqlinc');
emit('OFF_RULE_AUTHORITY.sqlinc',source,[{
 before:"  if p_initial_open then\n    if auth->>'purpose'='SOURCE_FIXTURE_SETUP' and (a.effective_from_week_ending<today or c.finalisation_week_ending<>a.effective_from_week_ending",
 after:"  -- HISTORICAL_FIXTURE_OPEN_BOUNDARY: every use rechecks the real native\n  -- assignment audit; only the first source OPEN additionally requires zero\n  -- prior uploads. No historical operational enablement is inferred.\n  if a.effective_from_week_ending<today then\n    perform private.weekly_source_historical_fixture_setup_assert_v1(auth,p_initial_open and auth->>'purpose'='SOURCE_FIXTURE_SETUP');\n  end if;\n  if p_initial_open then\n    if auth->>'purpose'='SOURCE_FIXTURE_SETUP' and (c.finalisation_week_ending<>a.effective_from_week_ending"}]);
source=input('POST_VERIFIED_SETUP_ASSERT.sqlinc');
emit('POST_VERIFIED_SETUP_ASSERT.sqlinc',source,[{
 before:"  if p_artifact->>'purpose'='AUTOMATIC_JOURNEY' then",
 after:"  -- HISTORICAL_FIXTURE_RECORD_BOUNDARY: every past scope uses the exact\n  -- genuine native disabled assignment; Source setup also proves vacancy.\n  if a.effective_from_week_ending<(clock_timestamp() at time zone 'Europe/London')::date then\n    perform private.weekly_source_historical_fixture_setup_assert_v1(p_artifact,p_artifact->>'purpose'='SOURCE_FIXTURE_SETUP');\n  end if;\n  if p_artifact->>'purpose'='AUTOMATIC_JOURNEY' then"}, {
 before:"  elsif a.effective_from_week_ending<(clock_timestamp() at time zone 'Europe/London')::date\n    or c.finalisation_week_ending<>a.effective_from_week_ending or c.current_final_revision_id is not null",
 after:"  else\n    if c.finalisation_week_ending<>a.effective_from_week_ending or c.current_final_revision_id is not null"},
 {before:"    raise exception 'WEEKLY_SOURCE_POST_VERIFIED_SETUP_FUTURE_UNFINALISED_REQUIRED' using errcode='55000'; end if;\nend;",
  after:"    raise exception 'WEEKLY_SOURCE_POST_VERIFIED_SETUP_FUTURE_UNFINALISED_REQUIRED' using errcode='55000'; end if;\n  end if;\nend;"}]);
const proofFile=path.join(out,'HISTORICAL_FIXTURE_REVERSE.json');
fs.writeFileSync(proofFile,JSON.stringify({source_only:true,consumed_source_commit:sourceCommit,prior_money_commit:priorCommit,consumed_file:file,consumed_file_sha256:sha(original),
 native_future_feature_owner_unchanged:true,all_native_assignment_guards_except_qualified_date_assertion_preserved:true,
 historical_fixture_operational_ready:false,no_clock_or_upload_timestamp_override:true,no_database_or_runtime_acceptance:true,proofs},null,2)+'\n');
console.log(JSON.stringify({source_only:true,consumed_source_commit:sourceCommit,historical_assignment_complete_owner:true,new_or_verified_reverse_proofs:proofs.length}));
