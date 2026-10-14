import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),sha=b=>createHash('sha256').update(b).digest('hex');
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')),'Preserve frozen history');
const beforeCommit='b9505669927d86f843b376376033db0b87f169f2',current='33927fff2f7b90b90241d746d9e35aa21c57bd07';
const base='docs/healthroster-build/proposals/kept-target-publication-context-successor-20261014/';
const kernelBefore=`      (publication.state='CURRENT' and upload.state='CURRENT' and upload.purpose='ORDINARY'
        and upload.correction_session_id is null and publication.correction_session_id is null
        and cycle.current_projection_publication_id=publication.id)`;
// This is the real completed correction, not an ordinary-purpose label or a
// preparer's asserted state. Future readers keep their own dated authority.
const appliedProof=`upload.purpose='FINAL_SOURCE_CORRECTION' and exists(
          select 1 from public.weekly_final_source_correction_sessions applied
          join public.weekly_source_final_revisions final on final.id=applied.applied_final_revision_id
          where applied.id=upload.correction_session_id and publication.correction_session_id=applied.id
            and applied.source_cycle_id=cycle.id and applied.authority_scope_kind='CYCLE'
            and applied.report_scope_id is null and applied.state='APPLIED'
            and applied.prepared_final_revision_id=applied.applied_final_revision_id
            and applied.replacement_correction_upload_id=upload.id
            and applied.replacement_projection_publication_id=publication.id
            and applied.completed_at_utc is not null
            and final.source_cycle_id=cycle.id and final.authority_scope_kind='CYCLE'
            and final.report_scope_id is null and final.upload_id=upload.id
            and final.reason='CORRECT_FINAL_SOURCE' and final.state='CURRENT'
            and final.predecessor_revision_id=applied.expected_current_final_revision_id
            and cycle.current_final_revision_id=final.id
            and applied.result_json->>'status'='CORRECTED'
            and applied.result_json->>'correction_session_id'=applied.id::text
            and applied.result_hash=private.weekly_source_sha256_jsonb_v1(
              'WEEKLY_SOURCE_CORRECT_FINAL_RESULT_V1',applied.result_json)
            and applied.review_result_hash=private.weekly_source_sha256_jsonb_v1(
              'WEEKLY_SOURCE_CORRECT_FINAL_REVIEW_RESULT_V1',applied.review_result_json))`;
const kernelAfter=`      (publication.state='CURRENT' and upload.state='CURRENT'
        and cycle.current_projection_publication_id=publication.id
        and ((upload.purpose='ORDINARY' and upload.correction_session_id is null
          and publication.correction_session_id is null)
          or (${appliedProof})))`;
const wrapperBefore="    and publication.state='CURRENT' and upload.state='CURRENT' and upload.purpose='ORDINARY'";
const wrapperAfter=`    and publication.state='CURRENT' and upload.state='CURRENT'
    and publication.source_cycle_id=cycle.id and publication.report_scope_id is null
    and cycle.current_projection_publication_id=publication.id
    and ((upload.purpose='ORDINARY' and upload.correction_session_id is null
      and publication.correction_session_id is null)
      or (${appliedProof}))`;
const files=[];
for(const leaf of ['00_target_publication_facts.sqlinc','01_target_publication_context.sqlinc']){
 const inputFile=base+'forward/'+leaf,input=execFileSync('git',['--no-replace-objects','show',beforeCommit+':'+inputFile],{cwd:root,maxBuffer:2000000}),old=input.toString('utf8'),nl=old.includes('\r\n')?'\r\n':'\n',normalize=s=>s.replaceAll('\r\n','\n').replaceAll('\n',nl),hunks=[];let after=old;
 for(const [name,before,newText] of [['completed-current target branch',kernelBefore,kernelAfter],['latest-current wrapper includes only proven completed correction',wrapperBefore,wrapperAfter]]){
  const b=normalize(before),a=normalize(newText);assert.equal(after.split(b).length,2,leaf+' '+name);after=after.replace(b,a);assert.equal(after.split(a).length,2,name+' reverse');hunks.push({name,before:b,after:a});
 }
 let reverse=after;for(const h of [...hunks].reverse()){assert.equal(reverse.split(h.after).length,2,h.name);reverse=reverse.replace(h.after,h.before);}assert.equal(reverse,old);
 fs.mkdirSync(path.join(dir,'before'),{recursive:true});fs.mkdirSync(path.join(dir,'forward'),{recursive:true});fs.writeFileSync(path.join(dir,'before',leaf),input);fs.writeFileSync(path.join(dir,'forward',leaf),after);
 files.push({input_commit:beforeCommit,input_file:inputFile,before:'before/'+leaf,after:'forward/'+leaf,input_bytes:input.length,input_sha256:sha(input),output_bytes:Buffer.byteLength(after),output_sha256:sha(after),full_reverse_verified:true,unique_hunks:hunks.length,hunks});
}
const nativeFiles=['supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql','supabase/repeatable/15092026_1534_weekly_source_correct_final_source_v1.sql'];const pins=nativeFiles.map(file=>{const b=execFileSync('git',['--no-replace-objects','show',current+':'+file],{cwd:root,maxBuffer:3000000});return {commit:current,file,bytes:b.length,sha256:sha(b)};});
const record={source_only:true,current_root:current,original_frozen_target14_commit:beforeCommit,original_frozen_target14_manifest:'91121aaaf3ccc7b4f8b8368f6eab0aac3f1b9fc29155d4898fcc57f37bacbfc0',source_findings:['Original ordinary-only read misses genuine CURRENT FINAL_SOURCE_CORRECTION after APPLIED'],files,actual_native_inputs:pins,complete_reverses:2,unique_hunks:4,new_private_public_or_table_signatures:0,financial_and_facts_tuple_and_prepared_branch_unchanged:true,current_viewer_not_original_actor:true,writer_correction_preconditions_unchanged:true,canonical_changed:false,sql_compiled:false,native_proof:false,default_and_accept_consumers_complete:false};
fs.writeFileSync(path.join(dir,'COMPLETE_APPLIED_CONTEXT_AND_REVERSES.json'),JSON.stringify(record,null,2)+'\n');console.log(JSON.stringify({source_only:true,complete_reverses:2,unique_hunks:4,signatures_changed:0,canonical_changed:false,native_proof:false}));
