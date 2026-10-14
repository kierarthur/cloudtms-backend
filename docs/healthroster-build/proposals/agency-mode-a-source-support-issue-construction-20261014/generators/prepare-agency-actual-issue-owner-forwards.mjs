import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
const dir=path.join(path.dirname(fileURLToPath(import.meta.url)),'issue-support-construction'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
assert.equal(fs.existsSync(path.join(dir,'REVIEW_MANIFEST.json')),false);
const outputs=[['before/02_public_invoice_issue_one.sql.txt','16_complete_actual_direct_issue_with_agency_callbacks.sqlinc'],['before/03_private__invoice_issue_advance_core_v8.sql.txt','17_complete_actual_async_issue_with_agency_callbacks.sqlinc']];
const reverse='ACTUAL_ISSUE_OWNERS_FULL_REVERSE.json';
if(fs.existsSync(path.join(dir,outputs[0][1]))){assert.ok(process.argv.includes('--refresh-unfrozen'));const p=JSON.parse(fs.readFileSync(path.join(dir,reverse)));for(const f of p.files)assert.equal(sha(fs.readFileSync(path.join(dir,f.output))),f.forward_sha256);}
const files=[];
for(const [input,output] of outputs){const before=fs.readFileSync(path.join(dir,input),'utf8'),eol=before.includes('\r\n')?'\r\n':'\n',native=s=>s.replace(/\r?\n/g,eol);let forward=before;const hunks=[];
 function replace(a,b){a=native(a);b=native(b);assert.equal(forward.split(a).length,2,a.slice(0,110));forward=forward.replace(a,()=>b);hunks.push({before:a,after:b});}
 if(input.includes('02_')){
  replace("  -- ======================================================\n  -- DEBUG (optional): single audit row per RPC call", "  v_agency_issue_codes text[]:=array[]::text[];\n\n  -- ======================================================\n  -- DEBUG (optional): single audit row per RPC call");
  // One existing batch validator produces the three Agency codes. The direct
  // owner's existing ordinary reasons and all native callbacks stay intact.
  const needle="  v_reasons := private.weekly_source_invoice_issue_reasons_v1(\n    v_weekly_source_issue, v_reasons\n  );";
  replace(needle,needle+`\n  select coalesce(array_agg(code order by code),array[]::text[]) into v_agency_issue_codes
  from private._invoice_issue_validate_batch(jsonb_build_array(jsonb_build_object(
    'request_key',p_invoice_id::text,'invoice_id',p_invoice_id,
    'expected_revision',(select i.document_revision from public.invoices i where i.id=p_invoice_id),
    'allow_early',true,'deliver',false)),v_anchor_ymd) native_issue
  cross join lateral jsonb_array_elements_text(native_issue.hard_blocker_codes) code
  where code in ('HEALTHROSTER_VALIDATION_REQUIRED','AGENCY_INVOICE_CREDIT_NOTE_NOT_ISSUED','HEALTHROSTER_EVIDENCE_MISSING');
  v_reasons:=array_cat(v_reasons,v_agency_issue_codes);`);
  replace("    p_actor_user_id\n  );\n\n  status := 'ISSUED';", "    p_actor_user_id\n  );\n\n  -- Only after this owner's actual new ISSUE update and real native audit.\n  perform private.weekly_source_agency_issue_close_v1(p_invoice_id);\n\n  status := 'ISSUED';");
 }else{
  replace('  v_part jsonb;', '  v_part jsonb;\n  v_agency_issued_ids uuid[];\n  v_agency_invoice_id uuid;');
  replace("      and c.payload_json->>'snapshot_hash'=v.snapshot_hash\n  ),\n  issued as (",`      and c.payload_json->>'snapshot_hash'=v.snapshot_hash
      -- Recheck the same three planned codes after actual invoice retention,
      -- immediately before the true transition; no second send/issue gate.
      and not exists(select 1 from private._invoice_issue_validate_batch(jsonb_build_array(jsonb_build_object(
        'request_key',i.id::text,'invoice_id',i.id,'expected_revision',i.document_revision,
        'allow_early',true,'deliver',false)),v_now::date) native_issue
        cross join lateral jsonb_array_elements_text(native_issue.hard_blocker_codes) code
        where code in ('HEALTHROSTER_VALIDATION_REQUIRED','AGENCY_INVOICE_CREDIT_NOTE_NOT_ISSUED','HEALTHROSTER_EVIDENCE_MISSING'))
  ),
  issued as (`);
  const marker="    'result',result_json,'error',error_json)),'[]')\n    into v_part from outcomes;\n  v_result:=v_result||coalesce(v_part,'[]');\n\n  -- QUEUE_DELIVERY durably delegates";
  replace(marker,`    'result',result_json,'error',error_json)),'[]'),
    (select coalesce(array_agg(id order by id),array[]::uuid[]) from issued)
    into v_part,v_agency_issued_ids from outcomes;
  v_result:=v_result||coalesce(v_part,'[]');
  -- Data-modifying CTEs share their statement snapshot. The next command sees
  -- the genuine issued RETURNING rows and native audits; only those new IDs
  -- cause retirement. Replayed/already-issued or ineligible chunks do not.
  foreach v_agency_invoice_id in array v_agency_issued_ids loop
    perform private.weekly_source_agency_issue_close_v1(v_agency_invoice_id);
  end loop;

  -- QUEUE_DELIVERY durably delegates`);
 }
 let restored=forward;for(const h of [...hunks].reverse()){assert.equal(restored.split(h.after).length,2);restored=restored.replace(h.after,()=>h.before);}assert.equal(restored,before);
 fs.writeFileSync(path.join(dir,output),forward);files.push({input,input_sha256:sha(before),output,forward_sha256:sha(forward),hunks,complete_reverse:true});
}
fs.writeFileSync(path.join(dir,reverse),JSON.stringify({source_only:true,native:false,files,independent_review_pending:true,mount_ready:false},null,2)+'\n');console.log(JSON.stringify({source_only:true,native:false,files:files.map(x=>({output:x.output,sha256:x.forward_sha256,hunks:x.hunks.length}))}));
