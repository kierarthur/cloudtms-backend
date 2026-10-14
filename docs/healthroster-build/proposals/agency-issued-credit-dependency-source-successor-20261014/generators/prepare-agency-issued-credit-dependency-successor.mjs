import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),dir=path.join(here,'issued-credit-dependency-construction');
const input='docs/healthroster-build/proposals/agency-mode-a-source-support-issue-construction-20261014/construction/13_native_agency_issue_requirements.sqlinc';
const before=fs.readFileSync(path.join(repo,input),'utf8'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
assert.equal(fs.existsSync(path.join(dir,'REVIEW_MANIFEST.json')),false);const manifest=JSON.parse(fs.readFileSync(path.join(repo,path.dirname(path.dirname(input)),'REVIEW_MANIFEST.json')));assert.equal(sha(before),manifest.entries.find(x=>x.path==='construction/13_native_agency_issue_requirements.sqlinc').sha256);
fs.mkdirSync(dir,{recursive:true});const eol=before.includes('\r\n')?'\r\n':'\n',native=s=>s.replace(/\r?\n/g,eol),hunks=[];let forward=before;
function replace(a,b){a=native(a);b=native(b);assert.equal(forward.split(a).length,2);forward=forward.replace(a,()=>b);hunks.push({before:a,after:b});}
replace("where request.replacement_timesheet_id=target and request.state='APPLIED' order by request.id loop", "where request.replacement_timesheet_id=target and request.state='APPLIED'\n        and request.original_invoice_id is not null order by request.id loop");
const start=forward.indexOf(native('      -- The real R0 must belong')),end=forward.indexOf(native('    end loop;'),start);assert.ok(start>0&&end>start);
replace(forward.slice(start,end),`      -- Scoped CN keeps the real T0 line identity and records R0 in metadata.
      -- Existing native resume authority verifies EVERY clone/original/member
      -- and exact original amounts; no second sum or native identity rewrite.
      if not exists(select 1 from public.invoices cn
        where cn.type::text='CREDIT_NOTE' and cn.status::text in ('ISSUED','PAID') and cn.issued_at_utc is not null
          and cn.client_id=i.client_id and cn.original_invoice_id=r.original_invoice_id
          and cn.header_snapshot_json#>>'{credit_scope,reversal_timesheet_id}'=r.reversal_timesheet_id::text
          and cn.header_snapshot_json#>>'{credit_scope,reversed_timesheet_id}'=r.reversed_timesheet_id::text
          and exists(select 1 from public.invoice_lines line where line.invoice_id=cn.id
            and line.timesheet_id=r.reversed_timesheet_id and line.meta_json->>'reversal_timesheet_id'=r.reversal_timesheet_id::text)
          and exists(select 1 from public.timesheets reversal join public.timesheets_financials financial
            on financial.timesheet_id=reversal.timesheet_id and financial.is_current and not financial.is_stale
              and financial.timesheet_version=reversal.version and financial.authorised_at_utc=reversal.authorised_at_server
            where reversal.timesheet_id=r.reversal_timesheet_id and reversal.is_current and reversal.revoked_at is null
              and financial.locked_by_invoice_id=cn.id)
          and exists(select 1 from public.invoice_document_versions doc where doc.id=cn.issued_document_version_id
            and doc.entity_type='INVOICE' and doc.entity_id=cn.id and doc.purpose='FINAL_ISSUE'
            and doc.status='READY' and doc.verified_at_utc is not null and doc.sha256 is not null and doc.r2_key is not null)
          and exists(select 1 from private._invoice_credit_resume_v1(r.original_invoice_id,
            case when pg_input_is_valid(coalesce(cn.header_snapshot_json#>>'{credit_scope,creating_operation_id}',
              cn.header_snapshot_json->>'credit_operation_id',''),'uuid')
              then coalesce(cn.header_snapshot_json#>>'{credit_scope,creating_operation_id}',cn.header_snapshot_json->>'credit_operation_id')::uuid end,
            cn.id,r.reversal_timesheet_id) native_credit where native_credit.credit_note_id=cn.id)
      ) then credit_unissued:=true;end if;
`);
let restored=forward;for(const h of [...hunks].reverse()){assert.equal(restored.split(h.after).length,2);restored=restored.replace(h.after,()=>h.before);}assert.equal(restored,before);
const output='18_complete_native_agency_issue_requirements_with_actual_scoped_credit.sqlinc';fs.writeFileSync(path.join(dir,output),forward);
fs.writeFileSync(path.join(dir,'ISSUED_CREDIT_FULL_REVERSE.json'),JSON.stringify({source_only:true,native:false,input,input_sha256:sha(before),output,forward_sha256:sha(forward),hunks,complete_reverse:true,signature_unchanged:true,independent_review_pending:true,mount_ready:false},null,2)+'\n');
console.log(JSON.stringify({source_only:true,native:false,forward_sha256:sha(forward),hunks:hunks.length,complete_reverse:true}));
