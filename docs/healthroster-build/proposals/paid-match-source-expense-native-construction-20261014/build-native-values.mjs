// Source construction only. One native Source expense calculation block,
// retained by the full current materialiser and the invoice-only companion.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {functionEvents} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),sha=b=>createHash('sha256').update(b).digest('hex');
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
const commit='973625f17cd51a49803872a819638e10f13bbbff';
const file='supabase/repeatable/13102026_1433_weekly_source_invoice_money_finalisation_v1.sql';
const raw=execFileSync('git',['show',commit+':'+file],{cwd:root,maxBuffer:3e6});
const signature='private.weekly_source_finalisation_materialise_expense_invoice_v1(uuid,uuid,text,uuid,uuid,uuid,uuid,text)';
const event=functionEvents(raw.toString()).events.find(e=>e.kind==='CREATE'&&e.id===signature);assert.ok(event);
const before=event.definition,eol=before.includes('\r\n')?'\r\n':'\n';
const nativeBlock=`    v_vat_rate:=case when v_current.source_expense_vat_enabled
      then v_expense_policy.invoice_vat_rate_pct else 0 end;
    v_vat:=pg_catalog.round((v_new_pence::numeric/100)*(v_vat_rate/100),2);
    v_total:=v_new_pence::numeric/100+v_vat;
    v_replacement_pay_vector:=pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_FIXED_EXPENSE_VECTOR_V1','kind','PAY',
      'source_expense_pence',v_new_pence,
      'source_expense_vat_enabled',v_current.source_expense_vat_enabled
    );
    v_replacement_charge_vector:=v_replacement_pay_vector||pg_catalog.jsonb_build_object('kind','CHARGE');`.replaceAll('\n',eol);
assert.equal(before.split(nativeBlock).length,2,'one actual native expense amount/vector block');
const hunks=[
 {name:'native values delegate state',before:'  v_count integer:=0;',after:'  v_values jsonb;\n  v_count integer:=0;'},
 {name:'single immutable-policy-bound native expense VAT/vector owner',before:nativeBlock,after:`    -- The existing scope/current/prior/policy equality guards above remain
    -- the write authority. This reads that exact immutable policy snapshot.
    v_values:=private.weekly_source_fixed_expense_invoice_values_v1(v_expense_policy.id);
    v_vat_rate:=(v_values->>'vat_rate_pct')::numeric(5,2);
    v_vat:=(v_values->>'vat_amount')::numeric(12,2);
    v_total:=(v_values->>'total_inc_vat')::numeric(12,2);
    v_replacement_pay_vector:=v_values->'pay_vector';
    v_replacement_charge_vector:=v_values->'charge_vector';`}
].map(h=>({...h,before:h.before.replaceAll('\n',eol),after:h.after.replaceAll('\n',eol)}));
let after=before;for(const h of hunks){assert.equal(after.split(h.before).length,2);after=after.replace(h.before,h.after);}
let reversed=after;for(const h of [...hunks].reverse()){assert.equal(reversed.split(h.after).length,2);reversed=reversed.replace(h.after,h.before);}assert.equal(reversed,before);
for(const d of ['before','forward','input'])fs.mkdirSync(path.join(dir,d),{recursive:true});
fs.writeFileSync(path.join(dir,'before/02_complete_native_expense_materialiser.sqlinc'),before);
fs.writeFileSync(path.join(dir,'input/exact_native_expense_values.sqlinc'),nativeBlock);
const acl=`\nALTER FUNCTION ${signature} OWNER TO postgres;\nREVOKE ALL ON FUNCTION ${signature} FROM public,anon,authenticated,service_role;\n`.replaceAll('\n',eol);
fs.writeFileSync(path.join(dir,'forward/02_complete_native_expense_materialiser.sqlinc'),after+acl);
const kernel=nativeBlock.replaceAll('v_current.source_expense_vat_enabled','v_expense_policy.source_expense_vat_enabled').replaceAll(eol,'\n');
const helper=`-- Exact native Source expense VAT/vector extraction. It provides invoice
-- values, never paid/amendment authority, a current expense generation or cash.
create or replace function private.weekly_source_fixed_expense_invoice_values_v1(p_policy_snapshot_id uuid)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_expense_policy public.weekly_source_row_expense_policy_snapshots%rowtype;
  v_new_pence bigint;v_vat_rate numeric(5,2);v_vat numeric(12,2);v_total numeric(12,2);
  v_replacement_pay_vector jsonb;v_replacement_charge_vector jsonb;
begin
  select * into strict v_expense_policy from public.weekly_source_row_expense_policy_snapshots
    where id=p_policy_snapshot_id;
  v_new_pence:=v_expense_policy.source_expense_pence;
  if v_new_pence is null or v_new_pence<0 or v_expense_policy.source_expense_vat_enabled is null
    or (v_expense_policy.source_expense_vat_enabled and v_expense_policy.invoice_vat_rate_pct is null) then
    raise exception 'WEEKLY_SOURCE_FIXED_EXPENSE_INVOICE_VALUES_UNBOUND' using errcode='55000';end if;
${kernel}
  return jsonb_build_object('schema_version','WEEKLY_SOURCE_FIXED_EXPENSE_INVOICE_VALUES_V1',
    'row_expense_policy_snapshot_id',v_expense_policy.id,'row_resolution_id',v_expense_policy.row_resolution_id,
    'work_event_id',v_expense_policy.work_event_id,'candidate_id',v_expense_policy.candidate_id,
    'client_id',v_expense_policy.client_id,'contract_id',v_expense_policy.contract_id,
    'net_pence',v_new_pence,'source_expense_vat_enabled',v_expense_policy.source_expense_vat_enabled,
    'vat_rate_pct',v_vat_rate,'vat_amount',v_vat,'total_inc_vat',v_total,
    'pay_vector',v_replacement_pay_vector,'charge_vector',v_replacement_charge_vector,
    'invoice_vat_policy_fingerprint',encode(v_expense_policy.invoice_vat_policy_fingerprint,'hex'));
end;
$function$;
alter function private.weekly_source_fixed_expense_invoice_values_v1(uuid) owner to postgres;
revoke all on function private.weekly_source_fixed_expense_invoice_values_v1(uuid) from public,anon,authenticated,service_role;
`;
fs.writeFileSync(path.join(dir,'forward/01_native_expense_invoice_values.sqlinc'),helper);
const censusPath='docs/healthroster-build/proposals/root-effective-body-amendment-census-case-forward-20261014/BODY_AMENDMENT_CENSUS.json';
const censusRaw=execFileSync('git',['show',commit+':'+censusPath],{cwd:root,maxBuffer:8e6}),census=JSON.parse(censusRaw);
fs.writeFileSync(path.join(dir,'NATIVE_VALUES_REVERSE.json'),JSON.stringify({source_only:true,commit,file,input_bytes:raw.length,input_sha256:sha(raw),signature,
 before_sha256:sha(before),after_definition_sha256:sha(after),after_leaf_sha256:sha(after+acl),hunks,whole_reverse_verified:true,
 extracted_native_block_bytes:Buffer.byteLength(nativeBlock),extracted_native_block_sha256:sha(nativeBlock),single_values_owner:true,
 original_native_scope_and_current_prior_reversal_guards_retained:true,source_policy_equality_precedes_delegate:true,
 exact_case_census:{commit,path:censusPath,sha256:sha(censusRaw),roots:census.rootCount,inputs:census.sourceInputCount,occurrences:census.sourceOccurrences,guarded_body_reads:census.guardedOrConditionalDefinitionReadsInDo},
 effective_native_catalogue_claim:false,canonical_mounted:false,SQL_compiled:false,native_proof:false,source_invoice_only_expense_producer_complete:false},null,2)+'\n');
console.log(JSON.stringify({new_private:'private.weekly_source_fixed_expense_invoice_values_v1(uuid)',whole_native_Source_factory_hunks:2,after_sha256:sha(after+acl),native_block_sha256:sha(nativeBlock),source_only:true}));
