import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {functionEvents} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),sha=b=>createHash('sha256').update(b).digest('hex'),commit='fa66183f01913e86651e82816f5e88f97e07123c';
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
const file='supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/09_complete_pay_consumers.sqlinc',raw=execFileSync('git',['show',commit+':'+file],{cwd:root,maxBuffer:2e6}),events=functionEvents(raw.toString()).events,records=[];
const names=[
 ['private.weekly_source_ordinary_projection_active_movements_v1(uuid,uuid)','15_complete_active_pay_movements.sqlinc'],
 ['private.weekly_source_ordinary_projection_source_units_v1(uuid,uuid)','16_complete_source_pay_units.sqlinc'],
 ['private.weekly_source_selected_source_witness_v1(uuid,uuid,uuid)','17_complete_selected_source_witness.sqlinc'],
 ['public.weekly_source_finalisation_pay_open_atomic_v1(jsonb)','18_complete_finalisation_pay_open.sqlinc']
];
for(const [id,output] of names){
 const e=events.find(e=>e.kind==='CREATE'&&e.id===id);assert.ok(e,id);const before=e.definition;
 const re=/\b(\w+)\.source_profile_kind<>'SELF_BILL_REPORT' and \1\.source_line_kind not in \('SELF_BILL_REPORT_LINE','INVOICE_VAT_ROUNDING','SOURCE_REBILL_LINE','SOURCE_INVOICE_CORRECTION'\)/g;
 const literals=[...new Set([...before.matchAll(re)].map(m=>m[0]))];assert.ok(literals.length,id);
 const hunks=literals.map(literal=>({name:'native effect exclusion for '+literal.match(/^\w+/)[0],before:literal,after:literal+' and '+literal.match(/^\w+/)[0]+".pay_effect='NORMAL'",occurrences:before.split(literal).length-1}));
 let after=before;for(const h of hunks)after=after.replaceAll(h.before,h.after);let reverse=after;for(const h of [...hunks].reverse())reverse=reverse.replaceAll(h.after,h.before);assert.equal(reverse,before);
 const acl=`\nALTER FUNCTION ${id} OWNER TO postgres;\nREVOKE ALL ON FUNCTION ${id} FROM public,anon,authenticated${id.startsWith('private.')?',service_role':''};\n${id.startsWith('public.')?`GRANT EXECUTE ON FUNCTION ${id} TO service_role;\n`:''}`;
 fs.writeFileSync(path.join(dir,'before',output),before);fs.writeFileSync(path.join(dir,'forward',output),after+acl);
 records.push({commit,file,input_sha256:sha(raw),signature:id,output,before_sha256:sha(before),after_definition_sha256:sha(after),after_leaf_sha256:sha(after+acl),hunks,whole_reverse_verified:true});
}
fs.writeFileSync(path.join(dir,'PAY_CONSUMER_REVERSES.json'),JSON.stringify({source_only:true,records,actual_line_kind_exclusions_retained:true,whole_pay_owner_census_complete:false,prepared_and_office_consumer_composition_complete:false,canonical_mounted:false,native_proof:false,pipeline_complete:false},null,2)+'\n');
console.log(JSON.stringify(records.map(r=>({output:r.output,literal_hunks:r.hunks.length,sites:r.hunks.reduce((n,h)=>n+h.occurrences,0)}))));
