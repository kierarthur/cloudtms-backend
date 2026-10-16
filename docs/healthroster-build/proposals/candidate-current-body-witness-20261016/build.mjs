import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),read=p=>fs.readFileSync(path.resolve(here,p),'utf8').replace(/\r\n/g,'\n');
fs.mkdirSync(path.join(here,'forward'),{recursive:true});
const pending=read('../candidate-native-pending-20261016/forward/08_candidate_context.sqlinc'),reviewed=read('../candidate-contract-move-lifecycle-20261016/forward/05_worked_fact_projection.sqlinc');
const a=" select coalesce(jsonb_agg(jsonb_build_object('id',v.id,'immutable_submission_json',v.immutable_submission_json,\n   'input_snapshot_json',v.input_snapshot_json) order by v.id),'[]'::jsonb)";
const x=reviewed.indexOf(" select coalesce(jsonb_agg(jsonb_build_object('worked_facts',"),z=reviewed.indexOf('\n  into native_workflows',x);
if(pending.split(a).length!==2||x<0||z<x)throw Error('EXACT_REVIEWED_AGGREGATE_REQUIRED');
fs.writeFileSync(path.join(here,'forward/00_current_body_witness.sqlinc'),pending.replace(a,()=>reviewed.slice(x,z)));
