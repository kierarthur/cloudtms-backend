import {execFileSync} from 'node:child_process';
import {writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const base=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(base,'../../../..');
const commit='18f844f1c01c7d1cf293bc4e92a2ab7e0fb762eb',inputs=[];
const prefix='docs/healthroster-build/proposals/source-contact-recomposition-native-20261016/forward/';
const sha=s=>createHash('sha256').update(s).digest('hex');
function source(file){const text=execFileSync('git',['show',`${commit}:${prefix+file}`],{cwd:root,encoding:'utf8',maxBuffer:8e6}).replace(/\r\n/g,'\n');
 inputs.push({path:prefix+file,source_commit:commit,sha256_normalized_lf:sha(text)});return text;}
function extract(text,name){const start=text.indexOf(`create or replace function ${name}(`),end=text.indexOf('$function$;',text.indexOf('as $function$',start));
 if(start<0||end<start)throw Error('Complete checkpoint owner '+name);return text.slice(start,end+12)+'\n';}
function once(text,old,next){if(text.split(old).length!==2)throw Error('Unique native source anchor '+old.slice(0,130));return text.replace(old,next);}
let prepare=extract(source('03_native_route_prepare.sqlinc'),'public.weekly_source_manager_route_prepare_atomic_v1');
prepare=once(prepare,`perform private.weekly_contact_source_cycle_lock_v1((select source_cycle_id
    from public.weekly_message_intents where id=v_intent_id))`,`perform private.weekly_contact_source_intent_lock_v1(v_intent_id)`);
let manager=extract(source('05_native_manager_generation.sqlinc'),'private.weekly_source_query_manager_generation_v1');
manager=once(manager,`perform private.weekly_contact_source_cycle_lock_v1((select source_cycle_id
    from public.weekly_manager_recipient_routes where id=p_recipient_route_id))`,`perform private.weekly_contact_source_route_lock_v1(p_recipient_route_id)`);
let candidate=extract(source('06_native_candidate_activation.sqlinc'),'private.weekly_source_query_candidate_generation_v1');
candidate=once(candidate,'perform private.weekly_contact_source_cycle_lock_v1(p_source_cycle_id)',
 `perform private.weekly_contact_source_submission_lock_v1(p_source_cycle_id,p_candidate_id,
    jsonb_build_array(jsonb_build_object('client_id',p_client_id)),p_projection_publication_id)`);
const recomposition=source('07_native_unsubmitted_recomposition.sqlinc');
let retire=extract(recomposition,'private.weekly_contact_source_retire_unsubmitted_v1');
retire=once(retire,'perform private.weekly_contact_source_cycle_lock_v1(i.source_cycle_id)',
 'perform private.weekly_contact_source_intent_lock_v1(i.id)');
let recompose=extract(recomposition,'private.weekly_contact_source_manager_recompose_v1');
recompose=once(recompose,'perform private.weekly_contact_source_cycle_lock_v1(r.source_cycle_id)',
 'perform private.weekly_contact_source_route_lock_v1(r.id)');
const output=[prepare,manager,candidate,retire,recompose].join('\n').trimEnd()+'\n';
writeFileSync(path.join(base,'forward/42_exact_native_lock_owners.sqlinc'),output);
writeFileSync(path.join(base,'EXACT_NATIVE_LOCK_OWNER_INPUTS.json'),JSON.stringify({version:'CONTACT_EXACT_NATIVE_LOCK_OWNERS_V1',inputs,
 output:{path:'forward/42_exact_native_lock_owners.sqlinc',sha256:sha(output)},
 qualification:'Finite full checkpoint owners; exact generation/intent/route transaction footprints, no cycle-wide history locks for single work; native lock/concurrency qualification NOT_RUN.'},null,2)+'\n');
console.log(JSON.stringify({complete_owners:5,source_only:true}));
