import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),prior=path.resolve(here,'../automatic-native-delta');
const lane=path.resolve(here,'../../../..'),root=path.resolve(lane,'../healthroster-v2-backend-13102026');
const money=path.resolve(lane,'../healthroster-v2-money-build-13102026');
const hash=bytes=>crypto.createHash('sha256').update(bytes).digest('hex'),pins=[];
function input(base,file,authority){const bytes=fs.readFileSync(path.join(base,file));pins.push({authority,path:file,bytes:bytes.length,sha256:hash(bytes)});return bytes.toString('utf8');}
function replaceOnce(text,before,after){if(text.split(before).length!==2)throw new Error('FORWARD_HUNK_DRIFT');return text.replace(before,()=>after);}
const sql=input(prior,'08_automatic_reconcile_delta.sqlinc','FROZEN_AUTOMATIC23');
const oracle=input(prior,'reference-comparison.mjs','FROZEN_AUTOMATIC23');
fs.writeFileSync(path.join(here,'08_complete_automatic_reconcile_delta_forward.sqlinc'),replaceOnce(sql,"'segment_id','segment_key',","'segment_id','segment_key','segment_stable_key',"));
fs.writeFileSync(path.join(here,'reference-comparison.mjs'),replaceOnce(oracle,"'segment_id', 'segment_key', 'work_date',","'segment_id', 'segment_key', 'segment_stable_key', 'work_date',"));
for(const file of ['03_approved_detail_evidence.sqlinc','06_expense_evidence.sqlinc','07_staged_entitlement_components.sqlinc','10_complete_stage_request.sqlinc'])input(prior,file,'FROZEN_AUTOMATIC23');
const canonical=input(root,'supabase/repeatable/17092026_0300_weekly_source_entitlement_publication_v1.sql','CURRENT_ROOT_NATIVE_CODEC');
if(!canonical.includes("'segment_stable_key'"))throw new Error('NATIVE_CANONICAL_FIELD_MISSING');
input(money,'docs/healthroster-build/proposals/protected-source-capability/SOURCE_CAS.sqlinc','FROZEN_MONEY_CAPABILITY');
const cas=input(money,'docs/healthroster-build/proposals/native-financial-fixtures/CAPABILITY_SOURCE_CAS_SCHEMA_FORWARD.sqlinc','CURRENT_MONEY_SCHEMA_FORWARD');
const ownerCensus=JSON.parse(input(money,'docs/healthroster-build/proposals/native-financial-fixtures/CAPABILITY_OWN_STAGE_SOURCE_CENSUS.json','CURRENT_MONEY_CALLEE_CENSUS'));
const stage=fs.readFileSync(path.join(prior,'10_complete_stage_request.sqlinc'),'utf8');
const factStart=cas.indexOf("select jsonb_build_object('schema_version','WEEKLY_SOURCE_BULK_SOURCE_FACTS_V1'");
const factEnd=cas.indexOf(' into facts;',factStart);
if(factStart<0||factEnd<0)throw new Error('CAS_COMPLETE_FACTS_MISSING');
const factBody=cas.slice(factStart,factEnd);
// Parse only alternating arguments of the outer object. Nested effective_rules
// client_id is not another top-level Source fact.
const args=[];let depth=0,quoted=false,start=factBody.indexOf('(')+1;
for(let i=start-1;i<factBody.length;i++){
  const char=factBody[i];if(char==="'"){if(quoted&&factBody[i+1]==="'"){i++;continue;}quoted=!quoted;continue;}
  if(quoted)continue;if(char==='(')depth++;if(char===')'){depth--;if(depth===0){args.push(factBody.slice(start,i).trim());break;}}
  if(char===','&&depth===1){args.push(factBody.slice(start,i).trim());start=i+1;}
}
const categories=args.filter((_,i)=>i%2===0).map(key=>{const match=/^'([a-z_]+)'$/.exec(key);if(!match)throw new Error('OUTER_FACT_KEY_INVALID');return match[1];});
if(categories.length!==39||JSON.stringify(categories)!==JSON.stringify(ownerCensus.categories.map(x=>x.key))
  ||ownerCensus.source_owner.sha256!==hash(Buffer.from(cas)))throw new Error('MONEY_CALLEE_CENSUS_DRIFT');
const written=[...new Set([...stage.matchAll(/\b(?:insert\s+into|update)\s+((?:public|private)\.[a-z_0-9]+)/gi)].map(m=>m[1]))].sort();
const referenced=[...new Set([...cas.matchAll(/\b(?:from|join)\s+((?:public|private)\.[a-z_0-9]+)/gi)].map(m=>m[1]))].sort();
const intersection=written.filter(x=>referenced.includes(x));
if(intersection.length)throw new Error('OWN_STAGE_CAS_TABLE_OVERLAP '+intersection.join(','));
fs.writeFileSync(path.join(here,'SOURCE_CAS_STAGE_CENSUS.json'),JSON.stringify({schema_version:'AUTOMATIC_SOURCE_CAS_STAGE_CENSUS_V1',evidence:'SOURCE_ONLY_NOT_STAGE_BEFORE_AFTER_RUNTIME_PROOF',facts_categories:categories,stage_direct_write_relations:written,source_facts_referenced_relations:referenced,direct_relation_intersection:intersection,intersection_scope:'DIRECT_STAGE_BODY_ONLY_NOT_WHOLE_CALLEE_OR_TRIGGER_CLOSURE',money_callee_census:{reachable_owner_count:ownerCensus.reachable_owner_count,source_write_candidates:ownerCensus.source_write_candidates,known_intersections:ownerCensus.known_intersections,root_remaining_proofs:ownerCensus.root_remaining_proofs},indirect_delegates:['private.weekly_source_query_current_publication_v1','private.weekly_source_checking_client_scope_v1','private._weekly_source_effective_policy_v1','private.weekly_source_effective_rule_set_v1'],indirect_effects:'Native protected caller graph has Timesheet/TSFIN lifecycle writes and prepare-family missing Source event branch; root same-transaction all-category equality required',runtime_equality:'HELD_ROOT_NATIVE_C03',mutable_protection_rows_selected:false,mutable_timesheet_financial_lifecycle_selected:false,financial_identity_authority:'Immutable Source ordinary pay receipt movement identity/hash; actual Contract candidate identity'},null,2)+'\n');
fs.writeFileSync(path.join(here,'CONTROLLING_INPUTS.json'),JSON.stringify({schema_version:'AUTOMATIC_FORWARD_INPUTS_V1',inputs:pins},null,2)+'\n');
console.log(JSON.stringify({forward_helpers:1,independent_stable_key_fence:true,direct_relation_intersection:intersection,root_or_frozen_edits:0}));
