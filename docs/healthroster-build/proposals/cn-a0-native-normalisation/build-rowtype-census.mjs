import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
const root=process.argv[2],here=path.dirname(fileURLToPath(import.meta.url)),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const contractFile='supabase/release/current-contract.json',contractBytes=fs.readFileSync(path.join(root,contractFile)),contract=JSON.parse(contractBytes);
const tables=new Map(contract.relations.filter(t=>Array.isArray(t.columns)).map(t=>[t.schema+'.'+t.name,new Set(t.columns.map(c=>c.name.toLowerCase()))]));
const migrationFile='supabase/migrations/13102026_1127_healthroster_timesheet_correction_requests_v1.sql';
const migrationBytes=fs.readFileSync(path.join(root,migrationFile)),migration=migrationBytes.toString('utf8');
const requestFields=[...migration.matchAll(/^\s{2}([a-z_]\w*)\s+(?:uuid|text|bigint|integer|date|jsonb|timestamptz)\b/gm)].map(m=>m[1]);
tables.set('private.timesheet_correction_requests',new Set(requestFields));
const inputs=[],routines=[],unmapped=[];
function mask(s){return s.replace(/\/\*[\s\S]*?\*\//g,' ').replace(/--[^\n]*/g,' ').replace(/'(?:''|[^'])*'/g,"''");}
for(const dir of ['supabase/repeatable/13102026_1127_healthroster_correction_requests','supabase/repeatable/13102026_1143_healthroster_scoped_credit']){
 for(const name of fs.readdirSync(path.join(root,dir)).filter(n=>n.endsWith('.sqlinc'))){
  const file=dir+'/'+name,b=fs.readFileSync(path.join(root,file)),s=b.toString('utf8');inputs.push({file,sha256:sha(b)});
  const re=/create\s+or\s+replace\s+function\s+([\w.]+)[\s\S]*?\bas\s+(\$\w*\$)([\s\S]*?)\2\s*;/gi;
  for(const m of s.matchAll(re)){
   const body=mask(m[3]),variables=[...body.matchAll(/\b([a-z_]\w*)\s+((?:public|private)\.[a-z_]\w*)\s*%rowtype/gi)].map(x=>({variable:x[1].toLowerCase(),relation:x[2].toLowerCase()}));
   const head=mask(m[0].slice(0,m[0].indexOf(m[3])));
   for(const p of head.matchAll(/\b([a-z_]\w*)\s+((?:public|private)\.[a-z_]\w*)/gi))if(tables.has(p[2].toLowerCase()))variables.push({variable:p[1].toLowerCase(),relation:p[2].toLowerCase()});
   for(const v of variables){
    const refs=[...body.matchAll(new RegExp('\\b'+v.variable+'\\.([a-z_]\\w*)','gi'))].map(x=>x[1].toLowerCase());
    const fields=[...new Set(refs)].sort(),cols=tables.get(v.relation);
    if(!cols){unmapped.push({file,routine:m[1],...v,fields,reason:'Relation is not in controlling generated contract; pending new migration authority needs root catalogue qualification.'});continue;}
    routines.push({file,routine:m[1],...v,columnSource:v.relation==='private.timesheet_correction_requests'?'DECLARED_NEW_MIGRATION':'GENERATED_CONTRACT',fields,missing:fields.filter(f=>!cols.has(f))});
   }
  }
 }
}
const physicalTs=tables.get('public.timesheets');
const result={contract:'HEALTHROSTER_CN_ACTUAL_ROWTYPE_SOURCE_CENSUS_V1',status:'SOURCE_COLUMN_CENSUS_NOT_SQL_COMPILE_OR_RUNTIME',schemaInput:{file:contractFile,sha256:sha(contractBytes)},
 declaredNewRelation:{file:migrationFile,sha256:sha(migrationBytes),relation:'private.timesheet_correction_requests',columns:requestFields,status:'SOURCE_DECLARATION_ACTUAL_CATALOGUE_PENDING'},
 physicalTimesheetFacts:{candidate_id:physicalTs.has('candidate_id'),family_booking_id:physicalTs.has('family_booking_id'),booking_id:physicalTs.has('booking_id'),contract_id:physicalTs.has('contract_id')},
 inputs,routines,missing:routines.filter(r=>r.missing.length),unmapped,
 limitation:'Checks explicit %rowtype variable dereferences per function against the current generated contract. It does not replace real relation-alias, view/record/trigger/enum/function/API or actual runtime qualification. New private request rowtypes are documented separately from source and remain root catalogue gates.'};
fs.writeFileSync(path.join(here,'ACTUAL_ROWTYPE_CENSUS.json'),JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({typedScopes:routines.length,generatedContractScopes:routines.filter(r=>r.columnSource==='GENERATED_CONTRACT').length,
 declaredMigrationScopes:routines.filter(r=>r.columnSource==='DECLARED_NEW_MIGRATION').length,
 missing:result.missing,unmapped:unmapped.length,physicalTimesheetFacts:result.physicalTimesheetFacts}));
