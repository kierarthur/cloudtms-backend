import fs from 'node:fs';
import path from 'node:path';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {fileURLToPath,pathToFileURL} from 'node:url';

export const sha=value=>createHash('sha256').update(value).digest('hex');
export const lf=value=>String(value).replaceAll('\r\n','\n');
const here=path.dirname(fileURLToPath(import.meta.url));
export const migrations=[
 {path:'supabase/migrations/16102026_1205_weekly_contact_20_automatic_followup_tranche.sql',sha256:'e45f1a433cfb7f6dacad7bc01a5191f40ec176e33f44658fe0e3d0457eadfc17',locks:[['weekly_manager_recipient_generations','access exclusive'],['weekly_message_dispatch_commands','access exclusive']]},
 {path:'supabase/migrations/16102026_1205_weekly_contact_30_submission_cohort_identity.sql',sha256:'4da7a96a5e9b2c2f7778d5478a4a7ea49586c4893d76ac309b3d36ef7132120f',locks:[['weekly_candidate_cohorts','share'],['weekly_timesheet_submission_requests','access exclusive'],['weekly_candidate_outreach_generations','share'],['weekly_timesheet_submission_request_memberships','share']]},
];
const entryModules=['scripts/cloudtms-managed-release.mjs','scripts/cloudtms-db-release-lib.mjs','scripts/healthroster-rehearsal-turn.mjs','scripts/cloudtms-managed-psql.mjs','scripts/healthroster-install1-full-rehearsal.mjs'];
const dataInputs=['supabase/release/migration-lock.json','supabase/release/export_contract.sql','supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql','supabase/migrations/30092026_2059_weekly_source_independent_candidate_requests.sql',
 'docs/healthroster-build/v6-owner-delivery-20261016/native-money-predecessor-contention-efd18231/build-input.mjs.source.txt',
 'docs/healthroster-build/v6-owner-delivery-20261016/native-money-predecessor-contention-efd18231/INPUT_RECEIPT.json',
 'docs/healthroster-build/v6-owner-delivery-20261016/native-money-predecessor-contention-efd18231/SUMMARY.json'];
export async function buildPlan(root){
 root=path.resolve(root);const read=file=>lf(fs.readFileSync(path.join(root,file),'utf8'));
 const inputMap=new Map();
 const add=file=>{if(inputMap.has(file))return;const source=read(file);inputMap.set(file,{file,sha256:sha(source),bytes:Buffer.byteLength(source),source});
  if(file.endsWith('.mjs'))for(const m of source.matchAll(/(?:from\s*|import\s*)['"]([^'"]+)['"]/g))if(m[1].startsWith('.')){
   const target=path.posix.normalize(path.posix.join(path.posix.dirname(file),m[1]));if(target.startsWith('../'))throw Error('INPUT_IMPORT_ESCAPES_ROOT');add(target);
  }
 };
 for(const file of [...entryModules,...dataInputs,...migrations.map(x=>x.path)])add(file);
 const load=file=>import(pathToFileURL(path.join(root,file)).href);
 const compiler=await load('scripts/cloudtms-managed-release.mjs'),sql=await load('scripts/cloudtms-managed-release-sql.mjs'),lib=await load('scripts/cloudtms-db-release-lib.mjs');
 const oldMode=process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER='CURRENT_USER';
 let packets;
 try{packets=migrations.map(spec=>{
  if(sha(read(spec.path))!==spec.sha256)throw Error('EXACT_MAPPED_MIGRATION_CHANGED');
  const locks=JSON.parse(read('supabase/release/migration-lock.json')).migrations.filter(row=>row.path===spec.path);
  if(locks.length!==1||locks[0].sha256!==spec.sha256)throw Error('IMMUTABLE_MIGRATION_LOCK_MISMATCH');
  const unit=compiler.compileManagedUnit(locks[0],'migration',{root,mapSource:lib.mapExecutableSqlSource});
  if(unit.exception||unit.ownerRlsEnvelope||unit.immutableGuardDependency||unit.contactClaimCompanion||unit.moneyPredecessorCompanion||unit.capabilityPredecessorCompanion)throw Error('UNEXPECTED_CONTACT_PACKET_COMPANION');
  // This reference packet is NEVER executable by this runner. The canonical
  // ledger suffix retains an explicitly non-release qualification identifier.
  const packet=compiler.unitCheckpointSql(unit,{releaseId:'healthroster-contact-six-lock-qualification-NOT-FOR-APPLY'});
  const scan=sql.scanManagedSql(packet),statements=scan.statements.map(row=>({start:row.start,end:row.end,sql:packet.slice(row.start,row.end),tokens:row.tokens}));
  if(scan.meta.length||statements[0].sql!=='begin;'||statements[1].sql!==compiler.assertWriterSql)throw Error('CANONICAL_PACKET_PREFIX_CHANGED');
  for(const [index,[table,mode]]of spec.locks.entries())if(statements[index+2].sql!==`lock table public.${table} in ${mode} mode nowait;`)throw Error('EXACT_CANONICAL_LOCK_PREFIX_CHANGED');
  if(statements[spec.locks.length+2].tokens[0]!=='DO')throw Error('FIRST_ORIGINAL_ASSERTION_CHANGED');
  return {path:spec.path,sourceSha256:spec.sha256,packet,packetSha256:sha(packet),body:unit.body,bodySha256:sha(unit.body),statements,
   firstOriginalStatement:spec.locks.length+3,firstOriginalOffset:statements[spec.locks.length+2].start,locks:spec.locks};
 });}finally{if(oldMode===undefined)delete process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;else process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER=oldMode;}
 const cases=packets.flatMap((packet,packetIndex)=>packet.locks.map(([table,mode],index)=>{
  const id=`contact${packetIndex===0?'20':'30'}-lock${index+1}`,target=packet.statements[index+2];
  const holderMode=mode==='share'?'row exclusive':'access share';
  return {id,migrationPath:packet.path,migrationSha256:packet.sourceSha256,canonicalPacketSha256:packet.packetSha256,
   targetTable:'public.'+table,targetMode:mode,holderMode,holderReadOnly:holderMode==='access share',
   holderLockSql:`lock table public.${table} in ${holderMode} mode nowait;`,
   canonicalTargetOrdinal:index+3,targetOffset:target.start,targetEnd:target.end,
   commands:packet.statements.slice(0,index+3).map(row=>row.sql),
   prefix:packet.packet.slice(0,target.end),prefixSha256:sha(packet.packet.slice(0,target.end)),
   preceding:packet.locks.slice(0,index).map(([name,lockMode])=>({table:'public.'+name,mode:lockMode})),
   expectedSqlState:'55P03',firstOriginalStatement:packet.firstOriginalStatement,
   assertionsBodyLedgerNeverTransmitted:true,expectedTargetConflict:'IMMEDIATE_NOWAIT_NOT_LOCK_TIMEOUT'};
 }));
 const positiveSql=`begin isolation level read committed;\n${compiler.assertWriterSql}\n${packets.map(row=>row.body).join('\n')}\nrollback;`;
 return {version:'HEALTH_CONTACT_EXACT_SIX_LOCK_QUALIFICATION_SOURCE_V1',root,rootHead:execFileSync('git',['rev-parse','HEAD'],{cwd:root,encoding:'utf8'}).trim(),
  sourceState:'SAVED_DISK_PINNED_NO_CLEAN_OR_NATIVE_CLAIM',producerCommit:'bdb86a015c343b685f94682e5ae644f63fab7468',
  historicalPeer:'7297352399dd7afe21abdf459e0b438f836e6cfb',
  marker:'hr_install2_6cae14b7a1024ec790a63e4e1bd91bd8',markerPolicy:'ABSENCE_SENTINEL_ONLY_NEVER_CREATED',
  relationNames:cases.map(row=>row.targetTable),inputs:[...inputMap.values()].map(({source,...row})=>row),packets,cases,
  positive:{sql:positiveSql,sha256:sha(positiveSql),scope:'EXACT_TWO_COMPLETE_CANONICAL_BODIES_READ_COMMITTED_ROLLBACK_NO_LEDGER_OR_RELEASE_ROWS',
   writerGate:'Root must own the real managed-writer advisory lock on this same execution session before sending this SQL.',
   native:'NOT_RUN',restorationRequired:true},
  limits:{native:'NOT_RUN',maximumConcurrentDatabaseSessions:2,fixtures:0,markerDdl:0,originalBodyExecuted:false,
   databaseCallsDuringPreparation:0,outerRepeatableReadSnapshotCurrentness:'NOT_QUALIFIED_BY_LOCK_CONTENTION',
   fullNativeMigrationPrerequisites:'See POSITIVE_PREREQUISITES.md; no prerequisite is synthesized.'}};
}
export function assertSourcePins(plan,root){
 for(const row of plan.inputs)if(sha(lf(fs.readFileSync(path.join(root,row.file),'utf8')))!==row.sha256)throw Error('QUALIFICATION_SOURCE_PIN_CHANGED');
 for(const row of plan.cases){if(sha(row.prefix)!==row.prefixSha256||row.commands.join('\n').includes('insert into private.cloudtms_migration_ledger'))throw Error('QUALIFICATION_PREFIX_CHANGED');}
 return true;
}
export function writePlan(plan,out){
 fs.mkdirSync(out,{recursive:true});
 const save=(file,value)=>{const target=path.join(out,file);fs.mkdirSync(path.dirname(target),{recursive:true});fs.writeFileSync(target,value);};
 for(const row of plan.inputs)save('inputs/root/'+row.file,lf(fs.readFileSync(path.join(plan.root,row.file),'utf8')));
 for(const [index,packet]of plan.packets.entries())save(`packets/contact${index===0?'20':'30'}-COMPLETE_REFERENCE_NOT_EXECUTED.sql`,packet.packet+'\n');
 for(const [index,packet]of plan.packets.entries())save(`positive/contact${index===0?'20':'30'}-COMPLETE_CANONICAL_BODY.sql`,packet.body);
 save('positive/CONTACT20_30_READ_COMMITTED_ROLLBACK.sql',plan.positive.sql);
 for(const row of plan.cases){save('cases/'+row.id+'-EXACT_PREFIX.sql',row.prefix+'\n');save('cases/'+row.id+'-HOLDER.sql',`begin ${row.holderReadOnly?'read only':'read write'};\nset local lock_timeout='2s';\n${row.holderLockSql}\n-- Retain until writer expected refusal, then ROLLBACK; never commit.\n`);}
 save('PLAN.json',JSON.stringify(plan,null,2)+'\n');
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 const root=process.argv[2];if(!root)throw Error('EXPLICIT_ROOT_REQUIRED');const plan=await buildPlan(root);writePlan(plan,here);
 console.log(JSON.stringify({cases:plan.cases.length,sourceInputs:plan.inputs.length,completeCanonicalPackets:plan.packets.length,native:'NOT_RUN'}));
}
