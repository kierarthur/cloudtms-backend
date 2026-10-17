// Health READ/classification relocation only. This proves source ancestry,
// never native financial qualification or public READ_RESULT availability.
import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';

export const reviewPath='docs/healthroster-build/proposals/source-current-read-peer-f23-20261017/CLASSIFICATION.json';
export const reviewSha256='7aa875bf450b81be3d945182c67398c4134f3cb325ab88ff2aa089ac99f2a399';
const sha=value=>createHash('sha256').update(value).digest('hex');
const canonical=value=>value.toString().replaceAll('\r\n','\n');
const fail=code=>{throw Error('HEALTH_SOURCE_READ_TYPE_'+code);};
const scoped=file=>file.startsWith('supabase/repeatable/17102026_0722_weekly_source_current_read_health')
  ||file.startsWith('supabase/repeatable/17102026_0741_weekly_source_complete_classification')
  ||file==='supabase/verification/15092026_1534_weekly_source_acl_contract_v1.sql'
  ||file==='supabase/verification/17102026_0722_source_current_read_security.sql'
  ||file==='supabase/verification/17102026_0741_source_complete_security.sql'
  ||file==='supabase/migrations/17102026_0749_source_current_read_private_admission.sql';

function reverse(source,hunks){
  for(const hunk of [...hunks].reverse()){
    const after=canonical(hunk.after),before=canonical(hunk.before);
    if(!after||source.split(after).length!==2)fail('WHOLE_OWNER_REVERSE_ANCHOR');
    source=source.replace(after,()=>before);
  }
  return source;
}
function census(points){
  const result=new Map();
  for(const point of points){
    const prior=result.get(point.key);
    if(prior&&prior.hazard!==point.hazard)fail('CENSUS_COLLISION');
    result.set(point.key,{hazard:point.hazard,count:(prior?.count??0)+1});
  }
  return result;
}

export function createReviewedSourceReadClassifier(root,{classifyPrior,hazardCensus,readFile}={}){
  const raw=file=>readFile?readFile(file):fs.readFileSync(path.join(root,file));
  const read=file=>canonical(raw(file));
  function load(){
    const bytes=raw(reviewPath);
    if(sha(bytes)!==reviewSha256)fail('REVIEW_CHANGED');
    const proof=JSON.parse(bytes);
    if(proof.version!=='HEALTH_SOURCE_READ_FINITE_STATEMENT_TRANSFER_V1'
      ||proof.native!=='NOT_RUN'||proof.qualification!=='UNCHANGED_ACTUAL_OWNER_STATEMENTS_ONLY'
      ||proof.variants.length!==17||new Set(proof.variants.map(v=>v.file)).size!==16
      ||proof.reverses.length!==4||proof.transfers.length!==38
      ||JSON.stringify(proof.stages)!=='[{"files":11,"keys":19},{"files":5,"keys":19}]')fail('SCOPE_CHANGED');
    if(typeof classifyPrior!=='function'||typeof hazardCensus!=='function')fail('PRIOR_PROOF_REQUIRED');
    for(const dependency of proof.dependencies){
      let dependencySource=raw(dependency.path);
      if(dependency.path==='scripts/healthroster-same-separate-type-review.mjs'&&dependency.sha256==='897df1661b265921f960c107f5189470475866ad8311aec358b15f8841391259'&&sha(dependencySource)!==dependency.sha256){
        if(sha(dependencySource)!=="563fcc88819328fd9f18a31675b39814f80fb36a7ba6a5f96807c14a5ceef4b0")fail('PRIOR_HELPER_CHANGED');
        const h={"before":"    if(sha(source)!=='3504d0e858e1fdefe95edae2c86f14324944a96e1f27dc1a5caf097266898eef')fail('DEPENDENCY_CHANGED');","after":"    // Reconstruct only the exact current prebody successor before the retained older inverse.\n    if(sha(source)===\"960ada704674f7993cb4bd558bce5bbce2b311a523d50ed6d34a4d8497faaacc\"){\n     const h={\"before\":\" const source=canonical(value);\",\"after\":\" const source=canonical(value);\\n // Exact approved stdout/snapshot successors have no prebody compiler effect.\\n const historicalDependencies=[{\\\"file\\\":\\\"scripts/bpay-next-final-definers.mjs\\\",\\\"priorSha256\\\":\\\"51ab7fd6d5c582ee7f4cab612502c4fc7ac11088cb1a996dd2cf1e6561468d09\\\",\\\"currentSha256\\\":\\\"b6a8143f2bae077a4b72857419b4f8fab9f7652181f6ad3bf1fdcfbd4b77bafa\\\",\\\"hunks\\\":[{\\\"before\\\":\\\"\\\\nconst git = args => {\\\\n  const result = spawnSync('git', args, { cwd: repoRoot, encoding: 'utf8', windowsHide: true });\\\\n  if (result.status !== 0) throw Error('BPAY_NEXT_FINAL_DEFINER_GIT_FAILED');\\\\n  return result.stdout.split(/\\\\\\\\r?\\\\\\\\n/).filter(Boolean).map(file => file.replaceAll('\\\\\\\\\\\\\\\\', '/'));\\\\n\\\",\\\"after\\\":\\\"\\\\nconst git = args => {\\\\n  const result = spawnSync('git', args, { cwd: repoRoot, encoding: 'utf8', maxBuffer: 8 * 1024 * 1024, windowsHide: true });\\\\n  if (result.status !== 0) throw Error('BPAY_NEXT_FINAL_DEFINER_GIT_FAILED');\\\\n  return result.stdout.split(/\\\\\\\\r?\\\\\\\\n/).filter(Boolean).map(file => file.replaceAll('\\\\\\\\\\\\\\\\', '/'));\\\\n\\\"}]},{\\\"file\\\":\\\"scripts/healthroster-install1-full-rehearsal.mjs\\\",\\\"priorSha256\\\":\\\"af2b6b591a8d6c251ad08507130bd2cf0cc9861d966055174e511de9c4102754\\\",\\\"currentSha256\\\":\\\"31acd390acf2ef6dbbd062fbb157feea66f9b5d328440414e67b2bfd4a3f78ab\\\",\\\"hunks\\\":[{\\\"before\\\":\\\"import {assertTurnIdentity,ownTurnActivityExclusionSql} from './healthroster-turn-control.mjs';\\\\nimport {verifierClosure} from './healthroster-install1-verifier-census.mjs';\\\\nimport {backgroundSnapshotFieldsSql,fixtureObservationSql,transactionIdentitySql,validateFixtureObservations,assertQualifiedLiveRestoration} from './healthroster-live-test-restoration.mjs';\\\\nimport {assertFreshInstall2NativeGate} from './healthroster-install2-native-gate.mjs';\\\\nimport {compileReviewedManagedEnvelope,managedEnvelopeReviewPath,managedCalleeAuditPath} from './healthroster-reviewed-managed-envelope.mjs';\\\\n\\\",\\\"after\\\":\\\"import {assertTurnIdentity,ownTurnActivityExclusionSql} from './healthroster-turn-control.mjs';\\\\nimport {verifierClosure} from './healthroster-install1-verifier-census.mjs';\\\\nimport {boundBackgroundSnapshotFieldsSql,fixtureObservationSql,transactionIdentitySql,validateFixtureObservations,assertQualifiedLiveRestoration} from './healthroster-live-test-restoration.mjs';\\\\nimport {assertFreshInstall2NativeGate} from './healthroster-install2-native-gate.mjs';\\\\nimport {compileReviewedManagedEnvelope,managedEnvelopeReviewPath,managedCalleeAuditPath} from './healthroster-reviewed-managed-envelope.mjs';\\\\n\\\"},{\\\"before\\\":\\\"  chunks.push(`select ${literal(name)} relation,count(*) row_count,\\\\n   encode(pg_catalog.sha256(pg_catalog.convert_to(coalesce(string_agg(row_hash,'|' order by row_hash),''),'UTF8')),'hex') row_fingerprint\\\\n   from (select encode(pg_catalog.sha256(pg_catalog.convert_to(to_jsonb(t)::text,'UTF8')),'hex') row_hash from \\\\\\\"${row.schema}\\\\\\\".\\\\\\\"${row.name}\\\\\\\" t) hashed_rows`);\\\\n }\\\\n requireThat(chunks.length>0,'REHEARSAL_SNAPSHOT_BUSINESS_RELATIONS_REQUIRED');\\\\n\\\",\\\"after\\\":\\\"  chunks.push(`select ${literal(name)} relation,count(*) row_count,\\\\n   encode(pg_catalog.sha256(pg_catalog.convert_to(coalesce(string_agg(row_hash,'|' order by row_hash),''),'UTF8')),'hex') row_fingerprint\\\\n   from (select encode(pg_catalog.sha256(pg_catalog.convert_to(to_jsonb(t)::text,'UTF8')),'hex') row_hash from \\\\\\\"${row.schema}\\\\\\\".\\\\\\\"${row.name}\\\\\\\" t offset 0) hashed_rows`);\\\\n }\\\\n requireThat(chunks.length>0,'REHEARSAL_SNAPSHOT_BUSINESS_RELATIONS_REQUIRED');\\\\n\\\"},{\\\"before\\\":\\\" 'repeatables',(select coalesce(jsonb_agg(to_jsonb(l) order by path),'[]'::jsonb) from private.cloudtms_repeatable_ledger l),\\\\n 'releases',(select coalesce(jsonb_agg(to_jsonb(l) order by release_id),'[]'::jsonb) from private.cloudtms_database_releases l)) value),\\\\n catalogue as (${exportQuery})\\\\n select (identity_proof.jsonb_build_object::jsonb || jsonb_build_object(\\\\n 'ledgerFingerprint',${hash('select value from ledger')},\\\\n\\\",\\\"after\\\":\\\" 'repeatables',(select coalesce(jsonb_agg(to_jsonb(l) order by path),'[]'::jsonb) from private.cloudtms_repeatable_ledger l),\\\\n 'releases',(select coalesce(jsonb_agg(to_jsonb(l) order by release_id),'[]'::jsonb) from private.cloudtms_database_releases l)) value),\\\\n catalogue as (${exportQuery})${observeLiveBusiness?', background_statement_snapshot as materialized (select pg_current_snapshot() snapshot_value)':''}\\\\n select (identity_proof.jsonb_build_object::jsonb || jsonb_build_object(\\\\n 'ledgerFingerprint',${hash('select value from ledger')},\\\\n\\\"},{\\\"before\\\":\\\" 'businessRelations',(select coalesce(jsonb_agg(jsonb_build_object('relation',relation,'count',row_count,'hash',row_fingerprint) order by relation),'[]'::jsonb) from business),\\\\n 'relationCensus',${literal(digest(JSON.stringify([...names].sort())))},\\\\n ${observeLiveBusiness?backgroundSnapshotFieldsSql()+',':''}\\\\n 'otherActivityCount',(select count(*) from pg_catalog.pg_stat_activity where datname=current_database() and ${ownTurnActivityExclusionSql(identity)} and (state <> 'idle' or xact_start is not null))))::text from identity_proof;`;\\\\n}\\\\n\\\",\\\"after\\\":\\\" 'businessRelations',(select coalesce(jsonb_agg(jsonb_build_object('relation',relation,'count',row_count,'hash',row_fingerprint) order by relation),'[]'::jsonb) from business),\\\\n 'relationCensus',${literal(digest(JSON.stringify([...names].sort())))},\\\\n ${observeLiveBusiness?boundBackgroundSnapshotFieldsSql()+',':''}\\\\n 'otherActivityCount',(select count(*) from pg_catalog.pg_stat_activity where datname=current_database() and ${ownTurnActivityExclusionSql(identity)} and (state <> 'idle' or xact_start is not null))))::text from identity_proof;`;\\\\n}\\\\n\\\"}]},{\\\"file\\\":\\\"scripts/healthroster-live-test-restoration.mjs\\\",\\\"priorSha256\\\":\\\"fb4ffa42acf7cf319cdcb45a233babdb9720bb1bc73e4cb1f6ddcd3be696a4a6\\\",\\\"currentSha256\\\":\\\"a91c1bf04f868ff372b20fb6beca239a7807fe9cc92c0c2682afffcd79554adc\\\",\\\"hunks\\\":[{\\\"before\\\":\\\"const hash=sql=>`encode(pg_catalog.sha256(pg_catalog.convert_to((${sql})::text,'UTF8')),'hex')`;\\\\nconst key=relation=>relation===backgroundRelations[0]?'batch_receipt_id':'id';\\\\nexport function backgroundSnapshotFieldsSql(){\\\\n const batch=backgroundRelations[0],outbox=backgroundRelations[1];\\\\n // Native claim provenance: the completed native receipt, item identity,\\\\n\\\",\\\"after\\\":\\\"const hash=sql=>`encode(pg_catalog.sha256(pg_catalog.convert_to((${sql})::text,'UTF8')),'hex')`;\\\\nconst key=relation=>relation===backgroundRelations[0]?'batch_receipt_id':'id';\\\\nexport function backgroundSnapshotFieldsSql(){return renderBackgroundSnapshotFieldsSql(false);}\\\\n// The existing zero-argument renderer remains byte-identical for legacy callers.\\\\n// Only the owning snapshot query may bind this fixed one-row statement CTE.\\\\nexport function boundBackgroundSnapshotFieldsSql(){return renderBackgroundSnapshotFieldsSql(true);}\\\\nfunction renderBackgroundSnapshotFieldsSql(bound){\\\\n const activeSnapshot=bound?'(select snapshot_value from background_statement_snapshot)':'pg_current_snapshot()';\\\\n const batch=backgroundRelations[0],outbox=backgroundRelations[1];\\\\n // Native claim provenance: the completed native receipt, item identity,\\\\n\\\"},{\\\"before\\\":\\\"  and r.terminal_response_sha256=private._candidate_daily_json_sha256_v1(r.terminal_response_body)`;\\\\n const batches=`select coalesce(jsonb_agg(jsonb_build_object('keyHash',${hash('r.batch_receipt_id')},\\\\n   'rowHash',${hash('to_jsonb(r)')},'xmin',r.xmin::text,'xactStatus',case when pg_snapshot_xmax(pg_current_snapshot())::text::numeric<4294967296 then pg_xact_status(r.xmin::text::xid8) end,\\\\n   'nativeClaimProved',coalesce((${proof}),false),'createdAt',r.created_at_utc,'updatedAt',r.updated_at_utc,'completedAt',r.completed_at_utc)\\\\n   order by r.batch_receipt_id),'[]'::jsonb) from ${batch} r`;\\\\n const outboxes=`select coalesce(jsonb_agg(jsonb_build_object('keyHash',${hash('r.id')},'rowHash',${hash('to_jsonb(r)')},\\\\n   'stableHash',${hash(\\\\\\\"to_jsonb(r)-array['attempt_count','next_attempt_at']\\\\\\\")},'xmin',r.xmin::text,\\\\n   'xactStatus',case when pg_snapshot_xmax(pg_current_snapshot())::text::numeric<4294967296 then pg_xact_status(r.xmin::text::xid8) end,\\\\n   'attemptCount',r.attempt_count,'nextAttemptAt',r.next_attempt_at,'createdAt',r.created_at)\\\\n   order by r.id),'[]'::jsonb) from ${outbox} r`;\\\\n return `'backgroundObservedAt',clock_timestamp(),'backgroundSnapshotStartedAt',transaction_timestamp(),\\\\n  'backgroundSnapshotXmax',pg_snapshot_xmax(pg_current_snapshot())::text,\\\\n  'backgroundSnapshotEpochZero',pg_snapshot_xmax(pg_current_snapshot())::text::numeric<4294967296,\\\\n  'backgroundSnapshotXip',(select coalesce(jsonb_agg(x::text),'[]'::jsonb) from pg_snapshot_xip(pg_current_snapshot()) x),\\\\n  'backgroundRows',jsonb_build_object(${literal(batch)},(${batches}),${literal(outbox)},(${outboxes})),\\\\n  'backgroundWriterRoutines',(select coalesce(jsonb_agg(r order by r->>'schema',r->>'identity'),'[]'::jsonb)\\\\n\\\",\\\"after\\\":\\\"  and r.terminal_response_sha256=private._candidate_daily_json_sha256_v1(r.terminal_response_body)`;\\\\n const batches=`select coalesce(jsonb_agg(jsonb_build_object('keyHash',${hash('r.batch_receipt_id')},\\\\n   'rowHash',${hash('to_jsonb(r)')},'xmin',r.xmin::text,'xactStatus',case when pg_snapshot_xmax(${activeSnapshot})::text::numeric<4294967296 then pg_xact_status(r.xmin::text::xid8) end,\\\\n   'nativeClaimProved',coalesce((${proof}),false),'createdAt',r.created_at_utc,'updatedAt',r.updated_at_utc,'completedAt',r.completed_at_utc)\\\\n   order by r.batch_receipt_id),'[]'::jsonb) from ${batch} r`;\\\\n const outboxes=`select coalesce(jsonb_agg(jsonb_build_object('keyHash',${hash('r.id')},'rowHash',${hash('to_jsonb(r)')},\\\\n   'stableHash',${hash(\\\\\\\"to_jsonb(r)-array['attempt_count','next_attempt_at']\\\\\\\")},'xmin',r.xmin::text,\\\\n   'xactStatus',case when pg_snapshot_xmax(${activeSnapshot})::text::numeric<4294967296 then pg_xact_status(r.xmin::text::xid8) end,\\\\n   'attemptCount',r.attempt_count,'nextAttemptAt',r.next_attempt_at,'createdAt',r.created_at)\\\\n   order by r.id),'[]'::jsonb) from ${outbox} r`;\\\\n return `'backgroundObservedAt',clock_timestamp(),'backgroundSnapshotStartedAt',transaction_timestamp(),\\\\n  'backgroundSnapshotXmax',pg_snapshot_xmax(${activeSnapshot})::text,\\\\n  'backgroundSnapshotEpochZero',pg_snapshot_xmax(${activeSnapshot})::text::numeric<4294967296,\\\\n  'backgroundSnapshotXip',(select coalesce(jsonb_agg(x::text),'[]'::jsonb) from pg_snapshot_xip(${activeSnapshot}) x),\\\\n  'backgroundRows',jsonb_build_object(${literal(batch)},(${batches}),${literal(outbox)},(${outboxes})),\\\\n  'backgroundWriterRoutines',(select coalesce(jsonb_agg(r order by r->>'schema',r->>'identity'),'[]'::jsonb)\\\\n\\\"}]}];\\n const historical=historicalDependencies.find(row=>row.file===input.file&&row.priorSha256===input.sha256);\\n if(historical&&digest(source)!==input.sha256){\\n  if(digest(source)!==historical.currentSha256)fail('BOUND_SOURCE_CHANGED');\\n  let reversed=source;for(const h of [...historical.hunks].reverse()){if(reversed.split(h.after).length!==2)fail('BOUND_SOURCE_CHANGED');reversed=reversed.replace(h.after,()=>h.before);}\\n  if(digest(reversed)!==input.sha256)fail('BOUND_SOURCE_CHANGED');return reversed;\\n }\"};if(source.split(h.after).length!==2)fail('PREBODY_SUCCESSOR_REVERSE_ANCHOR');source=source.replace(h.after,()=>h.before);\n    }\n    if(sha(source)!=='3504d0e858e1fdefe95edae2c86f14324944a96e1f27dc1a5caf097266898eef')fail('DEPENDENCY_CHANGED');"};const source=dependencySource.toString();if(source.split(h.after).length!==2)fail('PRIOR_HELPER_REVERSE_ANCHOR');dependencySource=source.replace(h.after,()=>h.before);
      }
      if(sha(dependencySource)!==dependency.sha256)fail('PRIOR_HELPER_CHANGED');
    }
    for(const parent of proof.actualParents)if(sha(read(parent.source??parent.path))!==parent.sha256)fail('ACTUAL_PARENT_CHANGED');
    for(const variant of proof.variants)if(sha(read(variant.source))!==variant.sha256)fail('EVIDENCE_SOURCE_CHANGED');
    for(const owner of proof.reverses){
      const source=read(owner.output),parent=read(owner.parentSource??owner.parent);
      if(sha(source)!==owner.outputSha256||sha(parent)!==owner.inputSha256
        ||reverse(source,owner.hunks)!==parent)fail('FULL_OWNER_REVERSE_CHANGED');
    }
    return proof;
  }
  return function reviewedSourceReadHazards(source,file,baseline,violations){
    if(!scoped(file))return baseline;
    const proof=load(),normalized=canonical(source);
    const variants=proof.variants.filter(v=>v.file===file&&sha(normalized)===v.sha256);
    if(variants.length!==1)fail('CURRENT_SOURCE_CHANGED');
    const entry=variants[0],actual=census(hazardCensus(normalized,file));
    if(actual.size!==entry.points.length)fail('POINT_CENSUS_CHANGED');
    if(violations){
      const supplied=census(violations);
      if(supplied.size!==actual.size||[...actual].some(([key,value])=>supplied.get(key)?.count!==value.count
        ||supplied.get(key)?.hazard!==value.hazard))fail('CALLER_CENSUS_CHANGED');
    }
    const result=new Map(baseline),priorResults=new Map();
    for(const point of entry.points){
      const found=actual.get(point.key);
      if(!found||found.hazard!==point.hazard||found.count!==point.count)fail('POINT_CHANGED');
      const transfers=proof.transfers.filter(t=>t.file===file&&t.key===point.key);
      // The mandatory verifier has the same one statement in both approved
      // stages; duplicate provenance is acceptable only when identical.
      if(!transfers.length||transfers.some(t=>JSON.stringify(t)!==JSON.stringify(transfers[0])))fail('TRANSFER_CHANGED');
      const transfer=transfers[0],currentKey=JSON.parse(point.key),priorKey=JSON.parse(transfer.priorKey);
      if(currentKey.length!==3||priorKey.length!==3||currentKey[1]!==priorKey[1]||currentKey[2]!==priorKey[2]
        ||currentKey[0]!=='outer:'+file||priorKey[0]!=='outer:'+transfer.priorFile)fail('WHOLE_STATEMENT_CHANGED');
      const parent=proof.actualParents.find(p=>p.path===transfer.priorFile);
      if(!parent)fail('PRIOR_OWNER_MISSING');
      const priorSource=read(parent.source??parent.path);
      if(!priorResults.has(transfer.priorFile)){
        const points=census(hazardCensus(priorSource,transfer.priorFile));
        const qualified=classifyPrior(priorSource,transfer.priorFile,baseline);
        priorResults.set(transfer.priorFile,{points,qualified});
      }
      const prior=priorResults.get(transfer.priorFile);
      if(prior.points.get(transfer.priorKey)?.count!==point.count
        ||(prior.qualified.get(transfer.priorKey)??0)<point.count)fail('PRIOR_STATEMENT_UNPROVED:'+file+':'+point.hazard);
      result.set(point.key,Math.max(result.get(point.key)??0,point.count));
    }
    return result;
  };
}
