// Finite Health source construction proof; no native or financial qualification.
import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
export const reviewPath='docs/healthroster-build/proposals/same-separate-f23-classification-20261017/CLASSIFICATION.json';
export const reviewSha256='f338e3eaf7f3ce2ff027cd2728cc5f16f0724ea4ce7eeea139f5217ee949766d';
const sha=x=>createHash('sha256').update(x).digest('hex'),canon=x=>x.toString().replaceAll('\r\n','\n');
const fail=code=>{throw Error('HEALTH_SAME_SEPARATE_TYPE_'+code);};
const includes=(body,...parts)=>parts.every(p=>body.includes(p));
const column=(schema,table,name,type)=>schema[table]?.columns.some(c=>c.name===name&&c.type===type)===true;
export function reverseLiteralHunks(source,hunks){
 for(const h of [...hunks].reverse()){
  const after=canon(h.after),before=canon(h.before),count=h.occurrences??1;
  if(!Number.isSafeInteger(count)||count<1||count>2||source.split(after).length!==count+1)fail('EXACT_REVERSE_ANCHOR');
  source=source.replaceAll(after,()=>before);
 }
 return source;
}
export function provesMigrationIdentifier(body){
 return includes(body,'declare v_name text;v_count integer;v_keys smallint[];',
  "from unnest(array['final_revision_id','work_event_id']) with ordinality key(name,ord)",
  "a.attrelid='public.weekly_source_final_snapshot_lines'::regclass",
  'and a.attname=key.name and not a.attisdropped;',
  'select count(*),min(conname::text) into v_count,v_name from pg_catalog.pg_constraint',
  "where conrelid='public.weekly_source_final_snapshot_lines'::regclass and contype='u' and conkey=v_keys;",
  'if v_count<>1 or cardinality(v_keys)<>2 then',
  "raise exception 'WEEKLY_SOURCE_PAYABLE_SNAPSHOT_UNIQUE_UNBOUND' using errcode='55000';end if;",
  "execute format('alter table public.weekly_source_final_snapshot_lines drop constraint %I',v_name);")
  &&body.indexOf('if v_count<>1')<body.indexOf('execute format(')
  &&body.split('execute format(').length===2;
}
export function provesExpenseJson(body,producer,schema){
 return column(schema,'public.weekly_source_row_expense_policy_snapshots','source_expense_pence','bigint')
  &&/\bv_values jsonb;v_pay jsonb;/.test(body)
  &&includes(body,'v_values:=private.weekly_source_fixed_expense_invoice_values_v1(p.id);',
   "v_pay:=v_values->'pay_vector'||jsonb_build_object('source_expense_pence',0);")
  &&includes(producer,'returns jsonb language plpgsql','v_replacement_pay_vector jsonb;',
   'v_replacement_pay_vector:=pg_catalog.jsonb_build_object(',
   "'schema_version','WEEKLY_SOURCE_FIXED_EXPENSE_VECTOR_V1','kind','PAY',",
   "'source_expense_pence',v_new_pence,",'v_new_pence:=v_expense_policy.source_expense_pence;',
   "return jsonb_build_object('schema_version','WEEKLY_SOURCE_FIXED_EXPENSE_INVOICE_VALUES_V1',",
   "'pay_vector',v_replacement_pay_vector,'charge_vector',v_replacement_charge_vector,");
}
export function provesInvoiceCompanions(sql,schema){
 return column(schema,'public.weekly_source_invoice_presentation_lines','id','uuid')
  &&sql.split('select pg_catalog.min(shift_line.id::text)::uuid').length===3
  &&includes(sql,"case when v_is_expense and v_movement.invoice_only_snapshot_line_id is not null then (",
   'from public.weekly_source_invoice_presentation_lines shift_line',
   'join public.weekly_source_invoice_line_bindings hours_binding on hours_binding.presentation_line_id=shift_line.id',
   "and hours_binding.state='CURRENT' and hours_binding.invoice_id=v_invoice_id",
   'hours_movement.invoice_only_snapshot_line_id=v_movement.invoice_only_snapshot_line_id',
   "hours_movement.source_line_kind='SOURCE_REBILL_LINE' and hours_movement.pay_effect='NONE'",
   'having count(*)=1',') when v_is_expense then (',
   'shift_line.work_event_id=v_movement.work_event_id',
   'shift_line.correction_role is not distinct from v_correction_role','having pg_catalog.count(*)=1');
}
export function provesNewScalar(signature,sql,body,schema){
 if(signature==='public.weekly_source_invoice_admit_atomic_v1(jsonb)')return provesInvoiceCompanions(sql,schema);
 if(signature==='private.weekly_source_paid_overlap_current_snapshot_v1(uuid,uuid,uuid,uuid,uuid,uuid,uuid[])')
  return column(schema,'public.weekly_source_final_snapshot_lines','id','uuid')
   &&includes(sql,'select s.id,scope.id as scope_id,','join public.weekly_source_final_snapshot_lines s on s.id=',
    ') select count(distinct id),max(id::text)::uuid,','into v_count,v_snapshot,v_rows from actual;')
   &&includes(body,'declare v_count integer;v_snapshot uuid;v_rows jsonb;',
    "if v_count>1 then raise exception 'WEEKLY_SOURCE_PAID_OVERLAP_CURRENT_SNAPSHOT_AMBIGUOUS' using errcode='55000';end if;",
    "'state',case when v_count=1 then 'CURRENT' else 'UNAVAILABLE' end,'snapshot_line_id',v_snapshot,");
 if(signature==='private.weekly_source_paid_match_retained_transition_assert_v1(uuid)')
  return column(schema,'public.weekly_source_billing_movements','id','uuid')
   &&includes(sql,"select count(*),max(value->>'current_pay_head_movement_id')::uuid into counted,movement_id",
    "from jsonb_array_elements(proof#>'{inputs,overlaps}') value where value->>'work_event_id'=t.work_event_id::text;")
   &&includes(body,'counted integer;movement_id uuid;',
    "if counted<>1 then raise exception 'WEEKLY_SOURCE_PAID_MATCH_PRIOR_PAY_BASIS_CHANGED' using errcode='55000';end if;",
    'select * into strict m from public.weekly_source_billing_movements where id=movement_id;',
    "item->>'movement_economic_hash' is distinct from encode(m.movement_economic_hash,'hex')")
   &&body.indexOf('if counted<>1')<body.indexOf('where id=movement_id;');
 if(signature==='private.weekly_source_nhsp_paid_overlap_basis_v1(uuid,uuid,uuid)')
  return column(schema,'private.bpay_next_source_current_scopes','id','uuid')
   &&includes(sql,'select count(*),min(scope.id::text)::uuid into v_scope_count,v_scope',
    'from private.bpay_next_source_current_scopes scope where scope.root_timesheet_id=v_root_id;')
   &&includes(body,'v_scope uuid;v_scope_count integer;',"if v_scope_count>1 then\n      raise exception 'WEEKLY_SOURCE_NHSP_PAID_BASIS_SCOPE_AMBIGUOUS'",
    'if v_scope_count=0 or not exists(',"'state','UNAVAILABLE','reason','NATIVE_CURRENT_UNCAPTURED','native_current_event',null));\n      continue;",
    'scope.id=v_scope','scope.root_timesheet_id=v_root_id');
 if(signature==='private.weekly_source_nhsp_negative_population_v1(uuid,uuid,uuid,uuid)')
  // This is deliberately INCOMPLETE evidence for count != 1. Its minimum
  // is never certified as a unique/current selector or writer authority.
  return column(schema,'public.weekly_source_projection_publications','id','uuid')
   &&includes(sql,'select count(*),min(x.id::text)::uuid into counted,p.id from public.weekly_source_projection_publications x',
    "x.authority_scope_kind='NHSP_REPORT_SCOPE' and x.authority_scope_version=scope.version")
   &&includes(body,'complete boolean:=true;',
    'if counted<>1 then complete:=false;\n  else\n    select * into strict p from public.weekly_source_projection_publications where id=p.id;',
    "'negative_population_complete',complete,'current_position_available',current_available,",
    "'no_reversal_proved',case when complete and current_available then to_jsonb(not matching) else 'null'::jsonb end,",
    "'paid_writer_permission',null);");
 if(signature==='private.weekly_source_known_source_key_v1(uuid,uuid,uuid)')
  return column(schema,'public.weekly_work_events','id','uuid')
   &&includes(sql,'select count(distinct event.id),min(event.id::text)::uuid,',
    'join public.weekly_work_events event on event.id=link.work_event_id',
    'into v_count,v_event,v_foreign_owner',"link.link_hash=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_WORK_EVENT_LINK_V1'")
   &&includes(body,'v_count integer;v_event uuid;',
    "if v_count>1 then raise exception 'WEEKLY_SOURCE_SOURCE_KEY_ALIAS_AMBIGUOUS' using errcode='55000';end if;",
    'if v_count=0 then return null;end if;',
    "if v_foreign_owner then raise exception 'WEEKLY_SOURCE_SOURCE_KEY_ALIAS_OWNER_CONFLICT' using errcode='55000';end if;",
    'return v_event;');
 return false;
}
export function createReviewedSameSeparateClassifier(root,{tupleProof,classifyPrior,hazardCensus,statementScan,readFile}={}){
 const read=file=>canon(readFile?readFile(file):fs.readFileSync(path.join(root,file)));
 const statements=source=>{if(typeof statementScan!=='function')fail('STATEMENT_SCANNER_REQUIRED');const p=statementScan(source);return p.statements.map(s=>p.source.slice(s.start,s.end).trim());};
 function load(){
  const raw=readFile?readFile(reviewPath):fs.readFileSync(path.join(root,reviewPath));if(sha(raw)!==reviewSha256)fail('REVIEW_CHANGED');const d=JSON.parse(raw);
  if(d.version!=='HEALTH_SAME_SEPARATE_FINITE_TYPE_PROPOSAL_V1'||d.native!=='NOT_RUN'||d.qualification!=='SOURCE_TYPES_AND_CONSTRUCTION_ONLY'
   ||d.counts.files!==78||d.counts.rawOccurrences!==458||d.counts.uniquePoints!==445||d.owners.length!==71||d.owners.filter(o=>o.priorFile).length!==37)fail('SCOPE_CHANGED');
  for(const pin of d.dependencies){
   let source=read(pin.path);
   if(pin.path==='scripts/healthroster-prebody-dynamic-review.mjs'&&pin.sha256==='079ea08052ad96600ee0f9bdfee70dbeccaa8d85acb8eaf51483a1be5e5f99c1'&&sha(source)!==pin.sha256){
    // Reconstruct only the exact current prebody successor before the retained older inverse.
    if(sha(source)==="960ada704674f7993cb4bd558bce5bbce2b311a523d50ed6d34a4d8497faaacc"){
     const h={"before":" const source=canonical(value);","after":" const source=canonical(value);\n // Exact approved stdout/snapshot successors have no prebody compiler effect.\n const historicalDependencies=[{\"file\":\"scripts/bpay-next-final-definers.mjs\",\"priorSha256\":\"51ab7fd6d5c582ee7f4cab612502c4fc7ac11088cb1a996dd2cf1e6561468d09\",\"currentSha256\":\"b6a8143f2bae077a4b72857419b4f8fab9f7652181f6ad3bf1fdcfbd4b77bafa\",\"hunks\":[{\"before\":\"\\nconst git = args => {\\n  const result = spawnSync('git', args, { cwd: repoRoot, encoding: 'utf8', windowsHide: true });\\n  if (result.status !== 0) throw Error('BPAY_NEXT_FINAL_DEFINER_GIT_FAILED');\\n  return result.stdout.split(/\\\\r?\\\\n/).filter(Boolean).map(file => file.replaceAll('\\\\\\\\', '/'));\\n\",\"after\":\"\\nconst git = args => {\\n  const result = spawnSync('git', args, { cwd: repoRoot, encoding: 'utf8', maxBuffer: 8 * 1024 * 1024, windowsHide: true });\\n  if (result.status !== 0) throw Error('BPAY_NEXT_FINAL_DEFINER_GIT_FAILED');\\n  return result.stdout.split(/\\\\r?\\\\n/).filter(Boolean).map(file => file.replaceAll('\\\\\\\\', '/'));\\n\"}]},{\"file\":\"scripts/healthroster-install1-full-rehearsal.mjs\",\"priorSha256\":\"af2b6b591a8d6c251ad08507130bd2cf0cc9861d966055174e511de9c4102754\",\"currentSha256\":\"31acd390acf2ef6dbbd062fbb157feea66f9b5d328440414e67b2bfd4a3f78ab\",\"hunks\":[{\"before\":\"import {assertTurnIdentity,ownTurnActivityExclusionSql} from './healthroster-turn-control.mjs';\\nimport {verifierClosure} from './healthroster-install1-verifier-census.mjs';\\nimport {backgroundSnapshotFieldsSql,fixtureObservationSql,transactionIdentitySql,validateFixtureObservations,assertQualifiedLiveRestoration} from './healthroster-live-test-restoration.mjs';\\nimport {assertFreshInstall2NativeGate} from './healthroster-install2-native-gate.mjs';\\nimport {compileReviewedManagedEnvelope,managedEnvelopeReviewPath,managedCalleeAuditPath} from './healthroster-reviewed-managed-envelope.mjs';\\n\",\"after\":\"import {assertTurnIdentity,ownTurnActivityExclusionSql} from './healthroster-turn-control.mjs';\\nimport {verifierClosure} from './healthroster-install1-verifier-census.mjs';\\nimport {boundBackgroundSnapshotFieldsSql,fixtureObservationSql,transactionIdentitySql,validateFixtureObservations,assertQualifiedLiveRestoration} from './healthroster-live-test-restoration.mjs';\\nimport {assertFreshInstall2NativeGate} from './healthroster-install2-native-gate.mjs';\\nimport {compileReviewedManagedEnvelope,managedEnvelopeReviewPath,managedCalleeAuditPath} from './healthroster-reviewed-managed-envelope.mjs';\\n\"},{\"before\":\"  chunks.push(`select ${literal(name)} relation,count(*) row_count,\\n   encode(pg_catalog.sha256(pg_catalog.convert_to(coalesce(string_agg(row_hash,'|' order by row_hash),''),'UTF8')),'hex') row_fingerprint\\n   from (select encode(pg_catalog.sha256(pg_catalog.convert_to(to_jsonb(t)::text,'UTF8')),'hex') row_hash from \\\"${row.schema}\\\".\\\"${row.name}\\\" t) hashed_rows`);\\n }\\n requireThat(chunks.length>0,'REHEARSAL_SNAPSHOT_BUSINESS_RELATIONS_REQUIRED');\\n\",\"after\":\"  chunks.push(`select ${literal(name)} relation,count(*) row_count,\\n   encode(pg_catalog.sha256(pg_catalog.convert_to(coalesce(string_agg(row_hash,'|' order by row_hash),''),'UTF8')),'hex') row_fingerprint\\n   from (select encode(pg_catalog.sha256(pg_catalog.convert_to(to_jsonb(t)::text,'UTF8')),'hex') row_hash from \\\"${row.schema}\\\".\\\"${row.name}\\\" t offset 0) hashed_rows`);\\n }\\n requireThat(chunks.length>0,'REHEARSAL_SNAPSHOT_BUSINESS_RELATIONS_REQUIRED');\\n\"},{\"before\":\" 'repeatables',(select coalesce(jsonb_agg(to_jsonb(l) order by path),'[]'::jsonb) from private.cloudtms_repeatable_ledger l),\\n 'releases',(select coalesce(jsonb_agg(to_jsonb(l) order by release_id),'[]'::jsonb) from private.cloudtms_database_releases l)) value),\\n catalogue as (${exportQuery})\\n select (identity_proof.jsonb_build_object::jsonb || jsonb_build_object(\\n 'ledgerFingerprint',${hash('select value from ledger')},\\n\",\"after\":\" 'repeatables',(select coalesce(jsonb_agg(to_jsonb(l) order by path),'[]'::jsonb) from private.cloudtms_repeatable_ledger l),\\n 'releases',(select coalesce(jsonb_agg(to_jsonb(l) order by release_id),'[]'::jsonb) from private.cloudtms_database_releases l)) value),\\n catalogue as (${exportQuery})${observeLiveBusiness?', background_statement_snapshot as materialized (select pg_current_snapshot() snapshot_value)':''}\\n select (identity_proof.jsonb_build_object::jsonb || jsonb_build_object(\\n 'ledgerFingerprint',${hash('select value from ledger')},\\n\"},{\"before\":\" 'businessRelations',(select coalesce(jsonb_agg(jsonb_build_object('relation',relation,'count',row_count,'hash',row_fingerprint) order by relation),'[]'::jsonb) from business),\\n 'relationCensus',${literal(digest(JSON.stringify([...names].sort())))},\\n ${observeLiveBusiness?backgroundSnapshotFieldsSql()+',':''}\\n 'otherActivityCount',(select count(*) from pg_catalog.pg_stat_activity where datname=current_database() and ${ownTurnActivityExclusionSql(identity)} and (state <> 'idle' or xact_start is not null))))::text from identity_proof;`;\\n}\\n\",\"after\":\" 'businessRelations',(select coalesce(jsonb_agg(jsonb_build_object('relation',relation,'count',row_count,'hash',row_fingerprint) order by relation),'[]'::jsonb) from business),\\n 'relationCensus',${literal(digest(JSON.stringify([...names].sort())))},\\n ${observeLiveBusiness?boundBackgroundSnapshotFieldsSql()+',':''}\\n 'otherActivityCount',(select count(*) from pg_catalog.pg_stat_activity where datname=current_database() and ${ownTurnActivityExclusionSql(identity)} and (state <> 'idle' or xact_start is not null))))::text from identity_proof;`;\\n}\\n\"}]},{\"file\":\"scripts/healthroster-live-test-restoration.mjs\",\"priorSha256\":\"fb4ffa42acf7cf319cdcb45a233babdb9720bb1bc73e4cb1f6ddcd3be696a4a6\",\"currentSha256\":\"a91c1bf04f868ff372b20fb6beca239a7807fe9cc92c0c2682afffcd79554adc\",\"hunks\":[{\"before\":\"const hash=sql=>`encode(pg_catalog.sha256(pg_catalog.convert_to((${sql})::text,'UTF8')),'hex')`;\\nconst key=relation=>relation===backgroundRelations[0]?'batch_receipt_id':'id';\\nexport function backgroundSnapshotFieldsSql(){\\n const batch=backgroundRelations[0],outbox=backgroundRelations[1];\\n // Native claim provenance: the completed native receipt, item identity,\\n\",\"after\":\"const hash=sql=>`encode(pg_catalog.sha256(pg_catalog.convert_to((${sql})::text,'UTF8')),'hex')`;\\nconst key=relation=>relation===backgroundRelations[0]?'batch_receipt_id':'id';\\nexport function backgroundSnapshotFieldsSql(){return renderBackgroundSnapshotFieldsSql(false);}\\n// The existing zero-argument renderer remains byte-identical for legacy callers.\\n// Only the owning snapshot query may bind this fixed one-row statement CTE.\\nexport function boundBackgroundSnapshotFieldsSql(){return renderBackgroundSnapshotFieldsSql(true);}\\nfunction renderBackgroundSnapshotFieldsSql(bound){\\n const activeSnapshot=bound?'(select snapshot_value from background_statement_snapshot)':'pg_current_snapshot()';\\n const batch=backgroundRelations[0],outbox=backgroundRelations[1];\\n // Native claim provenance: the completed native receipt, item identity,\\n\"},{\"before\":\"  and r.terminal_response_sha256=private._candidate_daily_json_sha256_v1(r.terminal_response_body)`;\\n const batches=`select coalesce(jsonb_agg(jsonb_build_object('keyHash',${hash('r.batch_receipt_id')},\\n   'rowHash',${hash('to_jsonb(r)')},'xmin',r.xmin::text,'xactStatus',case when pg_snapshot_xmax(pg_current_snapshot())::text::numeric<4294967296 then pg_xact_status(r.xmin::text::xid8) end,\\n   'nativeClaimProved',coalesce((${proof}),false),'createdAt',r.created_at_utc,'updatedAt',r.updated_at_utc,'completedAt',r.completed_at_utc)\\n   order by r.batch_receipt_id),'[]'::jsonb) from ${batch} r`;\\n const outboxes=`select coalesce(jsonb_agg(jsonb_build_object('keyHash',${hash('r.id')},'rowHash',${hash('to_jsonb(r)')},\\n   'stableHash',${hash(\\\"to_jsonb(r)-array['attempt_count','next_attempt_at']\\\")},'xmin',r.xmin::text,\\n   'xactStatus',case when pg_snapshot_xmax(pg_current_snapshot())::text::numeric<4294967296 then pg_xact_status(r.xmin::text::xid8) end,\\n   'attemptCount',r.attempt_count,'nextAttemptAt',r.next_attempt_at,'createdAt',r.created_at)\\n   order by r.id),'[]'::jsonb) from ${outbox} r`;\\n return `'backgroundObservedAt',clock_timestamp(),'backgroundSnapshotStartedAt',transaction_timestamp(),\\n  'backgroundSnapshotXmax',pg_snapshot_xmax(pg_current_snapshot())::text,\\n  'backgroundSnapshotEpochZero',pg_snapshot_xmax(pg_current_snapshot())::text::numeric<4294967296,\\n  'backgroundSnapshotXip',(select coalesce(jsonb_agg(x::text),'[]'::jsonb) from pg_snapshot_xip(pg_current_snapshot()) x),\\n  'backgroundRows',jsonb_build_object(${literal(batch)},(${batches}),${literal(outbox)},(${outboxes})),\\n  'backgroundWriterRoutines',(select coalesce(jsonb_agg(r order by r->>'schema',r->>'identity'),'[]'::jsonb)\\n\",\"after\":\"  and r.terminal_response_sha256=private._candidate_daily_json_sha256_v1(r.terminal_response_body)`;\\n const batches=`select coalesce(jsonb_agg(jsonb_build_object('keyHash',${hash('r.batch_receipt_id')},\\n   'rowHash',${hash('to_jsonb(r)')},'xmin',r.xmin::text,'xactStatus',case when pg_snapshot_xmax(${activeSnapshot})::text::numeric<4294967296 then pg_xact_status(r.xmin::text::xid8) end,\\n   'nativeClaimProved',coalesce((${proof}),false),'createdAt',r.created_at_utc,'updatedAt',r.updated_at_utc,'completedAt',r.completed_at_utc)\\n   order by r.batch_receipt_id),'[]'::jsonb) from ${batch} r`;\\n const outboxes=`select coalesce(jsonb_agg(jsonb_build_object('keyHash',${hash('r.id')},'rowHash',${hash('to_jsonb(r)')},\\n   'stableHash',${hash(\\\"to_jsonb(r)-array['attempt_count','next_attempt_at']\\\")},'xmin',r.xmin::text,\\n   'xactStatus',case when pg_snapshot_xmax(${activeSnapshot})::text::numeric<4294967296 then pg_xact_status(r.xmin::text::xid8) end,\\n   'attemptCount',r.attempt_count,'nextAttemptAt',r.next_attempt_at,'createdAt',r.created_at)\\n   order by r.id),'[]'::jsonb) from ${outbox} r`;\\n return `'backgroundObservedAt',clock_timestamp(),'backgroundSnapshotStartedAt',transaction_timestamp(),\\n  'backgroundSnapshotXmax',pg_snapshot_xmax(${activeSnapshot})::text,\\n  'backgroundSnapshotEpochZero',pg_snapshot_xmax(${activeSnapshot})::text::numeric<4294967296,\\n  'backgroundSnapshotXip',(select coalesce(jsonb_agg(x::text),'[]'::jsonb) from pg_snapshot_xip(${activeSnapshot}) x),\\n  'backgroundRows',jsonb_build_object(${literal(batch)},(${batches}),${literal(outbox)},(${outboxes})),\\n  'backgroundWriterRoutines',(select coalesce(jsonb_agg(r order by r->>'schema',r->>'identity'),'[]'::jsonb)\\n\"}]}];\n const historical=historicalDependencies.find(row=>row.file===input.file&&row.priorSha256===input.sha256);\n if(historical&&digest(source)!==input.sha256){\n  if(digest(source)!==historical.currentSha256)fail('BOUND_SOURCE_CHANGED');\n  let reversed=source;for(const h of [...historical.hunks].reverse()){if(reversed.split(h.after).length!==2)fail('BOUND_SOURCE_CHANGED');reversed=reversed.replace(h.after,()=>h.before);}\n  if(digest(reversed)!==input.sha256)fail('BOUND_SOURCE_CHANGED');return reversed;\n }"};if(source.split(h.after).length!==2)fail('PREBODY_SUCCESSOR_REVERSE_ANCHOR');source=source.replace(h.after,()=>h.before);
    }
    if(sha(source)!=='3504d0e858e1fdefe95edae2c86f14324944a96e1f27dc1a5caf097266898eef')fail('DEPENDENCY_CHANGED');
    const raw=fs.readFileSync(path.join(root,'docs/healthroster-build/v6-owner-delivery-20261016/SAME_SEPARATE_PREBODY_DEPENDENCY_FORWARD_20261017.json'));
    if(sha(raw)!=='6a349f504746da2545af40e7b83144c3e4b8312606761489cb6e396098cde4d9')fail('PREBODY_FORWARD_REVIEW_CHANGED');
    const delta=JSON.parse(raw);if(source.split(delta.hunk.after).length!==2)fail('PREBODY_FORWARD_REVERSE_ANCHOR');
    source=source.replace(delta.hunk.after,()=>delta.hunk.before);
   }
   if(sha(source)!==pin.sha256)fail('DEPENDENCY_CHANGED');
  }
  const schema=JSON.parse(read(d.schema.path));for(const[name,proof]of Object.entries(d.schema.relations)){
   if(proof.ddl){if(!statements(read(proof.ddlPath)).includes(proof.ddl))fail('CURRENT_DDL_CHANGED');}
   else{const actual=schema.relations.find(r=>r.schema+'.'+r.name===name);if(!actual||actual.kind!==proof.kind||JSON.stringify(actual.columns)!==JSON.stringify(proof.columns))fail('CATALOGUE_TYPE_CHANGED');}
  }
  for(const o of d.owners){const whole=statements(read(o.file)).filter(s=>s.startsWith('create or replace function '+o.signature.split('(')[0]+'('));
   if(whole.length!==1||sha(whole[0])!==o.successorStatementSha256)fail('COMPLETE_OWNER_CHANGED');if(!o.priorFile)continue;
   const prior=read(o.priorFile).trimEnd();if(sha(prior)!==o.priorStatementSha256||reverseLiteralHunks(whole[0]+'\n',o.hunks).trimEnd()!==prior)fail('FULL_OWNER_REVERSE_CHANGED');
   const parent=o.parentOwnerFile??o.currentFile??o.current?.physical;if(!parent||!statements(read(parent)).includes(prior))fail('ACTUAL_PARENT_CHANGED');
  }
  if(reverseLiteralHunks(read(d.classifier.target),d.classifier.hunks)!==read(d.classifier.inputTarget))fail('CLASSIFIER_REVERSE_CHANGED');
  // The entire new schema producer is source-pinned and its thirteen actual
  // ADD COLUMN types are independently proved in the PG17 AST receipt.
  if(sha(read(d.migration.file))!==d.migration.sha256||d.schema.addedColumns.length!==13)fail('SCHEMA_PRODUCER_CHANGED');
  return d;
 }
 return function reviewedSameSeparateHazards(source,file,baseline,violations,parts){
  if(!/^supabase\/(?:migrations\/17102026_0231_weekly_source_paid_same_separate_|repeatable\/17102026_(?:0231_weekly_source_paid_same_separate_health(?:\/|_v1\.sql$)|0335_weekly_source_paid_match_correction_receipt(?:\/|_v1\.sql$)))/.test(file))return baseline;
  const d=load(),entry=d.files.find(f=>f.file===file);if(!entry)return baseline;if(sha(canon(source))!==entry.sha256)fail('CURRENT_SOURCE_CHANGED');
  const actual=new Map();for(const p of violations){const old=actual.get(p.key);actual.set(p.key,{...p,count:(old?.count??0)+1});}if(actual.size!==entry.points.length)fail('POINT_CENSUS_CHANGED');
  const result=new Map(baseline);for(const p of entry.points){const found=actual.get(p.key);if(!found||found.hazard!==p.hazard||found.count!==p.count)fail('POINT_CHANGED');
   const[sig,hazard,sql]=JSON.parse(p.key),matching=parts.filter(b=>b.signature===sig&&b.text.includes(sql)),body=matching.length===1&&!matching[0].unclassified?matching[0].text:null;let proved=false;
   if(file===d.classifier.target){if(!classifyPrior)fail('PRIOR_CLASSIFIER_REQUIRED');const oldKey=JSON.stringify([sig.startsWith('outer:')?'outer:'+d.classifier.inputTarget:sig,hazard,sql]);proved=(classifyPrior(read(d.classifier.inputTarget),d.classifier.inputTarget,baseline).get(oldKey)??0)>=p.count;}
   else if(hazard.startsWith('ROWTYPE_SELECT:')){const[,variable,relationText]=hazard.split(':'),relation=relationText.replaceAll(' ','').toLowerCase();
    proved=!!body&&!!d.schema.relations[relation]&&typeof tupleProof==='function'&&tupleProof(found)&&new RegExp('\\b'+variable+'\\s+'+relation.replaceAll('.','\\.')+'\\s*%\\s*rowtype\\b','i').test(body);}
   else if(file===d.migration.file&&hazard==='DYNAMIC_SQL_UNCLASSIFIED')proved=provesMigrationIdentifier(source);
   else if(body&&hazard==='JSON_CONCAT')proved=provesExpenseJson(body,read(d.expenseProducer),d.schema.relations);
   else if(body&&(hazard==='UUID_CARDINALITY_UNCLASSIFIED'||hazard==='AGGREGATE_TYPE_UNCLASSIFIED'))proved=provesNewScalar(sig,sql,body,d.schema.relations);
   if(!proved){const t=d.transfers.find(t=>t.file===file&&t.key===p.key);if(t&&hazardCensus&&classifyPrior){const prior=read(t.currentFile);proved=hazardCensus(prior,t.currentFile).filter(x=>x.key===p.key).length>=p.count&&(classifyPrior(prior,t.currentFile,baseline).get(p.key)??0)>=p.count;}}
   if(!proved)fail('UNPROVED_'+hazard+':'+file+':'+sig);result.set(p.key,Math.max(result.get(p.key)??0,p.count));
  }
  return result;
 };
}
