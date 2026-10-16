"""Finite source review. Parses genuine PG17 SQL ASTs; never connects to a database."""
import hashlib,json,pathlib,re,sys
sys.path.insert(0,'C:/tmp/healthroster-v6-physical-parser')
import pglast
from pglast import parser
here=pathlib.Path(__file__).resolve().parent
if sys.platform=='win32':here=pathlib.Path('\\\\?\\'+str(here))
read=lambda file:(here/file).read_text(encoding='utf-8')
sha=lambda value:hashlib.sha256(value.encode() if isinstance(value,str) else value).hexdigest()
stable=lambda value:json.dumps(value,sort_keys=True,separators=(',',':'))
def strip(value):
 if isinstance(value,list):return [strip(v) for v in value]
 if isinstance(value,dict):return {k:strip(v) for k,v in value.items() if not k.endswith('location') and k!='stmt_len'}
 return value
def ast(source):return strip(json.loads(parser.parse_sql_json(source)))
def nodes(value,kind):
 out=[]
 if isinstance(value,list):
  for item in value:out+=nodes(item,kind)
 elif isinstance(value,dict):
  if kind in value:out.append(value[kind])
  for item in value.values():out+=nodes(item,kind)
 return out
def strings(values):return [v['String']['sval'] for v in values]
census=json.loads(read('ACTUAL_CENSUS.json'));deps=json.loads(read('DEPENDENCIES.json'))
contract=json.loads(read('inputs/native-source/supabase/release/current-contract.json'))
relations={r['schema']+'.'+r['name']:r for r in contract['relations']}
schema10='supabase/migrations/16102026_1205_weekly_contact_10_producer_journals.sql'
schema30='supabase/migrations/16102026_1205_weekly_contact_30_submission_cohort_identity.sql'
ctors={};ctor_sources={}
for file,folder in [(schema10,'inputs/actual/'),('supabase/migrations/15102026_0802_weekly_contact_pause_permits.sql','inputs/native-source/'),('supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql','inputs/native-source/')]:
 source=read(folder+file);tree=ast(re.sub(r'^\\.*$','',source,flags=re.M))
 for create in nodes(tree,'CreateStmt'):
  relation=create['relation'];name=relation['schemaname']+'.'+relation['relname'];ctors[name]=create;ctor_sources[name]=file
tuple_evidence={};scalar_evidence={}
def relation_evidence(name):
 if name not in tuple_evidence:
  if name in relations:tuple_evidence[name]={'kind':'EXACT_NATIVE_SOURCE_CONTRACT_RELATION','relation':relations[name],'relationObjectSha256':sha(stable(relations[name])),'dependency':'supabase/release/current-contract.json','installedStateClaim':False}
  else:
   assert name in ctors,name
   tuple_evidence[name]={'kind':'EXACT_PENDING_CREATE_TABLE_AST','constructorFile':ctor_sources[name],'completeCreateAst':ctors[name],'completeCreateAstSha256':sha(stable(ctors[name])),'installedStateClaim':False}
 return tuple_evidence[name]
def column_evidence(ref,expected):
 schema,table,column=ref.split('.');relation=schema+'.'+table
 if relation in relations:
  col=next(c for c in relations[relation]['columns'] if c['name']==column)
  actual=col['type'];proof={'kind':'EXACT_NATIVE_SOURCE_COLUMN','column':col,'dependency':'supabase/release/current-contract.json'}
 else:
  create=ctors[relation];col=next(e['ColumnDef'] for e in create['tableElts'] if 'ColumnDef'in e and e['ColumnDef']['colname']==column)
  actual='.'.join(strings(col['typeName']['names']));proof={'kind':'EXACT_PENDING_CONSTRUCTOR_COLUMN','columnAst':col,'constructorFile':ctor_sources[relation],'completeCreateAstSha256':sha(stable(create))}
 assert actual in expected,(ref,actual,expected)
 proof['resolvedType']=actual;scalar_evidence[ref]=proof
 return ref
integer={'integer','int4','pg_catalog.int4'};timestamp={'timestamp with time zone','timestamptz','pg_catalog.timestamptz'};date={'date','pg_catalog.date'}
aggregate_inputs={
19:[('public.weekly_contact_delivery_memberships.accepted_at_utc',timestamp),('public.weekly_message_dispatch_commands.provider_accepted_at_utc',timestamp),('public.weekly_message_provider_attempts.completed_at_utc',timestamp),('public.weekly_message_target_attempts.completed_at_utc',timestamp)],
20:[('public.weekly_contact_source_ordinal_owners.send_ordinal',integer)],
45:[('public.weekly_manager_recipient_generations.generation_number',integer)],47:[('public.weekly_message_intents.tranche_sequence',integer)],
64:[('public.weekly_contact_source_intent_capture_members.routine_step',integer)],76:[('public.weekly_message_renders.render_sequence',integer)],
84:[('public.weekly_source_row_resolutions.generation',integer)],
86:[('public.weekly_contact_pause_scopes.worker_paused_until_utc',timestamp),('public.weekly_contact_pause_scopes.manager_paused_until_utc',timestamp),('public.weekly_candidate_outreach_generations.reminder_due_at_utc',timestamp),('public.weekly_candidate_outreach_generations.deadline_at_utc',timestamp),('public.weekly_manager_cohort_due_events.due_at_utc',timestamp),('public.mail_outbox.next_attempt_at_utc',timestamp)],
139:[('public.weekly_manager_cohort_due_events.due_at_utc',timestamp)],152:[('public.weekly_source_row_resolutions.generation',integer)],
223:[('public.weekly_timesheet_submission_request_memberships.week_ending',date)],278:[('public.weekly_message_renders.render_sequence',integer)],
288:[('public.weekly_manager_cohort_due_events.due_at_utc',timestamp)],291:[('public.weekly_message_intents.tranche_sequence',integer)],293:[('public.weekly_message_intents.tranche_sequence',integer)]}
aggregate_operands={19:[('min','x.accepted_at')],20:[('max','send_ordinal')],45:[('max','g.generation_number')],47:[('max','intent.tranche_sequence')],64:[('max','capture_member.routine_step')],76:[('max','render_sequence')],84:[('max','current_resolution.generation')],86:[('min','value')],139:[('min','event.due_at_utc'),('min','event.due_at_utc')],152:[('max','current_resolution.generation')],223:[('min','membership.week_ending')],278:[('max','render_sequence')],288:[('min','event.due_at_utc'),('min','event.due_at_utc')],291:[('max','tranche_sequence')],293:[('max','tranche_sequence')]}
def sql_without_target(statement):
 return re.sub(r'\binto\s+(?:strict\s+)?[a-z_][a-z0-9_]*(?:\s*,\s*[a-z_][a-z0-9_]*)*\s+(?=from\b)','',statement,flags=re.I)
rows=[];tuple_count=0;aggregate_count=0;dynamic_count=0
for row in census['rows']:
 out={**row};body=read(row['bodyFile']);assert sha(body)==row['bodySha256']
 if row['baselineCovered']:
  out['proof']={'verdict':'EXACT_FROZEN_BASELINE_OCCURRENCE_ONLY','approvalTransfer':False};rows.append(out);continue
 if row['hazard'].startswith('ROWTYPE_SELECT:'):
  _,variable,declared=row['hazard'].split(':');name=re.sub(r'\s','',declared).lower()
  assert re.search(r'\b'+variable+r'\s+'+re.escape(name).replace(r'\.',r'\s*\.\s*')+r'\s*%\s*rowtype\b',body,re.I),(row['id'],name)
  matches=list(re.finditer(r'\bselect\s+\*\s+into\s+(?:strict\s+)?'+variable+r'\s+from\b',row['statement'],re.I));assert len(matches)==1,row['id']
  selected=row['statement'][matches[0].start():];query=sql_without_target(selected);tree=ast(query)
  assert len(tree['stmts'])==1 and 'SelectStmt'in tree['stmts'][0]['stmt'],row['id']
  select=tree['stmts'][0]['stmt']['SelectStmt'];targets=select['targetList'];sources=select['fromClause']
  assert len(targets)==1 and targets[0]['ResTarget']['val']=={'ColumnRef':{'fields':[{'A_Star':{}}]}},row['id']
  assert len(sources)==1 and 'RangeVar'in sources[0],row['id']
  source=sources[0]['RangeVar'];assert source['schemaname']+'.'+source['relname']==name,row['id']
  relation_evidence(name)
  out['proof']={'verdict':'FINITE_SAME_RELATION_WHOLE_TUPLE_PROVED','variable':variable.lower(),'declaredRelation':name,'selectedRelation':name,'selectedSql':selected,'sqlWithPlpgsqlTargetRemoved':query,'completeSelectAst':tree,'completeSelectAstSha256':sha(stable(tree)),'surroundingLoopSpan':matches[0].start()>0,'cardinalityApproval':'NONE_EXISTING_STRICT_LIMIT_AND_FAILURE_SEMANTICS_PRESERVED','projection':'ONE_STAR_FROM_EXACT_DECLARED_RELATION_NO_OUTER_JOIN'};tuple_count+=1
 elif row['hazard']=='AGGREGATE_TYPE_UNCLASSIFIED':
  statement=row['statement']
  if row['id']in(76,278):statement=re.split(r'\bthen\b',statement,maxsplit=1,flags=re.I)[1].strip()
  if row['id']in(139,152):statement=statement[re.search(r'\bselect\b',statement,re.I).start():];statement=re.split(r'\bloop\b',statement,maxsplit=1,flags=re.I)[0].strip()
  query=sql_without_target(statement);tree=ast(query)
  calls=[]
  for call in nodes(tree,'FuncCall'):
   fname=strings(call['funcname'])[-1]
   if fname in('min','max'):
    arg=call['args'];assert len(arg)==1 and 'ColumnRef'in arg[0],row['id']
    calls.append((fname,'.'.join(strings(arg[0]['ColumnRef']['fields']))))
  assert sorted(calls)==sorted(aggregate_operands[row['id']]),(row['id'],calls)
  inputs=[column_evidence(ref,expected) for ref,expected in aggregate_inputs[row['id']]]
  bindings={}
  for relation in nodes(tree,'RangeVar'):
   if 'schemaname'in relation:bindings.setdefault(relation.get('alias',{}).get('aliasname',relation['relname']),set()).add(relation['schemaname']+'.'+relation['relname'])
  if row['id']==19:
   assert re.search(r'for\s+c\s+in\s+select\s+command\.\*\s+from\s+public\.weekly_message_dispatch_commands\s+command\b',body,re.I)
   assert 'select c.provider_accepted_at_utc where c.provider_accepted_at_utc is not null'in statement
   bindings['c']={'public.weekly_message_dispatch_commands'}
  if row['id']==86:
   assert re.search(r'\bscope\s+public\.weekly_contact_pause_scopes\s*%rowtype\b',body,re.I)
   bindings['scope']={'public.weekly_contact_pause_scopes'}
  for ref in inputs:
   schema,table,column=ref.split('.');assert any(schema+'.'+table in values for values in bindings.values()),(row['id'],ref)
   assert re.search(r'\b'+re.escape(column)+r'\b',statement),ref
  out['proof']={'verdict':'FINITE_EXACT_SCALAR_AGGREGATE_PROVED','operators':[{'name':name,'operand':operand} for name,operand in calls],'inputEvidence':inputs,'actualAstAndOuterRecordBindings':{k:sorted(v)for k,v in bindings.items()},'sqlWithPlpgsqlTargetRemoved':query,'completeSqlAst':tree,'completeSqlAstSha256':sha(stable(tree)),'cardinalityApproval':'NONE_SCALAR_TYPE_ONLY_NO_UUID_REDUCTION','unionTypeProof':'ALL_FOUR_ARMS_TIMESTAMPTZ_WITH_REAL_COMMAND_RECORD_ALIAS' if row['id']==19 else 'ALL_SIX_ARMS_TIMESTAMPTZ_FROM_EXACT_SCOPE_ROW_AND_NATIVE_COLUMNS' if row['id']==86 else None};aggregate_count+=1
 elif row['hazard']=='DYNAMIC_SQL_UNCLASSIFIED':
  if row['id']<=7:
   assert row['file']==schema10
   tables=['weekly_contact_checking_scope_versions','weekly_contact_manual_reviews','weekly_contact_manual_acceptances','weekly_contact_source_intent_captures','weekly_contact_source_intent_capture_members','weekly_contact_source_ordinal_owners','weekly_contact_recheck_progress','weekly_contact_recheck_steps','weekly_contact_source_due_event_rebases']
   guards=['weekly_contact_manual_reviews','weekly_contact_manual_acceptances','weekly_contact_source_intent_captures','weekly_contact_source_intent_capture_members','weekly_contact_recheck_steps','weekly_contact_source_due_event_rebases']
   selected=guards if row['id']==6 else tables
   for table in selected:assert 'public.'+table in ctors
   loops=re.findall(r'foreach\s+relation\s+in\s+array\s+array\[([^\]]*)\]\s+loop',body,re.I)
   parsed=[re.findall(r"'([a-z_]+)'",value) for value in loops]
   assert parsed==([tables]if row['id']<=5 else[guards,tables])
   assert not re.search(r'\brelation\s*(?::=|=)|\binto\s+(?:strict\s+)?relation\b',body,re.I)
   formats=re.findall(r"execute\s+format\(\s*'([^']*)'\s*,(.*?)\)\s*;",body,re.I|re.S)
   expected=['alter table public.%I owner to %I','alter table public.%I enable row level security','alter table public.%I force row level security','create policy cloudtms_miget_service_owner_all on public.%I for all to CURRENT_USER,service_role using(true) with check(true)','revoke all on public.%I from public,anon,authenticated,service_role','create trigger %I before update or delete on public.%I for each row execute function private.weekly_contact_producer_immutable_v1()','create trigger %I before truncate on public.%I for each statement execute function private.weekly_contact_producer_immutable_v1()']
   assert formats==[(expected[i],args)for i,(_,args)in enumerate(formats,start=0 if row['id']<=5 else 5)]
   expected_args=['relation,CURRENT_USER','relation','relation','relation','relation',"relation||'_immutable',relation","relation||'_no_truncate',relation"]
   assert [re.sub(r'\s','',args)for _,args in formats]==expected_args[:5]if row['id']<=5 else [re.sub(r'\s','',args)for _,args in formats]==expected_args[5:]
   actual_format=expected[row['id']-1];executions=[]
   for table in selected:
    args=[table,'__REVIEWED_CURRENT_USER_IDENTIFIER__']if row['id']==1 else[table+('_immutable'if row['id']==6 else'_no_truncate'),table]if row['id']>=6 else[table]
    sql=actual_format
    for argument in args:sql=sql.replace('%I','"'+argument+'"',1)
    assert '%I'not in sql;executions.append({'target':'public.'+table,'sql':sql,'ast':ast(sql)})
   out['proof']={'verdict':'FINITE_CLOSED_CONTACT_DDL_IDENTIFIERS_PROVED','literalFormat':actual_format,'literalRelations':['public.'+x for x in selected],'identifierProvenance':'ONLY_FOREACH_CLOSED_LITERAL_ARRAY_AND_CURRENT_USER_FOR_OWNER','formatArguments':['relation','CURRENT_USER']if row['id']==1 else["relation||'_immutable'",'relation']if row['id']==6 else["relation||'_no_truncate'",'relation']if row['id']==7 else['relation'],'expandedSqlAstEvidence':executions,'dataValueInterpolation':False,'callerIdentifier':False,'effectBoundary':'OWN_NINE_NEW_JOURNALS_OWNER_RLS_FIXED_SERVICE_POLICY_REVOKE_OR_IMMUTABLE_TRIGGER_ONLY'}
  else:
   assert row['id']==8 and row['file']==schema30
   native=relations['public.weekly_timesheet_submission_requests'];unique=[c for c in native['constraints']if c['type']=='u'and c['definition']=='UNIQUE (source_cycle_id, candidate_id, request_generation)']
   assert len(unique)==1 and unique[0]['validated']and not unique[0]['deferrable']and not unique[0]['deferred']
   create=ctors['public.weekly_timesheet_submission_requests'];constraints=[e['Constraint']for e in create['tableElts']if 'Constraint'in e and e['Constraint']['contype']=='CONSTR_UNIQUE'];assert len(constraints)==1 and strings(constraints[0]['keys'])==['source_cycle_id','candidate_id','request_generation']
   for needle in ["select conname into strict prior_constraint from pg_constraint where conrelid=relation and contype='u' and conkey=expected",'and convalidated and not condeferrable and not condeferred',"unnest(array['source_cycle_id','candidate_id','request_generation'])with ordinality",'CONTACT_SUBMISSION_NATIVE_INDEX_DRIFT','CONTACT_SUBMISSION_RETAINED_IDENTITY_UNRESOLVED',"execute format('alter table public.weekly_timesheet_submission_requests drop constraint %I',prior_constraint);"]:assert needle in body
   source=read('inputs/actual/'+schema30);locks=nodes(ast(re.sub(r'^\\.*$','',source,flags=re.M)),'LockStmt');assert len(locks)==4 and all(l['nowait']for l in locks)
   assert source.index('lock table')<source.index('do $block$')<source.index(row['statement'])
   out['proof']={'verdict':'FINITE_CONTACT_EXACT_UNIQUE_PREDECESSOR_PROVED','fixedRelation':'public.weekly_timesheet_submission_requests','constructorFile':ctor_sources['public.weekly_timesheet_submission_requests'],'constructorConstraint':constraints[0],'nativeContractConstraint':unique[0],'runtimeIdentityPredicate':'EXACT_RELATION_OID_UNIQUE_THREE_ORDERED_NATIVE_ATTNUMS_VALIDATED_NONDEFERRABLE_NONDEFERRED_INTO_STRICT','runtimeCardinality':'ZERO_OR_MORE_THAN_ONE_MATCH_REFUSES_BEFORE_EXECUTE','identifierProvenance':'ONLY_SINGLE_MATCHED_CONSTRAINT_CONNAME_ESCAPED_PERCENT_I','effect':'ONE_UNIQUE_CONSTRAINT_DROP_FOLLOWED_BY_STATIC_FOUR_COLUMN_COHORT_UNIQUE_AND_EXACT_ACTIVE_INDEX_REPLACEMENT','sameTransactionLocks':locks,'lockReviewCommit':'bdb86a015c343b685f94682e5ae644f63fab7468','isolationLimit':'LOCK_SUCCESS_BLOCKS_LATER_WRITERS_BUT_DOES_NOT_REFRESH_AN_EARLIER_RR_OR_SERIALIZABLE_SNAPSHOT','native':'NOT_RUN'}
  dynamic_count+=1
 else:raise AssertionError((row['id'],row['hazard']))
 rows.append(out)
assert (tuple_count,aggregate_count,dynamic_count)==(164,15,8)
assert len(rows)==305 and sum(r['baselineCovered']for r in rows)==118
# Closed command START remains a refusal; no earlier unprotected START gains admission.
closed='supabase/repeatable/16102026_1205_weekly_contact_20_producer_completion/38_native_submission_readers.sqlinc'
source=read('inputs/actual/'+closed);match=re.search(r'(create or replace function public\.weekly_source_message_dispatch_submission_start_atomic_v1\([\s\S]*?\$function\$;)',source,re.I)
assert match,'closed START identity changed'
assert 'WEEKLY_SOURCE_PER_TARGET_DISPATCH_REQUIRED'in match[0]and "errcode='55000'"in match[0]
assert not re.search(r'\binsert\b|\bupdate\b|\bdelete\b',match[0],re.I)
review={'version':'CONTACT_OFFICE_FINITE_F23_SOURCE_REVIEW_V1','qualification':'FINITE_TYPES_AND_CLOSED_DDL_SOURCE_REVIEW_ONLY','sourceAssemblyCommit':census['sourceAssemblyCommit'],'lockGroupingSuccessorCommit':census['lockGroupingSuccessorCommit'],'counts':census['counts'],'rows':rows,'nativeTupleEvidence':tuple_evidence,'nativeScalarEvidence':scalar_evidence,'closedCommandStart':{'file':closed,'completeFunctionSha256':sha(match[0]),'retainedRefusal':'WEEKLY_SOURCE_PER_TARGET_DISPATCH_REQUIRED','effects':'NO_DML_NO_PERMIT_NO_PROVIDER_ATTEMPT'},'limitations':['NO_NATIVE_EXECUTION_OR_CONCURRENCY_OR_FULL_F23_CLAIM','NO_CARDINALITY_OR_BUSINESS_POLICY_APPROVAL','ALL_ORIGINAL_GENERIC_FENCES_AND_FINITE_MAPS_PRESERVED','NO_BANK_SCHEMA_OR_ABI_ASSUMPTION','RR_SERIALIZABLE_SNAPSHOT_CURRENTNESS_REMAINS_NATIVE_QUALIFICATION'],'sqlCorrections':[],'native':'NOT_RUN','databaseCalls':0,'pglast':pglast.__version__,'postgres':parser.get_postgresql_version()}
(here/'REVIEW.json').write_text(json.dumps(review,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'tuples':tuple_count,'aggregateOccurrences':aggregate_count,'dynamicOccurrences':dynamic_count,'nativeTupleObjects':len(tuple_evidence),'scalarInputs':len(scalar_evidence),'native':'NOT_RUN'}))
