"""Finite Source13 source evidence; PG17 parsers only, no database connection."""
import hashlib,json,pathlib,re,subprocess,sys
HERE=pathlib.Path(__file__).resolve().parent
ROOT=pathlib.Path(sys.argv[1]); OWN=HERE.parents[3]
sys.path.insert(0,r'C:/tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import parser
sha=lambda x:hashlib.sha256(x if isinstance(x,bytes)else x.encode()).hexdigest()
raw=json.loads((HERE/'RAW_CENSUS.json').read_text(encoding='utf-8'))
counter=json.loads((HERE/'COUNTERPARTS.json').read_text(encoding='utf-8'))
frozen=lambda p,ref=raw['rootHeadAtCapture']:subprocess.check_output(['git','show',ref+':'+p],cwd=ROOT)
schema_path='supabase/release/current-contract.json';schema_bytes=frozen(schema_path)
schema=json.loads(schema_bytes);relations={r['schema']+'.'+r['name']:r for r in schema['relations']}
manifest_path='docs/healthroster-build/proposals/v6-source-managed-assembly-20261016/ASSEMBLY_MANIFEST.json'
manifest=json.loads(frozen(manifest_path,raw['sourceProposalCommit']))
functions=json.loads((OWN/'.codex-tmp/f23-managed-source13/FUNCTIONS.json').read_text(encoding='utf-8'))
bodies={f['number']:(OWN/f'.codex-tmp/f23-managed-source13/{f["number"]:03}.sqlinc').read_text(encoding='utf-8')for f in functions}
checks=[]
def check(name,condition):
 if not condition:raise AssertionError(name)
 checks.append(name)
def tree(sql):return json.loads(parser.parse_sql_json(sql))['stmts'][0]['stmt']
def walk(o,key):
 if isinstance(o,dict):
  if key in o:yield o[key]
  for v in o.values():yield from walk(v,key)
 elif isinstance(o,list):
  for v in o:yield from walk(v,key)
def excerpt(text,pattern):
 m=re.search(pattern,text,re.I|re.S);check('excerpt:'+pattern,m is not None)
 return {'text':m[0],'sha256':sha(m[0])}
inputs=[{'path':schema_path,'commit':raw['rootHeadAtCapture'],'sha256':sha(schema_bytes),'purpose':'Exact source contract snapshot, never a live database observation'}]
producer_pins=[];pl_trees={};pl_limits=[]
for f in functions:
 body=bodies[f['number']]
 if f['outer']:
  pl_trees[f['number']]=json.loads(parser.parse_plpgsql_json('do $source_review_outer$'+body+'$source_review_outer$;'))
  check('outer-body-'+str(f['number']),sha(body.rstrip('\n'))==f['bodySha256'])
  producer_pins.append({**f,'frozenSourceCommit':raw['sourceProposalCommit'],'authority':'Exact13 proposal output; full outer DO body, not merely one literal'})
 else:
  check('whole-function-'+str(f['number']),sha(body.strip())==f['completeStatementSha256'])
  check('complete-CREATE-grammar-'+str(f['number']),'CreateFunctionStmt'in tree(body))
  try:pl_trees[f['number']]=json.loads(parser.parse_plpgsql_json(body))
  except parser.ParseError as error:
   # libpg_query has no native relation descriptors for %ROWTYPE field writes.
   # Do not replace those variables with guessed record/scalar declarations.
   pl_trees[f['number']]=None
   pl_limits.append({'partId':f['number'],'error':str(error),'status':'OFFLINE_PLPGSQL_PARSE_UNAVAILABLE_WITHOUT_NATIVE_ROWTYPE_CATALOG','native':'NOT_RUN'})
  original=[x for x in manifest['functions']if x['signature']==f['signature']and x['completeStatementSha256']==f['completeStatementSha256']]
  check('frozen-full-producer-'+str(f['number']),len(original)==1)
  origin=original[0];source=frozen(origin['path'],origin['commit']).decode('utf-8')
  check('exact-original-complete-function-'+str(f['number']),body.strip()in source)
  producer_pins.append({**f,'frozenOrigin':origin,'originFileSha256':sha(source),'wholeOriginStatementEqual':True})
 if pl_trees[f['number']]is not None:check('plpgsql-grammar-'+str(f['number']),len(pl_trees[f['number']])==1)

# The four executable DDL shapes have eleven closed, Health-owned identifiers.
tables=re.search(r'foreach name in array array\[(.*?)\] loop',bodies[1],re.S)[1]
tables=re.findall(r"'([a-z_]+)'",tables)
check('RLS-eleven-unique-constant-identifiers',len(tables)==len(set(tables))==11)
created={}
for file in raw['files']:
 if not file['path'].startswith('supabase/migrations/'):continue
 sql=(ROOT/file['path']).read_text(encoding='utf-8')
 sql=re.sub(r'^\\(?:set|ir)\b.*$','',sql,flags=re.M)
 # Strip entry control lines only; actual Source DDL is embedded, not replaced.
 for stmt in json.loads(parser.parse_sql_json(sql))['stmts']:
  c=stmt['stmt'].get('CreateStmt')
  if c:created[c['relation']['schemaname']+'.'+c['relation']['relname']]=c
for name in tables:check('closed-RLS-created-table:'+name,'private.'+name in created)
formats=[
 'alter table private.%I enable row level security',
 'alter table private.%I force row level security',
 'revoke all on table private.%I from public,anon,authenticated,service_role',
 'create policy cloudtms_source_pay_decision_owner_only on private.%I for all to %I using(true) with check(true)',
]
for i,form in enumerate(formats):
 check('exact-format-'+str(i),bodies[1].count("format('"+form+"'")==1)
 for name in tables:
  sql=form.replace('%I',name,1).replace('%I','source_review_owner',1)
  parsed=tree(sql)
  check('closed-DDL-grammar-'+str(i)+'-'+name,len(parsed)==1)
check('RLS-no-extra-execution',len(list(walk(pl_trees[1],'PLpgSQL_stmt_dynexecute')))==4)
check('incident-no-execution',len(list(walk(pl_trees[2],'PLpgSQL_stmt_dynexecute')))==0)
check('incident-only-literal-comparison',"if actual_definition is distinct from\n $source_expected_predecessor$CHECK"in bodies[2])
check('incident-closed-refusal',bodies[2].count("raise exception 'SOURCE_INCIDENT_RESOLUTION_PREDECESSOR_MISMATCH'")==2)

rotation_path='supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql'
rotation_source=frozen(rotation_path).decode('utf-8');rotation=excerpt(rotation_source,r'CREATE OR REPLACE FUNCTION public\._pay_timesheet_rotation_scope\(.*?\$function\$;')
rotation_ast=tree(rotation['text'])['CreateFunctionStmt']
returns={x['FunctionParameter']['name']:x['FunctionParameter']['argType']['names'][-1]['String']['sval']for x in rotation_ast['parameters']if x['FunctionParameter'].get('mode')=='FUNC_PARAM_TABLE'}
check('rotation-exact-return-types',returns=={'requested_timesheet_id':'uuid','booking_id':'text','canonical_timesheet_id':'uuid','family_timesheet_id':'uuid','family_is_current':'bool','family_version':'int4','requested_is_canonical':'bool'})
inputs.append({'path':rotation_path,'commit':raw['rootHeadAtCapture'],'sha256':sha(rotation_source),'completeFunction':rotation,'declaredTableReturns':returns})

aggregate_types={
 'MAX(s.sequence)':('integer','public.weekly_exceptional_orchestration_steps.sequence','s is the sole native orchestration_steps alias, filtered by the exact run id; MAX allocates the next run sequence, never an identity.'),
 'MAX(event_sequence)':('bigint',None,'The statement has one native family_events or target_events relation, with event_sequence BIGINT; MAX allocates the next event counter. The accompanying ARRAY_AGG(event_hash ORDER BY event_sequence DESC)[1] remains the original BYTEA hash lookup.'),
 'MAX(sequence)':('integer','public.weekly_exceptional_orchestration_steps.sequence','The sole source is native orchestration_steps constrained by the run id.'),
 'MAX(step.sequence)':('integer','public.weekly_exceptional_orchestration_steps.sequence','step is native orchestration_steps in the exact INSERT SELECT, constrained by the same run id.'),
 'MAX(x.review_no)':('bigint','private.weekly_source_pay_decision_reviews.review_no','x is the Source-owned review table created by1100; the decision row is locked before its next review number is selected. This type proof does not qualify a missing Bank cut producer.'),
 'MAX(x.binding_no)':('bigint','private.weekly_source_pay_decision_bindings.binding_no','x is the Source-owned binding table created by1100, filtered by decision_id and member_key. No Banking binding field type is inferred.'),
 'MIN(resolved.booking_id)':('text',None,'resolved is exactly the declared rotation scope output: booking_id TEXT. COUNT DISTINCT booking_id=1 and nonempty booking checks precede success.'),
 'MIN(resolved.canonical_timesheet_id::text)':('text',None,'The sole argument is a declared UUID explicitly cast TEXT. The accompanying canonical_count and canonical_member_count both must equal1 before this identity is returned as successful.'),
 'MIN(resolved.family_version)':('integer',None,'resolved.family_version is the exact native rotation-scope INTEGER table return; null and distinct-version guards remain in the complete current consumer.'),
 'MIN(checked.failure_reason)':('text',None,'checked.failure_reason is the closed CASE of literal failure strings and SQL NULL. MIN selects a failure message; it never chooses a work/financial identity.'),
}
def expressions(s):
 ts=parser.scan(s);out=[]
 for i,t in enumerate(ts):
  word=s[t.start:t.end+1]
  if word.lower()not in('min','max')or i+1==len(ts)or s[ts[i+1].start:ts[i+1].end+1]!='(':continue
  start=ts[i+1].end+1;depth=1;j=i+2
  while depth:
   w=s[ts[j].start:ts[j].end+1];depth+=1 if w=='('else -1 if w==')'else 0;j+=1
  inp=s[start:ts[j-1].start]
  out.append({'expression':word.upper()+'('+re.sub(r'\s+',' ',inp)+')','input':inp,'inputSha256':sha(inp),'offset':t.start,'end':ts[j-1].end+1})
 return out
column_inputs={};relation_inputs={};review_rows=[]
def column(ref,typ):
 rel,name=ref.rsplit('.',1)
 if rel in relations:
  c=next(c for c in relations[rel]['columns']if c['name']==name)
  check('native-scalar:'+ref,c['type']==typ)
  column_inputs[ref]={'column':c,'evidence':'Exact current-contract snapshot','snapshotSha256':sha(schema_bytes)}
 else:
  c=next(c['ColumnDef']for c in created[rel]['tableElts']if c.get('ColumnDef',{}).get('colname')==name)
  check('source-DDL-scalar:'+ref,c['typeName']['names'][-1]['String']['sval']=={'bigint':'int8'}[typ])
  column_inputs[ref]={'columnAst':c,'evidence':'Exact Source1100 CREATE TABLE PG17 AST','type':typ}
for row in raw['rows']:
 row={**row};body=bodies[row['partId']];kind=row['hazard'].split(':')[0]
 if kind=='ROWTYPE_SELECT':
  _,var,rel=row['hazard'].split(':');rel=re.sub(r'\s+','',rel).lower()
  declaration=excerpt(body,r'\b'+re.escape(var)+r'\s+'+re.escape(rel)+r'\s*%rowtype\b')
  statement=row['statement'];scan_artifact=None
  if row['id']==38:
   scan_artifact={'explanation':'The generic scanner starts at the outer FOR SELECT c.* and stops at the inner SELECT INTO. These are two different PLpgSQL statements; no chunks tuple is assigned to v_operation.','outerTargetDeclaration':excerpt(body,r'v_chunk\s+public\.invoice_operation_chunks%rowtype')}
   statement=re.search(r'select \* into strict v_operation from public\.invoice_operations where id=v_chunk\.operation_id;',statement,re.I)[0]
   nodes=[x for x in walk(pl_trees[11],'PLpgSQL_stmt_execsql')if x.get('into')and x.get('target',{}).get('PLpgSQL_row',{}).get('fields')==[{'name':'v_operation','varno':15}]and 'id=v_chunk.operation_id'in x['sqlstmt']['PLpgSQL_expr']['query']]
   check('inner-actual-execsql-v_operation',len(nodes)==1 and nodes[0]['strict'])
   scan_artifact['innerPlpgsqlAst']=nodes[0]
  sql=re.sub(r'\binto\s+(?:strict\s+)?'+re.escape(var)+r'\b','',statement,count=1,flags=re.I)
  ast=tree(sql)['SelectStmt'];targets=ast['targetList'];sources=ast['fromClause']
  check('whole-one-relation-tuple:'+str(row['id']),len(targets)==1 and targets[0]['ResTarget']['val']['ColumnRef']['fields']==[{'A_Star':{}}]and len(sources)==1 and sources[0]['RangeVar']['schemaname']+'.'+sources[0]['RangeVar']['relname']==rel and not ast.get('withClause')and ast.get('op','SETOP_NONE')=='SETOP_NONE')
  r=relations[rel];keys=[c for c in r['constraints']if c['type']in('p','u')]
  relation_inputs[rel]={'primaryAndUniqueConstraints':keys,'indexes':r['indexes'],'relationObjectSha256':sha(json.dumps(r,sort_keys=True,separators=(',',':')))}
  if row['id']in(19,36):
   check('STRICT-nonsingleton-refusal:'+str(row['id']),bool(re.search(r'into strict',statement,re.I)))
   bound='No predicate-matching UNIQUE key is claimed. INTO STRICT raises on zero or multiple matches; it cannot silently pick a successful tuple. Existing scope and locks are retained.'
  elif row['id']==24:
   check('pending-release-no-fabricated-unique',not any('decision_bundle_id'in i['definition']and'UNIQUE'in i['definition']for i in r['indexes']))
   bound='This existing non-STRICT lookup has no declared unique key for result_json decision_bundle_id. Only its complete same-relation tuple type is classified. It yields the first matching row if multiplicity exists; actual producer single-receipt behavior still requires native acceptance proof.'
  elif row['id']==21:
   index=next(i for i in r['indexes']if i['name']=='uq_tsfin_current')
   check('current-financial-unique-index',index['definition']=='CREATE UNIQUE INDEX uq_tsfin_current ON public.timesheets_financials USING btree (timesheet_id) WHERE is_current')
   idx=tree(index['definition'])['IndexStmt'];check('current-financial-index-AST',idx['unique']and idx['relation']['schemaname']== 'public'and idx['relation']['relname']=='timesheets_financials')
   bound='Native partial UNIQUE uq_tsfin_current(timesheet_id) WHERE is_current bounds this exact current-row lookup to zero/one; INTO STRICT refuses absence. Index presence is source evidence, not installed-state proof.'
  elif row['id']==37:
   check('resolution-unique',any(c['definition']=='UNIQUE (upload_row_id, generation)'for c in keys))
   bound='Exact upload_row_id+generation native UNIQUE bounds zero/one whole tuple; original non-STRICT not-found behavior is retained.'
  else:
   col='publication_request_id'if rel=='private.weekly_source_local_protected_decision_receipts'else'timesheet_id'if rel=='public.timesheets'else'id'
   check('PK-point:'+str(row['id']),any(c['definition']=='PRIMARY KEY ('+col+')'for c in keys)and bool(re.search(r'\b'+col+r'\s*=',statement,re.I)))
   bound='Exact '+col+' PRIMARY KEY point predicate bounds zero/one complete tuple; original STRICT/non-STRICT and lock behavior remain unchanged.'
  proof={'verdict':'FINITE_SOURCE_DECLARED_TUPLE_TYPE_PROVED','declaredRelation':rel,'declaration':declaration,'actualSelect':statement,'actualSelectSha256':sha(statement),'pg17SqlAstSha256':sha(json.dumps(ast,sort_keys=True)),'cardinalityBound':bound,'scannerArtifact':scan_artifact,'approvalLimit':'Exact tuple assignment only; no row visibility, actor permission, producer effect or native concurrency qualification'}
 elif kind=='AGGREGATE_TYPE_UNCLASSIFIED':
  es=expressions(row['statement']);check('aggregate-present:'+str(row['id']),bool(es))
  for a in es:
   check('closed-aggregate-expression:'+a['expression'],a['expression']in aggregate_types)
   typ,ref,binding=aggregate_types[a['expression']]
   if a['expression']=='MAX(event_sequence)':ref='public.weekly_exceptional_pay_'+('family'if row['id']==8 else'target')+'_events.event_sequence'
   if ref:column(ref,typ)
   a.update(inputType=typ,nativeColumnRef=ref,bindingProof=binding)
  proof={'verdict':'FINITE_SOURCE_SCALAR_AGGREGATE_TYPES_PROVED','aggregates':es,'approvalLimit':'MIN/MAX input scalar types only; counters can still exhaust, and transaction/concurrency/economic correctness remains a native proof obligation. All Bank receipt/cut/return dependencies remain unqualified.'}
 elif kind=='UUID_CARDINALITY_UNCLASSIFIED':
  patterns=[r'canonical_count<>1',r'canonical_member_count<>1',r'current_count<>1',r'null_version_count>0',r'distinct_version_count<>\w+\.member_count']
  guards=[excerpt(row['statement'],p)for p in patterns]
  check('single-UUID-reduction:'+str(row['id']),len(re.findall(r'min\(resolved\.canonical_timesheet_id::text\)::uuid',row['statement'],re.I))==1)
  proof={'verdict':'FINITE_SOURCE_UUID_CARDINALITY_PROVED','actualUuidReduction':'MIN(resolved.canonical_timesheet_id::text)::uuid','producerInputType':'Declared rotation_scope canonical_timesheet_id UUID; explicit TEXT cast is the aggregate input, explicit UUID cast restores that same id.','guards':guards,'cardinalityProof':'Success requires one distinct non-NULL canonical UUID, exactly one canonical member, exactly one actual current member, and non-NULL unique versions. MIN therefore cannot choose among multiple canonical identities on the successful arm. Failed rows never serialize as successful families. Defensive unresolved rows from rotation_scope fail booking/version guards.','scopeLimit':'The existing booking advisory/row-lock ordering and pending H103 preflight/postwait revalidation are preserved by whole-function equality to e8cc. Their runtime behavior is NOT_RUN.'}
 else:
  if row['id']<=4:
   proof={'verdict':'FINITE_CLOSED_HEALTH_DDL_FORMAT_PROVED','closedIdentifiers':tables,'format':formats[row['id']-1],'identifierArguments':'Only loop variable from the exact eleven-element literal array; owner grantee is current_user, quoted with %I. No request/session text is interpolated.','ddlGrammarCases':11,'effectLimit':'Exact one-time Health table RLS enable/force, PUBLIC/browser/service revocation and owner-only policy creation. This is not a runtime dynamic-SQL waiver or Banking permission.'}
  else:
   proof={'verdict':'FINITE_INERT_CHECK_COMPARISON_PROVED','literalRole':'Dollar-quoted expected predecessor is a TEXT constant in IS DISTINCT FROM, never EXECUTE or a dynamic query.','guard':'Exact catalog relation, column, constraint kind/name/flags/attnum, full expected definition and closed absence/multiplicity refusal. Source13 migration already holds ACCESS EXCLUSIVE before this DO and preserves unchanged d863 addition.','effectLimit':'Classifies this literal only. It does not approve replacing a different predecessor CHECK or an unproved installed constraint.'}
 row['proof']=proof;row['counterpart']=counter['rows'][row['id']-1];review_rows.append(row)

# Source receipt producer correlation is recorded without inventing JSON uniqueness.
pending_writer_evidence=[excerpt(bodies[5],r'v_bundle_id:=pg_catalog\.gen_random_uuid\(\);'),excerpt(bodies[5],r'common_decision_bundle_id=v_bundle_id,common_bundle_revision=1'),excerpt(bodies[5],r'private\.weekly_source_entitlement_proposal_request_v2\(v_root\.timesheet_id,.*?\);'),excerpt(bodies[5],r'r\.member_root_ids=array\[v_receipt\.root_timesheet_id\]')]
counts={'rawDiagnostics':len(review_rows),'frozenBaselineCoveredOccurrences':sum(r['counterpart']['baselineCovered']for r in review_rows),'newFiniteOccurrences':sum(not r['counterpart']['baselineCovered']for r in review_rows),'newFiniteUniqueKeys':len({r['key']for r in review_rows if not r['counterpart']['baselineCovered']}),'dynamicOrLiteralStatements':sum(r['hazard']=='DYNAMIC_SQL_UNCLASSIFIED'for r in review_rows),'tupleStatements':sum(r['hazard'].startswith('ROWTYPE_SELECT:')for r in review_rows),'aggregateStatements':sum(r['hazard']=='AGGREGATE_TYPE_UNCLASSIFIED'for r in review_rows),'minMaxExpressions':sum(len(r['proof'].get('aggregates',[]))for r in review_rows),'uuidStatements':sum(r['hazard']=='UUID_CARDINALITY_UNCLASSIFIED'for r in review_rows),'completeBodies':len(producer_pins)}
check('expected-finite-counts',(counts['rawDiagnostics'],counts['frozenBaselineCoveredOccurrences'],counts['newFiniteOccurrences'],counts['newFiniteUniqueKeys'])==(38,12,26,23))
out={'version':'F23_MANAGED_SOURCE13_FINITE_REVIEW_V1','qualification':'SOURCE_TYPE_LITERAL_CARDINALITY_REVIEW_ONLY_NOT_GATE_OR_NATIVE_ACCEPTANCE','rawCensusSha256':sha((HERE/'RAW_CENSUS.json').read_bytes()),'counterpartsSha256':sha((HERE/'COUNTERPARTS.json').read_bytes()),'counts':counts,'sourceInputs':inputs,'completeProducerPins':producer_pins,'nativeScalarEvidence':column_inputs,'nativeTupleEvidence':relation_inputs,'pendingReleaseSingletonLimit':{'point':24,'status':'NOT_PROVED_BY_DECLARED_UNIQUE_KEY','boundedProducerPattern':pending_writer_evidence,'requiredNativeProof':'Genuine pending release must map the exact bundle/revision/root to one receipt, under real producer and concurrency paths. Existing type classification and frozen-baseline match are not that proof. No replacement SQL or new uniqueness assumption is introduced.'},'rows':review_rows,'parser':{'library':'pglast','version':pglast.__version__,'postgres':parser.get_postgresql_version(),'proof':'Offline syntax/AST only, no catalog/type execution','catalogLimits':pl_limits},'native':'NOT_RUN','bankProducerOrSchemaQualification':'NONE','genericFenceEdits':False,'requiredSqlCorrections':[]}
(HERE/'REVIEW.json').write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8')
(HERE/'CHECKS.json').write_text(json.dumps({'qualification':'OFFLINE_SOURCE_AND_PG17_GRAMMAR_ONLY','counts':{'passed':len(checks),'failed':0},'checks':checks,'native':'NOT_RUN'},indent=2)+'\n',encoding='utf-8')
print(json.dumps({'counts':counts,'checks':len(checks),'reviewSha256':sha((HERE/'REVIEW.json').read_bytes())},indent=2))
