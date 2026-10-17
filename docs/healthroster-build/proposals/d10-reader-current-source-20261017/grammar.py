"""PG17 source/typed lineage proof; catalogue-dependent PL bodies remain NOT_PROVED."""
from pathlib import Path
import hashlib,json,re,sys
sys.dont_write_bytecode=True
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import ast
HERE=Path('\\\\?\\'+str(Path(__file__).resolve().parent))
def read(p):return (HERE/p).read_bytes()
def doc(p):return json.loads(read(p))
def sha(b):return hashlib.sha256(b).hexdigest()
def walk(x):
 if isinstance(x,ast.Node):
  yield x
  for k in x:yield from walk(getattr(x,k))
 elif isinstance(x,(list,tuple)):
  for v in x:yield from walk(v)
def dicts(x):
 if isinstance(x,dict):
  yield x
  for v in x.values():yield from dicts(v)
 elif isinstance(x,list):
  for v in x:yield from dicts(v)
def typename(t):
 s='.'.join(x.sval for x in t.names)
 return {'pg_catalog.bool':'boolean','pg_catalog.int4':'integer','pg_catalog.int8':'bigint','pg_catalog.timestamptz':'timestamp with time zone','timestamptz':'timestamp with time zone','timestamp':'timestamp without time zone','pg_catalog.timestamp':'timestamp without time zone'}.get(s,s.replace('pg_catalog.',''))
def clean(s):return '\n'.join(x for x in s.splitlines() if not x.startswith('\\'))
contract=doc('parents/supabase/release/current-contract.json')
relations={x['schema']+'.'+x['name']:{c['name']:c['type'] for c in x['columns']} for x in contract['relations']}
ddl_provenance=[]
for f in ['supabase/migrations/13102026_1127_healthroster_timesheet_correction_requests_v1.sql','supabase/migrations/13102026_1133_weekly_source_rule_sets_schema.sql']:
 raw=read('parents/'+f)
 for r in pglast.parse_sql(clean(raw.decode())):
  if isinstance(r.stmt,ast.CreateStmt):
   name=r.stmt.relation.schemaname+'.'+r.stmt.relation.relname
   if name not in ['private.timesheet_correction_requests','public.weekly_source_group_rule_assignments']:continue
   assert name not in relations
   relations[name]={c.colname:typename(c.typeName) for c in r.stmt.tableElts if isinstance(c,ast.ColumnDef)}
   ddl_provenance.append({'relation':name,'source':f,'sha256':sha(raw),'columns':relations[name]})
selection=doc('PAYLOAD_SELECTION.json');assembly=doc('ASSEMBLY.json')
rows=[];calls=[];exprs=0;field_checks=[];returns=[];programs={};source_by_signature={}
for target in selection['targets']:
 if not re.search(r'\.sql(?:inc)?$',target['target']):continue
 raw=read(target['source']);s=clean(raw.decode());parsed=pglast.parse_sql(s)
 record={'target':target['target'],'sha256':sha(raw),'sqlStatements':len(parsed),'sqlGrammar':'PASS','typeSubstitutions':0}
 try:pl=pglast.parse_plpgsql(s);record['originalPL']='SOURCE_PARSE_PASS_NOT_NATIVE'
 except Exception as e:
  record['originalPL']='NOT_PROVED';record['catalogueDiagnostic']=str(e)
  assert any(x in str(e) for x in ['"f.id" is not a known variable','"link.membership_id" is not a known variable']),str(e)
  # Only for extracting exact original SQL expressions/alias checks: split the
  # catalogue-dependent INTO field targets into schema-proved uuid scratch slots.
  surrogate=re.sub(r'(into\s+v_count,)\s*([fwh])\.id',lambda m:m[1]+' parser_'+m[2]+'_id',s)
  surrogate=surrogate.replace('into links,link.membership_id','into links,parser_link_id')
  surrogate=surrogate.replace('declare ','declare parser_f_id uuid; parser_w_id uuid; parser_h_id uuid; parser_link_id uuid; ',1)
  pl=pglast.parse_plpgsql(surrogate);record['surrogatePurpose']='EXPRESSION_AND_ALIAS_SOURCE_ONLY_NOT_BODY_OR_NATIVE_PROOF'
  record['scratchTargets']=['f.id','w.id','h.id'] if '"f.id"' in str(e) else ['link.membership_id']
  record['typeSubstitutions']=len(record['scratchTargets'])
 headers=[r.stmt for r in parsed if isinstance(r.stmt,ast.CreateFunctionStmt)]
 if len(headers)==1 and target['target'] in assembly['ownerPages'].values():
  header=headers[0];sig=next(k for k,v in assembly['ownerPages'].items() if v==target['target'])
  assert '.'.join(x.sval for x in header.funcname)==sig.split('(')[0]
  assert typename(header.returnType)=='jsonb'
  options={x.defname:x.arg for x in header.options}
  assert options['language'].sval=='plpgsql' and options['security'].boolval
  assert options['volatility'].sval==('immutable' if 'issue_filter_keys' in sig else 'stable')
  source_by_signature[sig]=s
  rowvars=dict(re.findall(r'\b([a-z_][a-z_0-9]*)\s+((?:public|private)\.[a-z_0-9]+)%rowtype',s,re.I))
  for variable,relation in rowvars.items():assert relation in relations,(sig,variable,relation)
  p=pl[0]['PLpgSQL_function'];programs[sig]=p
  names={v.get('refname') for d in p['datums'] for v in d.values() if isinstance(v,dict)}
  aliases=set();n=0
  for d in dicts(p):
   if 'PLpgSQL_stmt_return' in d:n+=1
   if 'PLpgSQL_expr' not in d:continue
   x=d['PLpgSQL_expr'];q=x['query'];mode=x.get('parseMode',0)
   if mode==3:q=q.split(':=',1)[1]
   parsed_q=pglast.parse_sql(('select ' if mode else '')+q);exprs+=1
   for node in walk(parsed_q):
    assert not isinstance(node,(ast.InsertStmt,ast.UpdateStmt,ast.DeleteStmt,ast.MergeStmt,ast.CallStmt)),(sig,q)
    if isinstance(node,ast.SelectStmt):assert not node.lockingClause,(sig,q)
    if isinstance(node,(ast.RangeVar,ast.RangeFunction)) and node.alias:
     alias=node.alias.aliasname;aliases.add(alias);assert alias not in names,(sig,'PL_RECORD_RANGE_ALIAS_CONFLICT',alias)
    if isinstance(node,ast.ColumnRef) and len(node.fields)==2 and all(isinstance(f,ast.String) for f in node.fields):
     a,c=[f.sval for f in node.fields]
     if a in rowvars:
      assert c in relations[rowvars[a]],(sig,a,c,rowvars[a])
      field_checks.append({'signature':sig,'variable':a,'relation':rowvars[a],'column':c,'type':relations[rowvars[a]][c]})
    if isinstance(node,ast.FuncCall):
     name='.'.join(v.sval for v in node.funcname)
     if name.startswith(('private.','public.','extensions.')):calls.append({'owner':sig,'callee':name,'arity':len(node.args or ())})
  assert n>0;returns.append({'signature':sig,'returns':n,'rowVariables':rowvars,'rangeAliases':sorted(aliases),'aliasConflicts':0})
  assert not any(k in d for d in dicts(p) for k in ['PLpgSQL_stmt_dynexecute','PLpgSQL_stmt_perform_dml','PLpgSQL_stmt_commit','PLpgSQL_stmt_rollback'])
 rows.append(record)
assert len(programs)==7
# Actual F23 rowtype statements: one direct relation, SELECT * and matching declaration.
hazards=doc('RAW_F23_CENSUS.json');proof=[]
for row in hazards:
 for h in row['hazards']:
  sig,kind,statement=json.loads(h['key'])
  if kind.startswith('ROWTYPE_SELECT:'):
   variable=kind.split(':')[1].lower();relation=kind.split(':',2)[2].replace(' ','').lower()
   declared=dict(re.findall(r'\b([a-z_][a-z_0-9]*)\s+((?:public|private)\.[a-z_0-9]+)%rowtype',source_by_signature[sig],re.I))
   assert declared[variable]==relation and relation in relations
   ast_q=pglast.parse_sql(statement)[0].stmt
   assert isinstance(ast_q,ast.SelectStmt) and len(ast_q.fromClause)==1 and isinstance(ast_q.fromClause[0],ast.RangeVar)
   rel=ast_q.fromClause[0];assert rel.schemaname+'.'+rel.relname==relation
   assert len(ast_q.targetList)==1 and isinstance(ast_q.targetList[0].val,ast.ColumnRef) and isinstance(ast_q.targetList[0].val.fields[0],ast.A_Star)
   assert not ast_q.distinctClause and not ast_q.groupClause and ast_q.op.name=='SETOP_NONE'
   evidence={'classification':'EXACT_SINGLE_RELATION_ROWTYPE','variable':variable,'relation':relation,'schemaColumns':relations[relation],'statementAstSha256':sha(str(ast_q).encode())}
  elif sig.startswith('outer:'):
   assert row['target'].endswith('/15092026_1534_weekly_source_acl_contract_v1.sql')
   before=read('parents/'+row['target']).decode();assert statement in before
   evidence={'classification':'UNCHANGED_CURRENT_MANDATORY_STATEMENT_FULL_PARENT_TRANSFER','completeParentSha256':sha(before.encode()),'privateLiteralInsertionOnly':True}
  elif kind=='AGGREGATE_TYPE_UNCLASSIFIED' and 'query_rows' in sig:
   s=source_by_signature[sig]
   for exact in ["coalesce(jsonb_agg(value order by worker_sort,actual_work_date,item_sort),'[]'::jsonb)","count(*)>v_limit","select * from rows union all select * from source_information"]:
    assert exact in s,exact
   evidence={'classification':'EXACT_TYPED_CTE_OUTPUTS','all_rows':['worker_sort text COLLATE C','actual_work_date date','item_sort text COLLATE C','value jsonb'],'aggregateOutputs':['version.hash text from encode(bytea,hex)','jsonb_agg(value) jsonb','count(*) bigint compared with v_limit integer -> boolean'],'into':['v_revision text','v_rows jsonb','v_more boolean'],'unionAllMultiplicityPreserved':True,'fullStatementSha256':sha(statement.encode())}
  elif kind=='AGGREGATE_TYPE_UNCLASSIFIED' and 'reminder_context' in sig:
   assert relations['public.candidate_notifications']['created_at_utc']=='timestamp with time zone'
   assert 'last_reminder timestamptz' in source_by_signature[sig]
   evidence={'classification':'MAX_NATIVE_TIMESTAMPTZ_TO_SAME_TYPE','relation':'public.candidate_notifications','column':'created_at_utc','sourceType':'timestamp with time zone','target':'last_reminder timestamp with time zone','emptyOrAllNull':'SQL NULL retained; genuine cooldown expression unchanged'}
  elif kind=='JSON_CONCAT':
   assert 'query_groups' in sig
   if statement=="rows:=rows||page->'rows';":
    assert "rows jsonb:='[]'::jsonb" in source_by_signature[sig]
    assert "'rows',v_rows" in source_by_signature['private.weekly_source_check_only_query_rows_v1(uuid,uuid,jsonb)']
    evidence={'classification':'JSONB_ARRAY_APPEND','left':'declared jsonb rows initialized []','right':'page.rows from exact included query_rows return of declared jsonb v_rows','producerSuccessGuard':"page->'ok' is distinct from 'true'::jsonb -> raise"}
   else:
    assert statement=="if reminder_action is not null then group_row:=jsonb_set(group_row,'{actions}',group_row->'actions'||jsonb_build_array(reminder_action),false);"
    assert "'first_seen_at_utc',first_seen,'children',children,'actions',jsonb_build_array(review_action)" in source_by_signature[sig]
    evidence={'classification':'JSONB_ARRAY_APPEND','left':'group_row.actions exact jsonb_build_array(review_action)','right':'jsonb_build_array(reminder_action)','guard':'reminder_action IS NOT NULL; group constructor precedes append'}
  else:raise AssertionError((sig,kind))
  proof.append({'target':row['target'],'key':h['key'],'hazard':kind,'evidence':evidence})
result={'version':'HEALTH_D10_PG17_SOURCE_TYPES_V1','parser':pglast.__version__,'grammar':rows,'privateHelpers':7,'originalPLSourceParses':sum(r.get('originalPL')=='SOURCE_PARSE_PASS_NOT_NATIVE' for r in rows),'originalPLNotProved':[r for r in rows if r.get('originalPL')=='NOT_PROVED'],'fullExpressionsParsed':exprs,'fieldChecks':sorted({json.dumps(x,sort_keys=True) for x in field_checks}),'functions':returns,'qualifiedDirectCalls':calls,'sourceSchema':{'sealedContractSha256':sha(read('parents/supabase/release/current-contract.json')),'ddlAdditions':ddl_provenance,'schemaChangesInProposal':0},'f23SourceProof':proof,'native':'NOT_RUN','wholeBodyCatalogueCompilation':'NOT_PROVED_FOR_TWO_ORIGINAL_BODIES','runtimeEqualityOrRoles':'NOT_RUN'}
if '--verify' in sys.argv:assert doc('GRAMMAR_TYPE_RESULTS.json')==result
else:(HERE/'GRAMMAR_TYPE_RESULTS.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'sqlInputs':len(rows),'helpers':7,'sourcePLPass':sum(x.get('originalPL')=='SOURCE_PARSE_PASS_NOT_NATIVE' for x in rows),'originalCatalogueBodiesNotProved':len(result['originalPLNotProved']),'expressions':exprs,'typedRecordFields':len(result['fieldChecks']),'f23Keys':len(proof),'native':'NOT_RUN'}))
