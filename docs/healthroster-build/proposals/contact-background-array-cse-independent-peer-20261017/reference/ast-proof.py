"""Complete PG17 source AST inverse and all-row array/hash lineage. No SQL run."""
from pathlib import Path
import sys,json,copy
sys.dont_write_bytecode=True
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
HERE=Path('\\\\?\\'+str(Path(__file__).resolve().parent))
def tree(s):return json.loads(pglast.parser.parse_sql_json(s))
def clean(x):
 if isinstance(x,dict):return{k:clean(v)for k,v in x.items()if k not in ['location','stmt_len','stmt_location']}
 if isinstance(x,list):return[clean(v)for v in x]
 return x
def walk(x):
 if isinstance(x,dict):
  yield x
  for v in x.values():yield from walk(v)
 elif isinstance(x,list):
  for v in x:yield from walk(v)
def select(s):return tree(s)['stmts'][0]['stmt']['SelectStmt']
def const(x):return x.get('A_Const',{}).get('sval',{}).get('sval')if isinstance(x,dict)else None
def strip(x):
 while 'TypeCast'in x:x=x['TypeCast']['arg']
 return x
def fields(x):
 x=strip(x)['FuncCall'];assert[n['String']['sval']for n in x['funcname']]==['jsonb_build_object'];args=x['args'];assert len(args)%2==0
 result={const(args[i]):args[i+1]for i in range(0,len(args),2)};assert None not in result and len(result)==len(args)//2;return result
def ctes(s):return{n['CommonTableExpr']['ctename']:n['CommonTableExpr']for n in s['withClause']['ctes']}
def outer(s):return strip(s['targetList'][0]['ResTarget']['val'])['A_Expr']['rexpr']
def leaves(s):
 if s.get('op')=='SETOP_UNION':
  assert s['all'];return leaves(s['larg'])+leaves(s['rarg'])
 return[s]
def vars(s):return s.replace(":'hr_contact_audit_bag'","'[]'").replace(":'hr_contact_other_bag'","'[]'")
def executable(s):
 assert[x for x in s.splitlines()if x.startswith('\\')]==['\\gset','\\gset']
 assert ';\n\\gset'not in s
 return vars('\n'.join(';'if x=='\\gset'else x for x in s.splitlines()))
def rename(x,before,after):
 x=copy.deepcopy(x)
 for d in walk(x):
  if 'ColumnRef'in d:
   fs=d['ColumnRef']['fields']
   if len(fs)==1 and fs[0].get('String',{}).get('sval')==before:fs[0]['String']['sval']=after
 return x
def prove(r):
 old=tree(r['originalInput'])['stmts'];new=tree(executable(r['input']))['stmts'];assert len(old)==6 and len(new)==8
 assert clean(new[:4]+new[7:])==clean(old[:4]+old[5:]),'ORIGINAL_PREFIX_ROLLBACK_CHANGED'
 original=old[4]['stmt']['SelectStmt'];final=new[6]['stmt']['SelectStmt'];oc=ctes(original);fc=ctes(final)
 assert list(fc)[:2]==['background_statement_snapshot','contact_background_maps']
 assert set(fc)==set(oc)|{'contact_background_maps'}
 assert clean(fc['background_statement_snapshot'])==clean(oc['background_statement_snapshot'])
 maps=fc['contact_background_maps'];assert maps['ctematerialized']=='CTEMaterializeAlways'
 assert clean(maps['ctequery'])==clean({'SelectStmt':select('select '+r['parts']['maps']+' as value')})
 of=fields(outer(original));assert clean(maps['ctequery']['SelectStmt']['targetList'][0]['ResTarget']['val'])==clean(of['backgroundRows'])
 # Exactly one copied full-map expression and one original snapshot in the final
 # statement. No map JSON enters psql variables or a SQL literal.
 assert r['finalQuery'].count(r['parts']['maps'])==1
 assert sum(1 for d in walk(final)if d.get('CommonTableExpr',{}).get('ctename')=='background_statement_snapshot')==1
 for name,node in oc.items():
  if name not in ['business','background_statement_snapshot']:assert clean(fc[name])==clean(node),'ORIGINAL_CTE_CHANGED:'+name
 # The final top-level statement has precisely two native rows in ordinal order.
 union=final['fromClause'][0]['RangeSubselect']['subquery']['SelectStmt'];assert union['op']=='SETOP_UNION'and union['all']
 left,right=union['larg'],union['rarg'];assert len(left['targetList'])==len(right['targetList'])==2
 assert left['targetList'][0]['ResTarget']['val']['A_Const']['ival']['ival']==1
 assert right['targetList'][0]['ResTarget']['val']['A_Const']['ival']['ival']==2
 base=left['fromClause'][0]['RangeSubselect']['subquery']['SelectStmt']
 # Full wrapper equality rules out LIMIT/DISTINCT/filter, alternate row order,
 # or an extra value projection around the complete original BASE target.
 anchor='\n select (identity_proof.jsonb_build_object';at=r['baseQuery'].index(anchor)
 expected_wrapper='with '+r['parts']['snapshot']+',\n contact_background_maps as materialized (select '+r['parts']['maps']+' as value),\n '+r['baseQuery'][5:at]+ '\n select contact_fragments.frame from (\n select 1 as ordinal, contact_base.frame from ('+r['baseQuery'][at+2:-1]+') as contact_base(frame)\n union all\n select 2 as ordinal, jsonb_build_object('+r['parts']['backgroundFieldsFromMaps']+')::text as frame\n ) as contact_fragments order by contact_fragments.ordinal;'
 assert clean(final)==clean(select(vars(expected_wrapper)))
 typed=select(vars(r['parts']['typedBusiness']));assert clean(fc['business']['ctequery'])==clean({'SelectStmt':typed})
 expected_base=copy.deepcopy(original)
 expected_base['withClause']['ctes']=[x for x in expected_base['withClause']['ctes']if x['CommonTableExpr']['ctename']!='background_statement_snapshot']
 for x in expected_base['withClause']['ctes']:
  if x['CommonTableExpr']['ctename']=='business':x['CommonTableExpr']['ctequery']={'SelectStmt':typed}
 bgkeys={'backgroundObservedAt','backgroundSnapshotStartedAt','backgroundSnapshotXmax','backgroundSnapshotEpochZero','backgroundSnapshotXip','backgroundRows'}
 args=outer(expected_base)['FuncCall']['args'];outer(expected_base)['FuncCall']['args']=[z for i in range(0,len(args),2)if const(args[i])not in bgkeys for z in args[i:i+2]]
 actual_base=copy.deepcopy(base);actual_base['withClause']=copy.deepcopy(final['withClause']);actual_base['withClause']['ctes']=actual_base['withClause']['ctes'][2:]
 assert clean(actual_base)==clean(expected_base),'BASE_WHOLE_AST_INVERSE_FAILED'
 assert clean(ctes(actual_base)['catalogue'])==clean(oc['catalogue'])
 bg=fields(right['targetList'][1]['ResTarget']['val']);assert set(bg)==bgkeys
 for k,v in bg.items():
  if k!='backgroundRows':assert clean(v)==clean(of[k]),'BACKGROUND_EXPRESSION_CHANGED:'+k
 assert clean(bg['backgroundRows'])==clean(select('select (select value from contact_background_maps)')['targetList'][0]['ResTarget']['val'])
 identity=fields(oc['identity_proof']['ctequery']['SelectStmt']['targetList'][0]['ResTarget']['val']);bf=fields(outer(base))
 assert set(identity)|set(bf)==set(r['descriptor']['baseKeys'])and len(identity)==7
 assert set(bf).isdisjoint(bg)and set(identity).isdisjoint(bg)and len(set(identity)|set(bf)|set(bg))==21
 assert r['descriptor']['snapshotKeys']==sorted(set(identity)|set(bf)|set(bg),key=lambda x:(len(x.encode()),x.encode()))
 # Both original bag-stage branch sets are copied raw: audit1+other9. The
 # remaining two relation records come from every complete map-array entry.
 original_leaves=leaves(oc['business']['ctequery']['SelectStmt']);assert len(original_leaves)==12
 parsed_parts=[select(x)for x in r['parts']['branches']];assert clean(parsed_parts)==clean(original_leaves)
 names=[const(x['targetList'][0]['ResTarget']['val'])for x in original_leaves]
 bg_names=['private.candidate_daily_batch_receipts','public.ts_financials_outbox'];assert r['parts']['backgroundRelations']==bg_names
 for i,chosen in enumerate([[x for x in original_leaves if const(x['targetList'][0]['ResTarget']['val'])=='public.audit_events'],[x for x in original_leaves if const(x['targetList'][0]['ResTarget']['val'])not in bg_names+['public.audit_events']]]):
  actual=new[4+i]['stmt']['SelectStmt'];cs=ctes(actual);assert clean(leaves(cs['retained_business']['ctequery']['SelectStmt']))==clean(chosen)
  assert len(chosen)==[1,9][i]
  assert clean(actual)==clean(select(r['bagQueries'][i]+';'))
  expected_bag='with retained_business as ('+'\nunion all\n'.join([r['parts']['audit']]if i==0 else r['parts']['other'])+"), fixed_bag as (select jsonb_agg(jsonb_build_object('relation',relation,'row_count',row_count,'row_fingerprint',row_fingerprint) order by relation)::text value from retained_business) select value as "+['hr_contact_audit_bag','hr_contact_other_bag'][i]+' from fixed_bag where octet_length(value)<=16384;'
  assert clean(actual)==clean(select(expected_bag))
 typed_leaves=leaves(typed);assert len(typed_leaves)==3
 expected_typed="select retained.relation,retained.row_count,retained.row_fingerprint from jsonb_to_recordset(:'hr_contact_audit_bag'::jsonb || :'hr_contact_other_bag'::jsonb) as retained(relation text,row_count bigint,row_fingerprint text)"
 assert r['parts']['typedBags']==expected_typed and clean(typed_leaves[0])==clean(select(vars(expected_typed)))
 maps_fields=fields(of['backgroundRows']);assert list(maps_fields)==bg_names
 entry=select("select bg_entry.value->>'rowHash'")['targetList'][0]['ResTarget']['val']
 for i,name in enumerate(bg_names):
  native=original_leaves[names.index(name)];derived=typed_leaves[i+1];expected=copy.deepcopy(native)
  fingerprint=expected['targetList'][2]['ResTarget']['val']
  for d in walk(fingerprint):
   if d.get('ColumnRef',{}).get('fields')==[{'String':{'sval':'row_hash'}}]:d.clear();d.update(copy.deepcopy(entry))
  expected['fromClause']=select("select 1 from jsonb_array_elements((select value->'"+name+"' from contact_background_maps)) as bg_entry(value)")['fromClause']
  assert clean(derived)==clean(expected),'ORIGINAL_COUNT_HASH_LINEAGE_CHANGED:'+name
  # Original full-array aggregation contains exactly one naked heap range;
  # jsonb_agg has no DISTINCT/filter/skip and preserves duplicate equal hashes.
  q=maps_fields[name]['SubLink']['subselect']['SelectStmt'];assert len(q['fromClause'])==1 and 'RangeVar'in q['fromClause'][0]
  assert not any(k in d for d in walk(q)for k in ['whereClause','distinctClause','limitCount','limitOffset','agg_filter'])
  aggs=[d['FuncCall']for d in walk(q)if d.get('FuncCall',{}).get('funcname')==[{'String':{'sval':'jsonb_agg'}}]];assert len(aggs)==1 and not aggs[0].get('agg_distinct')
  obj=fields(aggs[0]['args'][0]);hashed=native['fromClause'][0]['RangeSubselect']['subquery']['SelectStmt']
  row_hash=hashed['targetList'][0]['ResTarget']['val'];assert clean(rename(row_hash,'t','r'))==clean(obj['rowHash'])
  nr=hashed['fromClause'][0]['RangeVar'];mr=q['fromClause'][0]['RangeVar'];assert(nr['schemaname'],nr['relname'])==(mr['schemaname'],mr['relname'])
 # Literal full inverse of the logical BASE recovers the original canonical.
 back=r['baseQuery']
 for h in reversed(r['inverses']):
  if h['after']=='':
   assert back.count(anchor)==1;back=back.replace(anchor,h['before']+anchor,1)
  else:assert back.count(h['after'])==1;back=back.replace(h['after'],h['before'],1)
 assert back==r['wholeQuery']
 guard=old[1];pglast.parser.parse_plpgsql_json(r['originalInput'][guard['stmt_location']:guard['stmt_location']+guard['stmt_len']])
 return{'statements':8,'originalPrefixRollbackAstEqual':True,'wholeBaseInverse':True,'catalogueTextLedgerIdentityUnchanged':True,'bagRecords':[1,9],'backgroundArrayRecords':2,'allOriginalTwelveRelationRecords':True,'originalCompleteMapExpressions':True,'twoFullRowHashLineagesAlphaExact':True,'nativeCountStarAndOriginalSortedHashExpression':True,'materializedFullArraysOnce':True,'singleSnapshotOriginalStatusClocks':True,'finalFramesOrdinalOrder':[1,2],'raw21MemberJoin':'UNCHANGED','native':'NOT_RUN'}
proof=[]
for case in ['CONTACT_CONSTRUCTORS_V1','QT_ADMIN_CONTROLS_V1']:
 r=json.loads((HERE/'rendered'/(case+'.json')).read_bytes());proof.append({'case':case,**prove(r)})
r=json.loads((HERE/'rendered/CONTACT_CONSTRUCTORS_V1.json').read_bytes())
mutations={
 'audit skips row':('offset 0) hashed_rows','offset 1) hashed_rows'),
 'audit deduplicates':('from (select encode(pg_catalog.sha256','from (select distinct encode(pg_catalog.sha256'),
 'bag count retyped':('row_count bigint','row_count double precision'),
 'bag control removed':('octet_length(value)<=16384','true'),
 'map materialization removed':('contact_background_maps as materialized','contact_background_maps as not materialized'),
 'map excludes row':('from public.ts_financials_outbox r','from public.ts_financials_outbox r where false'),
 'map deduplicates hashes':('jsonb_agg(jsonb_build_object(\'keyHash\'','jsonb_agg(distinct jsonb_build_object(\'keyHash\''),
 'array row count deduplicates':("select 'public.ts_financials_outbox' relation,count(*) row_count","select 'public.ts_financials_outbox' relation,count(distinct bg_entry.value->>'rowHash') row_count"),
 'array count bounded int4':("select 'public.ts_financials_outbox' relation,count(*) row_count","select 'public.ts_financials_outbox' relation,count(*)::int row_count"),
 'array hash delimiter':("string_agg(bg_entry.value->>'rowHash','|'","string_agg(bg_entry.value->>'rowHash',','"),
 'array hash order':("order by bg_entry.value->>'rowHash'","order by bg_entry.value->>'rowHash' desc"),
 'array hash keyHash substitution':("string_agg(bg_entry.value->>'rowHash'","string_agg(bg_entry.value->>'keyHash'"),
 'array skips one':("as bg_entry(value)","as bg_entry(value) offset 1"),
 'snapshot refreshed shape':('select pg_current_snapshot() snapshot_value','select pg_current_snapshot() snapshot_value offset 0'),
 'actual clock replaced':("'backgroundObservedAt',clock_timestamp()","'backgroundObservedAt',transaction_timestamp()"),
 'frames reversed':('order by contact_fragments.ordinal','order by contact_fragments.ordinal desc'),
 'RR removed':('begin isolation level repeatable read read only','begin isolation level read committed read only'),
 'rollback removed':('rollback;','commit;'),
 'unquoted variable':(":'hr_contact_audit_bag'",':hr_contact_audit_bag'),
 'gset repeated execution':('\n\\gset',';\n\\gset'),
 'catalogue hash changed':("'catalogueFingerprint',encode(pg_catalog.sha256","'catalogueFingerprint',encode(pg_catalog.sha512"),
}
for label,(before,after)in mutations.items():
 m=copy.deepcopy(r);assert before in m['input'],label;m['input']=m['input'].replace(before,after,1)
 try:prove(m)
 except(AssertionError,pglast.parser.ParseError,KeyError):pass
 else:raise AssertionError('MUTATION_ADMITTED:'+label)
print(json.dumps({'parser':pglast.__version__,'postgresql':list(pglast.get_postgresql_version()),'cases':proof,'counterexamples':list(mutations),'queriesExecuted':0,'native':'NOT_RUN'},indent=2))
