"""Exact existing branch/prefix AST proofs. PostgreSQL parser only, never execution."""
from pathlib import Path
import sys,json,copy
sys.dont_write_bytecode=True
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
HERE=Path(__file__).resolve().parent
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
def value(s):return select('select '+s)['targetList'][0]['ResTarget']['val']
def const(x):return x.get('A_Const',{}).get('sval',{}).get('sval')if isinstance(x,dict)else None
def fields(expr):
 while 'TypeCast'in expr:expr=expr['TypeCast']['arg']
 f=expr['FuncCall'];assert [p['String']['sval']for p in f['funcname']]==['jsonb_build_object'];args=f['args'];assert len(args)%2==0
 out={const(args[i]):args[i+1]for i in range(0,len(args),2)};assert None not in out;assert len(out)==len(args)//2;return out
def ctes(s):return{n['CommonTableExpr']['ctename']:n['CommonTableExpr']for n in s['withClause']['ctes']}
def prove(r,mode,raw=None):
 old=tree(r['originalInput'])['stmts'];raw=raw or r['inputs'][mode];new=tree('\n'.join(x for x in raw.splitlines()if not x.startswith('\\echo ')))['stmts'];assert len(old)==len(new)==6
 assert clean(new[:4]+new[5:])==clean(old[:4]+old[5:]),'ORIGINAL_PREFIX_OR_ROLLBACK_CHANGED'
 q=new[4]['stmt']['SelectStmt'];assert len(q['targetList'])==1;assert not any(k in q for k in ['fromClause','whereClause','havingClause','groupClause','distinctClause','limitCount','limitOffset','lockingClause','intoClause']);f=fields(q['targetList'][0]['ResTarget']['val']);assert set(f)=={'version','component','counts','digest'};assert const(f['version'])=='CONTACT_CANONICAL_COMPONENT_V1' and const(f['component'])==mode
 canonical=tree(r['canonicalSql'])['stmts'][1]['stmt']['SelectStmt'];original_ctes=ctes(canonical);current=ctes(q)
 if mode=='AUDIT':
  assert set(current)=={'component_audit'}
  expected=select(r['parts']['audit']);assert clean(current['component_audit']['ctequery'])==clean({'SelectStmt':expected})
  leaves=[n for n in walk(original_ctes['business']['ctequery'])if n.get('targetList')and const(n['targetList'][0]['ResTarget']['val'])=='public.audit_events'];assert len(leaves)==1;assert clean(leaves[0])==clean(expected)
  physical=[n['RangeVar']for n in walk(expected)if'RangeVar'in n];assert len(physical)==1 and(physical[0]['schemaname'],physical[0]['relname'])==('public','audit_events')
  assert clean(f['counts'])==clean(value('(select jsonb_build_array(row_count::text) from component_audit)'));assert clean(f['digest'])==clean(value('(select row_fingerprint from component_audit)'))
 elif mode=='CATALOGUE':
  assert set(current)=={'catalogue'};assert clean(current['catalogue'])==clean(original_ctes['catalogue'])
  original_fields=[fields(n)for n in walk({'SelectStmt':canonical})if'FuncCall'in n and any(const(a)=='catalogueFingerprint'for a in n['FuncCall'].get('args',[]))];assert len(original_fields)==1
  assert clean(f['digest'])==clean(original_fields[0]['catalogueFingerprint'])
  assert clean(f['counts'])==clean(value('jsonb_build_array((select count(*)::text from catalogue))'))
 else:
  assert set(current)=={'background_statement_snapshot','component_background'};assert clean(current['background_statement_snapshot'])==clean(original_ctes['background_statement_snapshot'])
  component=current['component_background'];assert component['ctematerialized']=='CTEMaterializeAlways';sub=component['ctequery']['SelectStmt'];assert clean(sub)==clean(select('select '+r['parts']['background']+' value'));assert len(sub['targetList'])==1 and sub['targetList'][0]['ResTarget']['name']=='value';maps=sub['targetList'][0]['ResTarget']['val'];assert clean(maps)==clean(value(r['parts']['background']))
  original_fields=[fields(n)for n in walk({'SelectStmt':canonical})if'FuncCall'in n and any(const(a)=='backgroundRows'for a in n['FuncCall'].get('args',[]))];assert len(original_fields)==1;assert clean(maps)==clean(original_fields[0]['backgroundRows'])
  mapping=fields(maps);assert set(mapping)=={'private.candidate_daily_batch_receipts','public.ts_financials_outbox'}
  for expr in mapping.values():
   selects=[n['SelectStmt']for n in walk(expr)if'SelectStmt'in n and any('RangeVar'in x and x['RangeVar'].get('schemaname')in['public','private']for x in n['SelectStmt'].get('fromClause',[]))];assert len(selects)==1 and 'whereClause'not in selects[0]
  assert clean(f['counts'])==clean(value("(select jsonb_build_array(jsonb_array_length(value->'private.candidate_daily_batch_receipts')::text,jsonb_array_length(value->'public.ts_financials_outbox')::text) from component_background)"));assert clean(f['digest'])==clean(value("encode(pg_catalog.sha256(pg_catalog.convert_to((select value from component_background)::text,'UTF8')),'hex')"))
 return {'component':mode,'exactExistingBranchAst':True,'exactOriginalFourStatementPrefixAndRollback':True,'closedFourFieldResult':True,'statements':6,'fullRestorationVerdict':False}
proof=[]
for case in ['CONTACT_CONSTRUCTORS_V1','QT_ADMIN_CONTROLS_V1']:
 r=json.loads((HERE/'rendered'/(case+'.json')).read_text(encoding='utf-8'))
 for mode in ['AUDIT','CATALOGUE','BACKGROUND']:proof.append({'case':case,**prove(r,mode)})
 old=tree(r['originalInput'])['stmts'];guard=old[1];start=guard['stmt_location'];pglast.parser.parse_plpgsql_json(r['originalInput'][start:start+guard['stmt_len']])
r=json.loads((HERE/'rendered/CONTACT_CONSTRUCTORS_V1.json').read_text(encoding='utf-8'));a=r['inputs']['AUDIT'];b=r['inputs']['BACKGROUND'];c=r['inputs']['CATALOGUE']
# Inspect existing overlap only; no CSE/source rewrite is authorised here.
def alpha(expr,alias):
 x=copy.deepcopy(clean(expr))
 for n in walk(x):
  if 'ColumnRef'in n:
   f=n['ColumnRef']['fields']
   if f and f[0].get('String',{}).get('sval')==alias:f[0]['String']['sval']='same_physical_row'
 return x
canonical=tree(r['canonicalSql'])['stmts'][1]['stmt']['SelectStmt'];business=ctes(canonical)['business']['ctequery'];maps=fields(value(r['parts']['background']));overlap=[]
for relation,expr in maps.items():
 branches=[n for n in walk(business)if n.get('targetList')and const(n['targetList'][0]['ResTarget']['val'])==relation];assert len(branches)==1
 inner=[n['RangeSubselect']['subquery']['SelectStmt']for n in walk(branches[0])if'RangeSubselect'in n and n['RangeSubselect'].get('alias',{}).get('aliasname')=='hashed_rows'];assert len(inner)==1
 business_hash=inner[0]['targetList'][0]['ResTarget']['val'];store=inner[0]['fromClause'][0]['RangeVar'];assert store['schemaname']+'.'+store['relname']==relation
 rows=[n for n in walk(expr)if n.get('targetList')and any('RangeVar'in f and f['RangeVar'].get('schemaname')in['public','private']for f in n.get('fromClause',[]))];assert len(rows)==1
 store2=rows[0]['fromClause'][0]['RangeVar'];assert store2['schemaname']+'.'+store2['relname']==relation
 objects=[fields(n)for n in walk(expr)if'FuncCall'in n and any(const(v)=='rowHash'for v in n['FuncCall'].get('args',[]))];assert len(objects)==1;assert alpha(business_hash,'t')==alpha(objects[0]['rowHash'],'r')
 overlap.append({'relation':relation,'existingBusinessAndMapFullRowHashAstAlphaEqual':True,'completeRowsNoFilter':True,'sameStatementSnapshotRequired':True,'potentialReuseOnly':'DERIVE_COMPLETE_BUSINESS_COUNT_AND_SORTED_HASH_BAG_FROM_SAME_COMPLETE_MAP','changeAuthored':False,'nativeCostOrEvaluationCountProved':False})
mutations={
 'audit skips row':('AUDIT',a.replace('offset 0) hashed_rows','offset 1) hashed_rows',1)),
 'audit filters population':('AUDIT',a.replace('offset 0) hashed_rows','where false offset 0) hashed_rows',1)),
 'audit loses duplicates':('AUDIT',a.replace('from (select encode(pg_catalog.sha256','from (select distinct encode(pg_catalog.sha256',1)),
 'audit changes delimiter':('AUDIT',a.replace("string_agg(row_hash,'|'","string_agg(row_hash,','",1)),
 'audit changes order':('AUDIT',a.replace('order by row_hash','order by row_hash desc',1)),
 'audit fabricates count':('AUDIT',a.replace('jsonb_build_array(row_count::text)',"jsonb_build_array('1'::text)",1)),
 'audit fabricates digest':('AUDIT',a.replace('(select row_fingerprint from component_audit)',"repeat('0',64)",1)),
 'background omits map':('BACKGROUND',b.replace("'public.ts_financials_outbox',(","'wrong_map',(",1)),
 'background filters outbox':('BACKGROUND',b.replace('from public.ts_financials_outbox r','from public.ts_financials_outbox r where false',1)),
 'background weakens row hash':('BACKGROUND',b.replace("'rowHash',encode","'changedHash',encode",1)),
 'catalogue changes exact hash':('CATALOGUE',c.replace('pg_catalog.sha256','pg_catalog.sha512',1)),
 'guard isolation changed':('AUDIT',a.replace('begin isolation level repeatable read read only','begin isolation level read committed read only',1)),
 'rollback changed':('AUDIT',a.replace('rollback;','commit;',1)),
 'raw result field added':('BACKGROUND',b.replace("'version','CONTACT_CANONICAL_COMPONENT_V1'","'rawMap',(select value from component_background),'version','CONTACT_CANONICAL_COMPONENT_V1'",1)),
}
for name,(mode,raw)in mutations.items():
 assert raw!=r['inputs'][mode],name
 try:prove(r,mode,raw)
 except(AssertionError,pglast.parser.ParseError):pass
 else:raise AssertionError('MUTATION_ADMITTED:'+name)
print(json.dumps({'parser':pglast.__version__,'postgresql':list(pglast.get_postgresql_version()),'cases':proof,'actualGuardBodiesParsed':2,'counterexamples':list(mutations),'existingHashOverlap':overlap,'native':'NOT_RUN','queriesExecuted':0},indent=2))
