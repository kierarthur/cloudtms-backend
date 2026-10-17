"""PG17 parser/whole-stage equality proof; no database or SQL execution."""
from pathlib import Path
import sys,json,copy
sys.dont_write_bytecode=True
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
HERE=Path(__file__).resolve().parent
def read(p):
 p=str(p.resolve())
 if len(p)>=260:p='\\\\?\\'+p
 return Path(p).read_text(encoding='utf-8')
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
 x=strip(x)['FuncCall'];assert[p['String']['sval']for p in x['funcname']]==['jsonb_build_object'];args=x['args'];assert len(args)%2==0
 out={const(args[i]):args[i+1]for i in range(0,len(args),2)};assert None not in out and len(out)==len(args)//2;return out
def ctes(s):return{n['CommonTableExpr']['ctename']:n['CommonTableExpr']for n in s['withClause']['ctes']}
def outer(s):return strip(s['targetList'][0]['ResTarget']['val'])['A_Expr']['rexpr']
def vars(s):return s.replace(":'hr_contact_audit_bag'","'[]'").replace(":'hr_contact_other_bag'","'[]'")
def executable(s):
 assert[s for s in s.splitlines()if s.startswith('\\')]==['\\gset','\\gset']
 assert ';\n\\gset'not in s
 return vars('\n'.join(';'if x=='\\gset'else x for x in s.splitlines()))
def prove(r):
 old=tree(r['originalInput'])['stmts'];new=tree(executable(r['input']))['stmts'];assert len(old)==6 and len(new)==9
 assert clean(new[:4]+new[8:])==clean(old[:4]+old[5:]),'ORIGINAL_PREFIX_ROLLBACK_CHANGED'
 original=old[4]['stmt']['SelectStmt'];base=new[6]['stmt']['SelectStmt'];bg=new[7]['stmt']['SelectStmt'];oc=ctes(original);bc=ctes(base)
 assert set(bc)==set(oc)-{'background_statement_snapshot'}
 typed=select(vars(r['parts']['typedBusiness']))
 assert clean(bc['business']['ctequery'])==clean({'SelectStmt':typed})
 assert len(typed['fromClause'])==1 and 'RangeFunction'in typed['fromClause'][0]
 typed_ranges=typed['fromClause'][0]['RangeFunction'];assert typed_ranges['alias']['aliasname']=='retained'
 # Complete BASE AST inverse: business is the only replacement relation. All
 # catalogue TEXT/exporter, ledger, identity, hash, ordering, census and activity
 # nodes remain exactly the original AST, excluding the six BG fields/CTE.
 expected=copy.deepcopy(original)
 expected['withClause']['ctes']=[x for x in expected['withClause']['ctes']if x['CommonTableExpr']['ctename']!='background_statement_snapshot']
 for x in expected['withClause']['ctes']:
  if x['CommonTableExpr']['ctename']=='business':x['CommonTableExpr']['ctequery']={'SelectStmt':typed}
 bgkeys=set(r['descriptor']['backgroundKeys']);args=outer(expected)['FuncCall']['args'];outer(expected)['FuncCall']['args']=[x for i in range(0,len(args),2)if const(args[i])not in bgkeys for x in args[i:i+2]]
 assert clean(base)==clean(expected),'BASE_WHOLE_AST_INVERSE_FAILED'
 # Catalogue expression is a TEXT column; neither it nor its wrapper hash is
 # round-tripped through psql/jsonb. Its full original CTE/raw AST remains.
 assert clean(bc['catalogue'])==clean(oc['catalogue'])
 of=fields(outer(original));bf=fields(outer(base));identity=fields(ctes(original)['identity_proof']['ctequery']['SelectStmt']['targetList'][0]['ResTarget']['val'])
 assert set(bf)==set(of)-bgkeys
 assert set(identity)|set(bf)==set(r['descriptor']['baseKeys']) and len(identity)==7
 assert len(set(identity)|set(of))==21 and set(identity).isdisjoint(of)
 assert clean(bg)==clean(select('with '+r['parts']['snapshot']+'\nselect jsonb_build_object('+r['parts']['bgFields']+')::text;'))
 assert set(ctes(bg))=={'background_statement_snapshot'} and clean(ctes(bg)['background_statement_snapshot'])==clean(oc['background_statement_snapshot'])
 gf=fields(bg['targetList'][0]['ResTarget']['val']);assert set(gf)==bgkeys and len(gf)==6
 for k,v in gf.items():assert clean(v)==clean(of[k]),'BACKGROUND_EXPRESSION_CHANGED:'+k
 assert set(bf).isdisjoint(gf) and set(identity).isdisjoint(gf)
 # Exact twelve original all-row branch ASTs, bagged 1+11 without any WHERE,
 # DISTINCT, LIMIT or change to row hash, sorted string_agg, duplicate handling.
 original_leaves=[select(x)for x in r['parts']['branches']]
 assert len(original_leaves)==12
 audit_indices=[i for i,x in enumerate(original_leaves)if const(x['targetList'][0]['ResTarget']['val'])=='public.audit_events'];assert len(audit_indices)==1
 for i,(leaves,name)in enumerate([([r['parts']['audit']],'hr_contact_audit_bag'),(r['parts']['other'],'hr_contact_other_bag')]):
  actual=new[4+i]['stmt']['SelectStmt'];cs=ctes(actual);assert set(cs)=={'retained_business','fixed_bag'}
  assert clean(cs['retained_business']['ctequery'])==clean({'SelectStmt':select('\nunion all\n'.join(leaves))})
  expected_bag="with retained_business as ("+'\nunion all\n'.join(leaves)+"), fixed_bag as (select jsonb_agg(jsonb_build_object('relation',relation,'row_count',row_count,'row_fingerprint',row_fingerprint) order by relation)::text value from retained_business) select value as "+name+" from fixed_bag where octet_length(value)<=16384;"
  assert clean(actual)==clean(select(expected_bag))
  assert clean(actual)==clean(select(r['bagQueries'][i]+';'))
  selected=[audit_indices[0]]if i==0 else[j for j in range(12)if j!=audit_indices[0]]
  assert[clean(select(x))for x in leaves]==[clean(original_leaves[j])for j in selected]
 for branch in original_leaves:
  scopes=[n['RangeVar']for n in walk(branch)if'RangeVar'in n];assert len(scopes)==1
  assert not any(k in n for n in walk(branch)for k in ['whereClause','distinctClause','limitCount'])
  fence=[n['SelectStmt']['limitOffset']for n in walk(branch)if'SelectStmt'in n and'limitOffset'in n['SelectStmt']];assert len(fence)==1 and fence[0]['A_Const']['ival'].get('ival',0)==0
 # The native recordset casts preserve original PG count bigint and text/hash
 # types. Both arrays retain all records and SQL owns ordering/hash arithmetic.
 assert r['parts']['typedBusiness']=="select retained.relation,retained.row_count,retained.row_fingerprint from jsonb_to_recordset(:'hr_contact_audit_bag'::jsonb || :'hr_contact_other_bag'::jsonb) as retained(relation text,row_count bigint,row_fingerprint text)"
 names=[x['ColumnDef']['colname']for x in typed_ranges['coldeflist']];assert names==['relation','row_count','row_fingerprint']
 types=[x['ColumnDef']['typeName']['names'][-1]['String']['sval']for x in typed_ranges['coldeflist']];assert types==['text','int8','text']
 assert r['descriptor']['snapshotKeys']==sorted(set(identity)|set(of),key=lambda x:(len(x.encode('utf8')),x.encode('utf8')))
 # Full raw inverse, including exact CR/LF/source. The deleted fixed CTE has
 # one predetermined anchor; no global normalization or blind empty splice.
 back=r['baseQuery']
 for h in reversed(r['inverses']):
  if h['after']=='':
   anchor='\n select (identity_proof.jsonb_build_object';assert back.count(anchor)==1;back=back.replace(anchor,h['before']+anchor,1)
  else:assert back.count(h['after'])==1;back=back.replace(h['after'],h['before'],1)
 assert back==r['wholeQuery']
 guard=old[1];pglast.parser.parse_plpgsql_json(r['originalInput'][guard['stmt_location']:guard['stmt_location']+guard['stmt_len']])
 return{'statements':9,'originalPrefixRollbackAstEqual':True,'allTwelveBranchesAstEqual':True,'bags':[1,11],'bagControlBytes':16384,'typedBagRoundtrip':['text','int8','text'],'baseWholeAstInverse':True,'catalogueTextAndHashUnchanged':True,'backgroundSixFieldsAndSnapshotAstEqual':True,'rawCanonicalInverse':True,'partition':[15,6],'snapshot':'ONE_ORIGINAL_READ_ONLY_RR_CHILD','native':'NOT_RUN'}
proof=[]
for case in ['CONTACT_CONSTRUCTORS_V1','QT_ADMIN_CONTROLS_V1']:
 r=json.loads(read(HERE/'rendered'/(case+'.json')));proof.append({'case':case,**prove(r)})
r=json.loads(read(HERE/'rendered/CONTACT_CONSTRUCTORS_V1.json'))
mutations={
 'audit filters rows':('offset 0) hashed_rows','where false offset 0) hashed_rows'),
 'audit skips row':('offset 0) hashed_rows','offset 1) hashed_rows'),
 'loses duplicate row hash':('from (select encode(pg_catalog.sha256','from (select distinct encode(pg_catalog.sha256'),
 'hash delimiter':("string_agg(row_hash,'|'","string_agg(row_hash,','"),
 'hash order':('order by row_hash','order by row_hash desc'),
 'bag count retyped':('row_count bigint','row_count double precision'),
 'bag bound removed':('octet_length(value)<=16384','true'),
 'gset reexecutes prior query':('\n\\gset',';\n\\gset'),
 'gset unknown mode':('\\gset','\\gexec'),
 'bag unquoted interpolation':(":'hr_contact_audit_bag'",':hr_contact_audit_bag'),
 'catalogue hash changes':("'catalogueFingerprint',encode(pg_catalog.sha256","'catalogueFingerprint',encode(pg_catalog.sha512"),
 'background map filter':('from public.ts_financials_outbox r','from public.ts_financials_outbox r where false'),
 'background false clock':("'backgroundObservedAt',clock_timestamp()","'backgroundObservedAt',transaction_timestamp()"),
 'background snapshot refreshed':('select pg_current_snapshot() snapshot_value','select pg_current_snapshot() snapshot_value offset 0'),
 'isolation changed':('begin isolation level repeatable read read only','begin isolation level read committed read only'),
 'rollback changed':('rollback;','commit;'),
 'raw map extra BASE member':("'otherActivityCount',","'unreviewed',42,'otherActivityCount',"),
}
for name,(before,after)in mutations.items():
 m=copy.deepcopy(r);assert before in m['input'],name;m['input']=m['input'].replace(before,after,1)
 try:prove(m)
 except(AssertionError,pglast.parser.ParseError,KeyError):pass
 else:raise AssertionError('MUTATION_ADMITTED:'+name)
print(json.dumps({'parser':pglast.__version__,'postgresql':list(pglast.get_postgresql_version()),'cases':proof,'counterexamples':list(mutations),'actualGuardBodiesParsed':2,'native':'NOT_RUN','queriesExecuted':0},indent=2))
