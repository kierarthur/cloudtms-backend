"""Parse/inspect fixed generated SQL only; no connection, plan or SQL execution."""
from pathlib import Path
import sys,json,subprocess
sys.dont_write_bytecode=True
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
H=Path(__file__).resolve().parent
assert pglast.get_postgresql_version()[0]==17
code="import{buildCostInput,MODES}from './factory.mjs';import{identity}from './fixtures.mjs';console.log(JSON.stringify(MODES.map(m=>buildCostInput(identity,m))))"
built=json.loads(subprocess.check_output(['node','--input-type=module','-e',code],cwd=H))
def tree(s):return json.loads(pglast.parser.parse_sql_json(s))['stmts']
def walk(x):
 if isinstance(x,dict):
  for k,v in x.items():yield k,v;yield from walk(v)
 elif isinstance(x,list):
  for v in x:yield from walk(v)
programmes=0
for b in built:
 main=tree(b['input']['sql']);fresh=tree(b['input']['verification_sql'])
 assert len(main)==4 and len(fresh)==1
 assert list(main[0]['stmt'])==['VariableSetStmt']
 assert main[0]['stmt']['VariableSetStmt']['name']=='transaction_read_only'
 assert main[0]['stmt']['VariableSetStmt']['is_local']
 for s in main[1:]+fresh:assert list(s['stmt'])==['SelectStmt']
 aggregate=main[2]['stmt']['SelectStmt'];ctes=aggregate['withClause']['ctes'];assert len(ctes)==1
 c=ctes[0]['CommonTableExpr'];assert c['ctename']=='measured' and c['ctematerialized']=='CTEMaterializeAlways'
 measured=c['ctequery']['SelectStmt'];assert len(measured['fromClause'])==1
 sample=measured['fromClause'][0]['RangeSubselect']['subquery']['SelectStmt']
 assert sample['limitCount']['A_Const']['ival']['ival']==1000
 assert sample['fromClause']==[{'RangeVar':{'schemaname':'public','relname':'audit_events','inh':True,'relpersistence':'p','alias':{'aliasname':'a'},'location':sample['fromClause'][0]['RangeVar']['location']}}]
 assert not any(sample.get(k) for k in ['whereClause','sortClause','groupClause','havingClause','limitOffset','lockingClause'])
 calls=[v['funcname'] for k,v in walk(measured) if k=='FuncCall']
 for name in calls:assert name[0]['String']['sval']=='pg_catalog'
 assert len(aggregate['targetList'])==1 and aggregate['targetList'][0]['ResTarget']['name']=='cost_counters'
 assert not aggregate.get('intoClause') and not aggregate.get('groupClause') and not aggregate.get('havingClause')
 fields={v for k,v in walk(aggregate['targetList']) if k=='sval' and isinstance(v,str)}
 for required in ['sampleCount','evaluatedCount','bytesTotal','count','sum']:assert required in fields
 for s in main[1:]+fresh:
  for k,v in walk(s):
   if k=='RangeVar':assert v.get('schemaname')in('pg_catalog','private','public',None)
   assert k not in {'InsertStmt','UpdateStmt','DeleteStmt','CreateStmt','CreateTableAsStmt','DoStmt','CallStmt','CopyStmt'}
 programmes+=2
print(json.dumps({'status':'PG17_SOURCE_GRAMMAR_AST_PASS','modes':len(built),'programmes':programmes,'mainStatements':20,'freshStatements':5,'sampleLimit':1000,'native':'NOT_RUN'}))
