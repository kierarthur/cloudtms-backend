"""Offline PostgreSQL17 expression/DDL evidence. No native catalogue or SQL execution."""
from pathlib import Path
import json,sys,hashlib
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
here=Path(__file__).parent
data=json.loads((here/'CLASSIFICATION.json').read_text())
expression="v_audit.after_json->'result'||jsonb_build_object('idempotent_replay',true)"
ast=json.loads(pglast.parser.parse_sql_json('select '+expression))
value=ast['stmts'][0]['stmt']['SelectStmt']['targetList'][0]['ResTarget']['val']['A_Expr']
assert value['name'][0]['String']['sval']=='||'
assert value['lexpr']['A_Expr']['name'][0]['String']['sval']=='->'
assert value['rexpr']['FuncCall']['funcname'][0]['String']['sval']=='jsonb_build_object'
native=data['schema']['relations']['public.weekly_source_kept_as_paid_decisions']
ddl=json.loads(pglast.parser.parse_sql_json(native['ddl']))['stmts'][0]['stmt']['CreateStmt']
assert ddl['relation']['schemaname']=='public'
assert ddl['relation']['relname']=='weekly_source_kept_as_paid_decisions'
columns=[]
for item in ddl['tableElts']:
 if 'ColumnDef' in item:
  col=item['ColumnDef'];typ='.'.join(x['String']['sval'] for x in col['typeName']['names'])
  columns.append(dict(name=col['colname'],type=typ))
assert len(columns)==8
assert [x['name'] for x in columns]==[x['name'] for x in native['columns']]
assert [x['type'] for x in columns]==['uuid','uuid','text','text','uuid','bytea','uuid','timestamptz']
result=dict(parser='pglast '+pglast.__version__,PostgresGrammar='17.7',replayExpression=expression,
 replayParse='JSONB result extraction is the left operand of JSONB concatenation; boolean object is the right operand',
 replayAst=value,nativeDecisionColumns=columns,native='NOT_RUN',catalogue='NOT_RUN',databaseCalls=0)
(here/'AST_PROOF.json').write_text(json.dumps(result,indent=2)+'\n',newline='\n')
print(json.dumps(dict(parser=result['parser'],replayOperator='||',leftOperator='->',nativeColumns=8,native='NOT_RUN')))
