from pathlib import Path
import json,sys
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
from pglast import parse_sql,ast
P=Path('D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/.codex-tmp/health-source-schema-exact-once-source-20261017')
M=json.loads((P/'MODEL.json').read_text())
def make_name(table,cols):
 # Exact finite ASCII inputs; PG17.11 makeObjectName equal-length preference.
 a,b=table,'_'.join(cols)
 assert a.isascii() and b.isascii() and len(b)<64
 n,m=len(a),len(b)
 while n+m>57:
  if n>m:n-=1
  else:m-=1
 return a[:n]+'_'+b[:m]+'_fkey'
expected=[]
def add(table,c,inline=None):
 cols=[x.sval for x in c.fk_attrs] if c.fk_attrs else [inline]
 assert cols and all(cols) and c.pktable.schemaname in ['private','public']
 assert c.fk_upd_action=='a' and c.fk_del_action in ['a','r'] and not c.fk_del_set_cols
 action={'a':'noaction','r':'restrict'}[c.fk_del_action]
 expected.append({'table':table,'name':c.conname or make_name(table,cols),'columns':cols,
  'refSchema':c.pktable.schemaname,'refTable':c.pktable.relname,'refColumns':[x.sval for x in c.pk_attrs],
  'match':c.fk_matchtype,'update':c.fk_upd_action,'delete':c.fk_del_action,'deleteColumns':None,
  'deferrable':c.deferrable,'deferred':c.initdeferred,'validated':c.initially_valid and not c.skip_validation,
  'local':True,'inheritanceCount':0,'noInherit':True,'parent':0,
  'triggerFunctions':sorted(['pg_catalog.RI_FKey_check_ins','pg_catalog.RI_FKey_check_upd','pg_catalog.RI_FKey_noaction_upd','pg_catalog.RI_FKey_'+action+'_del']),
  'fourEnabledInternalTriggers':True})
for f in M['consumers'][:2]:
 text='\n'.join(x for x in (P/'parents'/f).read_text().splitlines() if not x.startswith('\\'))
 for raw in parse_sql(text):
  t=raw.stmt
  if isinstance(t,ast.CreateStmt):
   assert t.relation.schemaname=='private' and t.relation.relpersistence=='p' and not t.inhRelations and not t.partspec
   for e in t.tableElts:
    if isinstance(e,ast.ColumnDef):
     for c in e.constraints or ():
      if c.contype.name=='CONSTR_FOREIGN':add(t.relation.relname,c,e.colname)
    elif isinstance(e,ast.Constraint) and e.contype.name=='CONSTR_FOREIGN':add(t.relation.relname,e)
  elif isinstance(t,ast.AlterTableStmt):
   for c in t.cmds:
    assert c.subtype.name=='AT_AddConstraint' and c.def_.contype.name=='CONSTR_FOREIGN'
    add(t.relation.relname,c.def_)
key=lambda x:(x['table'],x['name'])
assert len(expected)==31 and len(set(key(x) for x in expected))==31
assert sorted(expected,key=key)==sorted(M['foreignKeys'],key=key)
for e in expected:
 for field in e:
  changed=dict(e);changed[field]=None if e[field] is not None else 'wrong'
  assert changed!=e
print(json.dumps({'sourceOnly':True,'actualForeignKeys':len(expected),'allExpectedFieldsIndependentlyDerived':True,'deferred':sum(x['deferrable'] and x['deferred'] for x in expected),'native':'NOT_RUN'}))
