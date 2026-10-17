"""Actual complete PG17 source AST, aliases and closed-helper call census."""
from pathlib import Path
import sys,json,hashlib,re
sys.dont_write_bytecode=True
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import ast
HERE=Path('\\\\?\\'+str(Path(__file__).resolve().parent))
ROOT=Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')
selection=json.loads((HERE/'PAYLOAD_SELECTION.json').read_bytes())
spec=json.loads((HERE/'EXPECTED_SECURITY_DELTA.json').read_bytes())['privateOwnerOnly']
expected={s['signature']:s for s in spec};grammar=[];functions=[];expressions=0;call_sites=[]
def walk(v):
 if isinstance(v,ast.Node):
  yield v
  for f in v:yield from walk(getattr(v,f))
 elif isinstance(v,(tuple,list)):
  for x in v:yield from walk(x)
def dictionaries(v):
 if isinstance(v,dict):
  yield v
  for x in v.values():yield from dictionaries(x)
 elif isinstance(v,list):
  for x in v:yield from dictionaries(x)
def type_name(t):
 n='.'.join(x.sval for x in t.names)
 return {'pg_catalog.bool':'boolean','pg_catalog.text':'text','pg_catalog.jsonb':'jsonb','pg_catalog.uuid':'uuid'}.get(n,n)
for row in selection['targets']:
 if not row['target'].endswith(('.sql','.sqlinc')):continue
 raw=(HERE/row['source']).read_bytes();source=raw.decode()
 # Only psql client directives are removed for grammar; no SQL/body/type substitutions.
 sql='\n'.join(line for line in source.splitlines() if not line.startswith('\\'))
 parsed=pglast.parse_sql(sql);programmes=pglast.parse_plpgsql(sql)
 grammar.append({'target':row['target'],'sha256':hashlib.sha256(raw).hexdigest(),'statements':len(parsed),'plpgsqlPrograms':len(programmes),'sqlSubstitutions':0})
 headers=[n.stmt for n in parsed if isinstance(n.stmt,ast.CreateFunctionStmt)]
 for header,wrapped in zip(headers,programmes) if headers else []:
  signature='.'.join(x.sval for x in header.funcname)+'('+','.join(type_name(p.argType) for p in header.parameters or ()) +')'
  assert signature in expected,signature
  e=expected[signature];assert type_name(header.returnType)=='jsonb',signature
  options={x.defname:x.arg for x in header.options}
  assert options['language'].sval=='plpgsql' and options['security'].boolval
  assert options['volatility'].sval=={'s':'stable','v':'volatile'}[e['volatility']]
  program=wrapped['PLpgSQL_function'];names=set()
  for datum in program['datums']:
   for value in datum.values():
    if isinstance(value,dict) and value.get('refname'):names.add(value['refname'])
  aliases=[];collisions=[];returns=0;calls=[]
  for d in dictionaries(program):
   if 'PLpgSQL_stmt_return' in d:returns+=1
   if 'PLpgSQL_expr' not in d:continue
   exp=d['PLpgSQL_expr'];query=exp['query'];mode=exp.get('parseMode',0)
   if mode==3:query=query.split(':=',1)[1]
   sql_ast=pglast.parse_sql(('select ' if mode else '')+query);expressions+=1
   for n in walk(sql_ast):
    if isinstance(n,(ast.RangeVar,ast.RangeFunction)) and n.alias:
     aliases.append(n.alias.aliasname)
     if n.alias.aliasname in names:collisions.append(n.alias.aliasname)
    if isinstance(n,ast.FuncCall):
     name='.'.join(x.sval for x in n.funcname)
     if name.startswith(('private.','public.','extensions.')):
      call={'owner':signature,'callee':name,'arity':len(n.args or ())};calls.append(call);call_sites.append(call)
  assert returns>0,signature
  assert not collisions,(signature,collisions)
  functions.append({'signature':signature,'returns':'jsonb','securityDefiner':True,'volatility':e['volatility'],'fullBodySqlExpressionsParsed':True,'declaredNames':sorted(names),'rangeAliases':sorted(set(aliases)),'aliasDeclarationCollisions':collisions,'returnStatements':returns,'callSites':calls})
assert len(functions)==len(expected)==9
assert set(x['signature'] for x in functions)==set(expected)
native=ROOT/'supabase/repeatable/21072026_1235_36_tsfin_load_context_batch.sql'
raw=native.read_bytes();declaration=pglast.parse_sql(raw.decode())[0].stmt
native_fields={p.name:type_name(p.argType) for p in declaration.parameters if p.mode.name=='FUNC_PARAM_TABLE'}
assert native_fields=={'effective_timesheet_id':'uuid','out_timesheet':'jsonb','out_cur_fin':'jsonb','out_candidate':'jsonb','out_umbrella':'jsonb','out_client_id':'uuid','out_effective_flags':'jsonb','out_policy':'jsonb'}
helper=next(r for r in selection['targets'] if r['target'].endswith('/04_daily_current_placement.sqlinc'))
program=pglast.parse_plpgsql((HERE/helper['source']).read_text())[0]['PLpgSQL_function'];strict_native=[];field_uses=[]
for d in dictionaries(program):
 if 'PLpgSQL_stmt_execsql' in d:
  q=d['PLpgSQL_stmt_execsql']
  if 'public.tsfin_load_context_batch' in q['sqlstmt']['PLpgSQL_expr']['query']:
   assert q['into'] and q['strict'];strict_native.append(q['sqlstmt']['PLpgSQL_expr']['query'])
 if 'PLpgSQL_expr' in d:
  x=d['PLpgSQL_expr'];q=x['query'];mode=x.get('parseMode',0)
  if mode==3:q=q.split(':=',1)[1]
  for n in walk(pglast.parse_sql(('select ' if mode else '')+q)):
   if isinstance(n,ast.ColumnRef) and len(n.fields)==2 and all(isinstance(x,ast.String) for x in n.fields):
    alias,column=[x.sval for x in n.fields]
    if alias in ['native_context','native_checked']:
     assert column in native_fields,(alias,column);field_uses.append((alias,column,native_fields[column]))
assert len(strict_native)==2
result={'version':'HEALTH_CLOSED9_ACTUAL_PG17_AST_V1','parserVersion':pglast.__version__,'grammar':grammar,'functions':functions,'sqlExpressions':expressions,'exactNativeReturnTable':{'source':'supabase/repeatable/21072026_1235_36_tsfin_load_context_batch.sql','sha256':hashlib.sha256(raw).hexdigest(),'fields':native_fields,'strictReadCount':2,'consumedFields':sorted(set(field_uses)),'nestedClosure':'EXISTING_UNCHANGED_NOT_NATIVE_QUALIFIED'},'qualifiedCallSites':call_sites,'native':'NOT_RUN','bodyTypeSubstitutions':0}
if '--verify' in sys.argv:assert json.loads((HERE/'GRAMMAR_ALIAS_RETURN_RESULTS.json').read_bytes())==json.loads(json.dumps(result))
else:(HERE/'GRAMMAR_ALIAS_RETURN_RESULTS.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({'grammarInputs':len(grammar),'privateBodies':len(functions),'sqlExpressions':expressions,'aliasConflicts':0,'strictNativeReads':2,'native':'NOT_RUN'}))
