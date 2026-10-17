"""PG17 parse, exact CTE substitution proof and finite negative mutations only."""
from pathlib import Path
import sys,json,copy,collections
sys.dont_write_bytecode=True
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
HERE=Path(__file__).resolve().parent
def walk(x):
 if isinstance(x,dict):
  yield x
  for v in x.values():yield from walk(v)
 elif isinstance(x,list):
  for v in x:yield from walk(v)
def names(a):return '.'.join(v['String']['sval']for v in a)
def strip(x):
 if isinstance(x,dict):return{k:strip(v)for k,v in x.items()if k not in ['location','stmt_len','stmt_location']}
 if isinstance(x,list):return[strip(v)for v in x]
 return x
def tree(file):return json.loads(pglast.parser.parse_sql_json((HERE/file).read_text(encoding='utf-8')))
def require(ok,code):
 if not ok:raise ValueError(code)
def prove(parent,after):
 require(len(parent['stmts'])==2 and len(after['stmts'])==2,'EXACT_SET_AND_SNAPSHOT_REQUIRED')
 require(strip(parent['stmts'][0])==strip(after['stmts'][0]),'SEARCH_PATH_CHANGED')
 p=copy.deepcopy(after);query=p['stmts'][1]['stmt']['SelectStmt'];ctes=query['withClause']['ctes'];matches=[(i,v['CommonTableExpr'])for i,v in enumerate(ctes)if v['CommonTableExpr']['ctename']=='background_statement_snapshot']
 require(len(matches)==1,'ONE_BOUND_SNAPSHOT_CTE_REQUIRED');index,cte=matches[0]
 require(cte.get('ctematerialized')=='CTEMaterializeAlways','ACTUAL_MATERIALIZED_REQUIRED')
 target=cte['ctequery']['SelectStmt'];require(len(target['targetList'])==1,'ONE_SNAPSHOT_VALUE_REQUIRED');t=target['targetList'][0]['ResTarget'];require(t['name']=='snapshot_value','EXACT_PRIVATE_ALIAS_REQUIRED')
 value=t['val'];require(strip(value)=={'FuncCall':{'funcname':[{'String':{'sval':'pg_current_snapshot'}}],'funcformat':'COERCE_EXPLICIT_CALL'}},'ONLY_ORIGINAL_SNAPSHOT_BUILTIN_REQUIRED')
 require(set(target)=={'targetList','limitOption','op'}and target['op']=='SETOP_NONE'and target['limitOption']=='LIMIT_OPTION_DEFAULT','ONE_ROW_NO_OTHER_EVALUATION_REQUIRED')
 del ctes[index];count=0
 def inline(x):
  nonlocal count
  if isinstance(x,dict):
   if 'SubLink'in x:
    s=x['SubLink'];q=s.get('subselect',{}).get('SelectStmt',{});r=q.get('fromClause',[])
    if len(r)==1 and r[0].get('RangeVar',{}).get('relname')=='background_statement_snapshot':
     require(s['subLinkType']=='EXPR_SUBLINK'and set(s)=={'subLinkType','subselect','location'},'EXACT_SCALAR_SUBQUERY_REQUIRED')
     require(set(q)=={'targetList','fromClause','limitOption','op'},'BOUND_VALUE_SUBQUERY_EXTRA_EFFECT_HELD')
     require(len(q['targetList'])==1 and names(q['targetList'][0]['ResTarget']['val']['ColumnRef']['fields'])=='snapshot_value','BOUND_REFERENCE_REQUIRED')
     require(set(r[0]['RangeVar'])=={'relname','inh','relpersistence','location'},'UNREVIEWED_BOUND_RELATION_SHAPE')
     count+=1;return copy.deepcopy(value)
   return{k:inline(v)for k,v in x.items()}
  if isinstance(x,list):return[inline(v)for v in x]
  return x
 result=inline(p);require(count==5,'EXACT_FIVE_ORIGINAL_CALL_SITES_REQUIRED');require(strip(result)==strip(parent),'COMPLETE_OUTPUT_AST_DIFFERENCE')
 return {'wholeOriginalAstEqualAfterExactScalarSubstitution':True,'boundCallSites':count,'allFieldsLiteralsOrderHashesRowsAndCallsExceptSnapshotEvaluationCountPreserved':True}
def census(t):
 calls=collections.Counter();relations=collections.Counter();ctes=[];jsonkeys=[];orders=0
 for n in walk(t):
  if 'FuncCall'in n:
   f=n['FuncCall'];name=names(f['funcname']);calls[name]+=1
   if name=='jsonb_build_object':
    args=f.get('args',[])
    for a in args[::2]:
     c=a.get('A_Const',{}).get('sval',{}).get('sval')
     if c is not None:jsonkeys.append(c)
  if 'RangeVar'in n:
   r=n['RangeVar'];relations[(r.get('schemaname','')+'.'+r['relname']).lstrip('.')]+=1
  if 'CommonTableExpr'in n:ctes.append(n['CommonTableExpr']['ctename'])
  if 'SortBy'in n:orders+=1
 return{'sourceCallOccurrences':dict(sorted(calls.items())),'relationAndCteReferenceOccurrences':dict(sorted(relations.items())),'cteNames':ctes,'jsonObjectLiteralKeys':jsonkeys,'orderedItems':orders}
def run():
 parent=tree('rendered/PARENT_CANONICAL.sql');after=tree('rendered/CANDIDATE_CANONICAL.sql');proof=prove(parent,after);bad=[]
 def reject(name,change):
  t=copy.deepcopy(after);change(t)
  try:prove(parent,t)
  except (ValueError,KeyError,TypeError,IndexError):bad.append(name);return
  raise AssertionError('NEGATIVE_MUTATION_ACCEPTED:'+name)
 def change_first(t,predicate,mutation):
  for n in walk(t):
   if predicate(n):mutation(n);return
  raise AssertionError('MUTATION_POINT_NOT_FOUND')
 reject('nonmaterialized snapshot could repeat work',lambda t:change_first(t,lambda n:n.get('ctename')=='background_statement_snapshot',lambda n:n.update(ctematerialized='CTEMaterializeDefault')))
 reject('row-dependent or other snapshot function',lambda t:change_first(t,lambda n:'FuncCall'in n and names(n['FuncCall']['funcname'])=='pg_current_snapshot',lambda n:n['FuncCall'].update(funcname=[{'String':{'sval':'clock_timestamp'}}])))
 reject('drop full background rows field',lambda t:change_first(t,lambda n:n.get('sval')=='backgroundRows',lambda n:n.update(sval='missingBackgroundRows')))
 reject('change full row fingerprint input',lambda t:change_first(t,lambda n:'FuncCall'in n and names(n['FuncCall']['funcname'])=='to_jsonb',lambda n:n['FuncCall'].update(funcname=[{'String':{'sval':'jsonb_build_object'}}])))
 reject('change volatile transaction-status observation',lambda t:change_first(t,lambda n:'FuncCall'in n and names(n['FuncCall']['funcname'])=='pg_xact_status',lambda n:n['FuncCall'].update(funcname=[{'String':{'sval':'pg_current_snapshot'}}])))
 reject('change actual clock observation',lambda t:change_first(t,lambda n:'FuncCall'in n and names(n['FuncCall']['funcname'])=='clock_timestamp',lambda n:n['FuncCall'].update(funcname=[{'String':{'sval':'transaction_timestamp'}}])))
 reject('change ordering',lambda t:change_first(t,lambda n:'SortBy'in n,lambda n:n['SortBy'].update(sortby_dir='SORTBY_DESC')))
 reject('change relation scope',lambda t:change_first(t,lambda n:'RangeVar'in n and n['RangeVar'].get('relname')=='ts_financials_outbox',lambda n:n['RangeVar'].update(relname='unreviewed_relation')))
 reject('change epoch refusal boundary',lambda t:change_first(t,lambda n:n.get('fval',{}).get('fval')=='4294967296',lambda n:n['fval'].update(fval='4294967297')))
 # Complete reader source order without assuming which native statement failed.
 before=tree('rendered/PARENT_BEFORE.sql');sequence=[next(iter(s['stmt']))for s in before['stmts']]
 return{'version':'CONTACT_SNAPSHOT_PG17_COMPLETE_AST_CSE_PROOF_V1','parser':pglast.__version__,'postgresql':list(pglast.get_postgresql_version()),**proof,'negativeMutationsRefused':bad,'parent':census(parent),'candidate':census(after),'beforeStatementKinds':sequence,'native':'NOT_RUN','measuredSpeedup':'NOT_MEASURED','timedOutStatement':'UNKNOWN','builtinBindingRequired':'Actual pg_catalog.pg_current_snapshot() OID5061/prosrc pg_current_snapshot/STABLE on PG17; root alone may obtain fixed metadata.'}
if __name__=='__main__':
 result=run();(HERE/'AST_PROOF.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8');print(json.dumps({'substitutions':result['boundCallSites'],'negativeMutations':len(result['negativeMutationsRefused']),'parentSnapshotCalls':result['parent']['sourceCallOccurrences'].get('pg_current_snapshot'),'candidateSnapshotCalls':result['candidate']['sourceCallOccurrences'].get('pg_current_snapshot'),'beforeStatementKinds':result['beforeStatementKinds'],'native':'NOT_RUN'}))
