"""Actual SQL-AST guard comparison only; no database/native reachability claim."""
from pathlib import Path
import runpy,copy,json,contextlib,io,sys
BASE=Path(__file__).resolve().parent
if not str(BASE).startswith('\\\\?\\'):BASE=Path('\\\\?\\'+str(BASE))
sys.dont_write_bytecode=True
with contextlib.redirect_stdout(io.StringIO()):
    original=runpy.run_path(str(BASE/'replay/placement-ast.py'))
fixture=original['fixture'];run=original['run'];U=original['U'];evaluate=original['evaluate']
import pglast
from pglast import ast
native=pglast.parse_plpgsql((BASE/'reference/0910_complete_context.sqlinc').read_text())[0]['PLpgSQL_function']['action']['PLpgSQL_stmt_block']['body']
queries=[x['PLpgSQL_stmt_if']['cond']['PLpgSQL_expr']['query'] for x in native if 'PLpgSQL_stmt_if' in x]
physical=next(q for q in queries if "t.line_type::text" in q)
physical_ast=pglast.parse_sql('select '+physical)[0].stmt.targetList[0].val
def walk(node):
    if isinstance(node,ast.Node):
        yield node
        for key in node:yield from walk(getattr(node,key))
    elif isinstance(node,(tuple,list)):
        for value in node:yield from walk(value)
def columns(node):
    return {'.'.join(x.sval for x in n.fields) for n in walk(node) if isinstance(n,ast.ColumnRef) and all(isinstance(x,ast.String) for x in n.fields)}
native_nodes=[n for q in queries for n in walk(pglast.parse_sql('select '+q)[0].stmt.targetList[0].val)]
def exact_comparison(required):
    found=[n for n in native_nodes if isinstance(n,ast.A_Expr) and n.kind.name=='AEXPR_DISTINCT' and columns(n)==set(required)]
    assert len(found)==1,required
    return found[0]
candidate=exact_comparison(['f.candidate_id','v_candidate_id'])
client=exact_comparison(['f.client_id','v_client_id'])
financial=exact_comparison(['f.id','native_context.out_cur_fin'])
effective=exact_comparison(['native_context.effective_timesheet_id','t.timesheet_id'])
authorised=next(n for n in native_nodes if isinstance(n,ast.NullTest) and columns(n)=={'t.authorised_at_server'} and n.nulltesttype.name=='IS_NULL')
base_eval=evaluate
def extended(n,c):
    if isinstance(n,ast.FuncCall) and n.funcname[-1].sval=='btrim':
        value=extended(n.args[0],c)
        return None if value is None else value.strip()
    if isinstance(n,ast.A_Expr) and n.kind.name=='AEXPR_NULLIF':
        left,right=extended(n.lexpr,c),extended(n.rexpr,c)
        return None if left==right else left
    if isinstance(n,ast.BooleanTest):
        assert n.booltesttype.name=='IS_TRUE'
        return extended(n.arg,c) is True
    return base_eval(n,c)
# Retain the actual evaluator for all other nodes, including its recursive calls.
base_eval.__globals__['evaluate']=extended
results=[]
def healthy():
    row=fixture();row['t'].update(line_type='HOURS',is_adjustment=False,parent_timesheet_id=None,correction_id=None,
        correction_kind=None,adjustment_origin=None,authorised_at_server='APPROVED')
    row.update(v_candidate_id=row['f']['candidate_id'],v_client_id=row['f']['client_id'],
        native_context={'effective_timesheet_id':row['t']['timesheet_id'],'out_cur_fin':{'id':row['f']['id']}})
    return row
for field,node,new_value in [('candidate',candidate,U(999)),('client',client,U(999)),('financial',financial,{'id':U(999)}),('effective',effective,U(999))]:
    row=healthy()
    if field=='candidate':row['v_candidate_id']=new_value
    if field=='client':row['v_client_id']=new_value
    if field=='financial':row['native_context']['out_cur_fin']=new_value
    if field=='effective':row['native_context']['effective_timesheet_id']=new_value
    assert extended(node,row) is True
    placement=run(row)
    results.append({'case':'canonical_'+field+'_disagrees','actual0910NativeGuard':'REFUSES','040bbSelectorAndFiveGuards':'ACCEPTS','placement':placement})
for field,value in [('line_type','EXPENSE'),('is_adjustment',True),('parent_timesheet_id',U(999)),('correction_id','different'),('correction_kind','OTHER'),('adjustment_origin','OTHER'),('authorised_at_server',None)]:
    row=healthy();row['t'][field]=value
    assert extended(authorised if field=='authorised_at_server' else physical_ast,row) is True
    placement=run(row)
    results.append({'case':'physical_'+field+'_not_admitted','actual0910NativeGuard':'REFUSES','040bbSelectorAndFiveGuards':'ACCEPTS','placement':placement})
source=(BASE/'frozen/health/20_current_placement.sqlinc').read_text()
for text in ['tsfin_load_context_batch','t.line_type','t.authorised_at_server','t.is_adjustment','t.parent_timesheet_id','t.correction_id','t.correction_kind','t.adjustment_origin']:
    assert text not in source,text
output={'status':'CONFIRMED_SOURCE_GUARD_GAP','cases':results,'caseCount':len(results),
    'scope':'Actual authored selector/five guards and actual0910 guard ASTs; final helper query has the same missing reference census. Existing native basis is not executed.',
    'native':'NOT_RUN','databaseReachabilityOrRace':'NOT_PROVED','BankingOwnerBodyReviewed':False}
(BASE/'PREDICATE_GAP_RESULTS.json').write_text(json.dumps(output,indent=2)+'\n')
print(json.dumps({'sourceCounterexamples':len(results),'native':'NOT_RUN','databaseRace':'NOT_PROVED'}))
