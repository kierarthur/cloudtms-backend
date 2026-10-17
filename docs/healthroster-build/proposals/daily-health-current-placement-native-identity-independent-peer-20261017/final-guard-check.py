"""Independent actual final tuple-predicate evaluation; no SQL/native execution."""
from pathlib import Path
import sys,runpy,contextlib,io,copy,json
sys.dont_write_bytecode=True
BASE=Path(__file__).resolve().parent
if not str(BASE).startswith('\\\\?\\'):BASE=Path('\\\\?\\'+str(BASE))
with contextlib.redirect_stdout(io.StringIO()):data=runpy.run_path(str(BASE/'replay/placement-ast.py'))
from pglast import ast
import pglast
body=data['body'];evaluate=data['evaluate'];joins=data['joins'];fixture=data['fixture'];selected=data['selected'];U=data['U']
last=data['conditions'][-1]
assert isinstance(last,ast.BoolExpr) and last.boolop.name=='OR_EXPR'
negative=last.args[0]
assert isinstance(negative,ast.BoolExpr) and negative.boolop.name=='NOT_EXPR'
exists=negative.args[0]
assert isinstance(exists,ast.SubLink) and exists.subLinkType.name=='EXISTS_SUBLINK'
query=exists.subselect
def qualified(row):return joins(query.fromClause[0],row) and evaluate(query.whereClause,row) is True
def healthy():
    row=fixture();row['current_binding']=selected([row]);return row
assert qualified(healthy())
cases=[]
for owner,field,value in [('t','line_type','EXPENSE'),('t','is_adjustment',True),('t','parent_timesheet_id',U(999)),
 ('t','correction_id','OTHER'),('t','correction_kind','OTHER'),('t','adjustment_origin','OTHER'),('t','authorised_at_server',None),
 ('t','worked_start_iso',None),('t','worked_end_iso','2026-10-01T08:00:00Z'),('t','job_title_norm','HCA'),('t','band','6'),
 ('f','is_stale',True),('f','is_current',False),('w','current_revision_no',999),('w','current_revision_id',U(999)),
 ('native_checked','out_cur_fin',{'id':U(999)})]:
    row=healthy();row[owner][field]=value
    assert not qualified(row),(owner,field)
    cases.append({'case':owner+'.'+field,'actualFinalExists':'REFUSES_CONTROLLED_POST_READ_ROW'})
native_reads=[s['PLpgSQL_stmt_execsql'] for s in body if 'PLpgSQL_stmt_execsql' in s and 'public.tsfin_load_context_batch' in s['PLpgSQL_stmt_execsql']['sqlstmt']['PLpgSQL_expr']['query']]
assert len(native_reads)==2 and all(s['into'] and s['strict'] for s in native_reads)
for s in native_reads:
    query=s['sqlstmt']['PLpgSQL_expr']['query']
    assert 'array[o.from_timesheet_id]' in query and 'limit' not in query.lower()
helper=(BASE/'frozen/health/20_current_placement.sqlinc').read_text()
assert 'daily_rate_correction_context_v1(' not in helper and 'weekly_source_pay_actor_allowed_v1(' not in helper
result=(BASE/'frozen/templates/03_result.sql.tmpl').read_text();call='private.daily_rate_correction_health_current_placement_v1(o.id,p_actor_user_id)'
assert result.count(call)==2 and result.index(call)<result.index('@@BANK_RESULT@@')<result.rindex(call)
output={'status':'PASS','controlledFinalTupleCases':cases,'strictNativeReads':2,'originalF1':'CLOSED_AT_SOURCE_ONLY',
 'scope':'Actual final EXISTS and strict native-call source; native basis, nested resolver execution, full result/Bank joins and database races not executed.',
 'native':'NOT_RUN','BankingProducer':'UNPROVIDED'}
(BASE/'FINAL_GUARD_RESULTS.json').write_text(json.dumps(output,indent=2)+'\n')
print(json.dumps({'finalTupleCases':len(cases),'strictNativeReads':2,'native':'NOT_RUN'}))
