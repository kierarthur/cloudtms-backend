"""Execute the exact 018 PL/pgSQL assignment AST over closed in-memory inputs.
This proves assignment order/output only, not native SQL, row types, origins,
prices, permissions or actual invoice insertion. Catalogue/native are NOT_RUN.
"""
from pathlib import Path
import sys,os,json,re,hashlib
from decimal import Decimal
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
here=Path(__file__).parent
def raw(p): return open(('\\\\?\\'+str(p.absolute())) if os.name=='nt' else p,'rb').read()
target='supabase/repeatable/17102026_0231_weekly_source_paid_same_separate_health/018_public_weekly_source_invoice_admit_atomic_v1.sqlinc'
def expr(x): return x['PLpgSQL_expr']['query']
def tree(source):
    # Exact grammar-only surrogate for existing catalogue-dependent row types;
    # no surrogate is a payload, compilation proof or database statement.
    source=re.sub(r'\b([a-z_][a-z0-9_]*)\s+(?:public|private)[.][a-z_][a-z0-9_]*%rowtype',r'\1 record',source,flags=re.I)
    return pglast.parse_plpgsql(source)[0]['PLpgSQL_function']
def find_list(node):
    if isinstance(node,list):
        if any(isinstance(x,dict) and 'PLpgSQL_stmt_execsql' in x and 'sum((movement.canonical_pay_vector_json' in expr(x['PLpgSQL_stmt_execsql']['sqlstmt']) for x in node):return node
        for x in node:
            found=find_list(x)
            if found is not None:return found
    elif isinstance(node,dict):
        for x in node.values():
            found=find_list(x)
            if found is not None:return found
    return None
names=['v_hours_'+x for x in ('day','night','sat','sun','bh')]
tracked=set(names+['v_total_pay']+['v_pay_'+x for x in ('day','night','sat','sun','bh')])
def evaluate(source,fixture):
    f=tree(source);datums=f['datums'];body=find_list(f['action']);assert body is not None
    first=next(i for i,x in enumerate(body) if 'PLpgSQL_stmt_execsql' in x and 'sum((movement.canonical_pay_vector_json' in expr(x['PLpgSQL_stmt_execsql']['sqlstmt']))
    # The actual final presentation INSERT must consume the exact tracked vars.
    final=next(i for i,x in enumerate(body) if i>first and 'PLpgSQL_stmt_execsql' in x and 'insert into public.weekly_source_invoice_presentation_lines' in expr(x['PLpgSQL_stmt_execsql']['sqlstmt']).lower())
    insert=expr(body[final]['PLpgSQL_stmt_execsql']['sqlstmt'])
    assert all(n in insert for n in names+['v_total_pay','v_pay_day'])
    values={};events=[]
    def relevant(node):
        if isinstance(node,list):return any(relevant(x) for x in node)
        if not isinstance(node,dict):return False
        if 'PLpgSQL_stmt_assign' in node:return datums[node['PLpgSQL_stmt_assign']['varno']].get('PLpgSQL_var',{}).get('refname') in tracked
        if 'PLpgSQL_stmt_execsql' in node:
            fields=node['PLpgSQL_stmt_execsql'].get('target',{}).get('PLpgSQL_row',{}).get('fields',[])
            return any(x['name'] in tracked for x in fields)
        return any(relevant(x) for x in node.values())
    def condition(sql):
        sql=' '.join(sql.split())
        known={"v_movement.pay_effect='NONE' and not v_is_expense":fixture['effect']=='NONE' and not fixture['expense'],
          'v_movement.invoice_only_snapshot_line_id is not null':fixture['snapshot'],
          "v_movement.source_profile_kind='NHSP_TRUST_BACKING_REPORT'":fixture['nhsp'],
          'v_is_expense':fixture['expense'],
          'v_is_rounding and v_round_authority is not null':False}
        assert sql in known,('UNCLASSIFIED_RELEVANT_CONDITION',sql)
        return known[sql]
    def run(items):
        for node in items:
            if not relevant(node):continue
            if 'PLpgSQL_stmt_execsql' in node:
                s=node['PLpgSQL_stmt_execsql'];q=expr(s['sqlstmt'])
                if 'sum((movement.canonical_pay_vector_json' in q:
                    for name in names:values[name]=fixture['pay_hours'][name[8:]]
                    values['v_total_pay']=fixture['pay_total'];events.append(('ACTUAL_NATIVE_VECTOR_AGGREGATE',s['lineno']))
                elif 'from public.weekly_source_row_economic_snapshots e' in q:
                    for name in names:values[name]=fixture['economic_hours'][name[8:]]
                    events.append(('ACTUAL_BOUND_ECONOMIC_SELECT',s['lineno']))
                else:raise AssertionError(('UNCLASSIFIED_TRACKED_SELECT',q))
            elif 'PLpgSQL_stmt_assign' in node:
                s=node['PLpgSQL_stmt_assign'];name=datums[s['varno']]['PLpgSQL_var']['refname'];q=expr(s['expr']);rhs=q.split(':=',1)[1].rstrip(';')
                m=re.fullmatch(r"\(v_snapshot.pay_vector_json#>>'\{hours,(day|night|sat|sun|bh)\}'\)::numeric",rhs)
                rate=re.fullmatch(r"\(v_movement.canonical_pay_vector_json#>>'\{rates,(day|night|sat|sun|bh)\}'\)::numeric",rhs)
                if m:values[name]=fixture['snapshot_hours'][m[1]]
                elif rate:values[name]=fixture['pay_rates'][rate[1]]
                elif rhs=='0':values[name]='0'
                elif rhs=='null':values[name]=None
                elif re.fullmatch(re.escape(name)+r'::numeric\([0-9,]+\)',rhs):pass
                else:raise AssertionError(('UNCLASSIFIED_TRACKED_ASSIGN',name,rhs))
                events.append((name,s['lineno'],q))
            elif 'PLpgSQL_stmt_if' in node:
                s=node['PLpgSQL_stmt_if'];taken=False
                if condition(expr(s['cond'])):run(s.get('then_body',[]));taken=True
                for alt in s.get('elsif_list',[]):
                    alt=alt.get('PLpgSQL_if_elsif',alt)
                    if not taken and condition(expr(alt['cond'])):run(alt.get('stmts',[]));taken=True
                if not taken:run(s.get('else_body',[]))
            else:raise AssertionError(('UNCLASSIFIED_TRACKED_NODE',list(node)))
    run(body[first:final])
    return dict(hours={k:values['v_hours_'+k] for k in ('day','night','sat','sun','bh')},pay_total=values['v_total_pay'],pay_rates={k:values['v_pay_'+k] for k in ('day','night','sat','sun','bh')},events=events,insertLine=body[final]['PLpgSQL_stmt_execsql']['lineno'])
zero={k:'0' for k in ('day','night','sat','sun','bh')}
fixtures=[]
for label,effect,expense,snapshot,nhsp in [('SOURCE_NONE_REBILL','NONE',False,True,False),('NHSP_NONE_HOURS','NONE',False,False,True),('SOURCE_NONE_FIXED_EXPENSE','NONE',True,True,False),('ORDINARY_NORMAL','NORMAL',False,False,False)]:
    fixtures.append(dict(name=label,effect=effect,expense=expense,snapshot=snapshot,nhsp=nhsp,pay_hours={**zero,'day':'6.25'} if effect=='NORMAL' else zero,snapshot_hours={**zero,'day':'7.50'},economic_hours={**zero,'night':'8.25'},pay_rates={**zero,'day':'20'} if effect=='NORMAL' else zero,pay_total='125' if effect=='NORMAL' else '0'))
before=raw(here/'current'/target);after=raw(here/'forward'/target);rows=[]
for fixture in fixtures:
    old=evaluate(before.decode(),fixture);new=evaluate(after.decode(),fixture)
    expected=fixture['pay_hours'] if fixture['effect']=='NORMAL' else zero if fixture['expense'] else fixture['snapshot_hours'] if fixture['snapshot'] else fixture['economic_hours']
    assert new['hours']==expected
    assert new['pay_total']==old['pay_total']==fixture['pay_total']
    assert new['pay_rates']==old['pay_rates']
    if fixture['effect']=='NONE' and not fixture['expense']:assert old['hours']==zero and new['hours']!=zero
    rows.append(dict(fixture=fixture['name'],before=old,after=new,expectedHours=expected,passAssignmentOrder=True))
(here/'SOURCE_DATAFLOW_RESULTS.json').write_text(json.dumps(dict(beforeSha256=hashlib.sha256(before).hexdigest(),afterSha256=hashlib.sha256(after).hexdigest(),parser='pglast '+pglast.__version__,grammarOnlyRecordSurrogate=True,scope='ACTUAL_ASSIGNMENT_AST_ONLY_NO_NATIVE_SQL_OR_ORIGIN_PROOF',fixtures=rows,native='NOT_RUN',dbConnections=0),indent=2)+'\n',encoding='utf8',newline='\n')
print(json.dumps(dict(actualAssignmentAstVectors=len(rows),native='NOT_RUN')))
