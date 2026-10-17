"""Bounded control-flow traversal of actual PG17 parsed helper, no SQL execution."""
from pathlib import Path
import sys,json,hashlib
HERE=Path(__file__).resolve().parent
sys.path.insert(0,r'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
sql=(HERE/'frozen/health/05_review_enrollment.sqlinc').read_text()
body=pglast.parse_plpgsql(sql)[0]['PLpgSQL_function']['action']['PLpgSQL_stmt_block']['body']
class Raised(Exception): pass
class Returned(Exception): pass
def query(obj): return obj['PLpgSQL_expr']['query']
def run(case):
    events=[];replay=False;calls=0;prepared=False
    def execute(nodes):
        nonlocal replay,calls,prepared
        for node in nodes:
            kind,data=next(iter(node.items()))
            if kind=='PLpgSQL_stmt_perform':
                q=query(data['expr'])
                if 'request_assert_v1' in q:
                    events.append('request_assert')
                    if case.get('bad_request'): raise Raised('REQUEST_INVALID')
                elif 'health_actor_require_v1(actor,false)' in q: events.append('actor_initial')
                elif 'daily_rate_correction_context_v1' in q: events.append('native_context_true')
                elif 'health_actor_require_v1(actor,true)' in q:
                    events.append('actor_locked')
                    if case.get('actor_lost'): raise Raised('ACTOR_NOT_ALLOWED')
                else: raise AssertionError(q)
            elif kind=='PLpgSQL_stmt_assign':
                q=query(data['expr'])
                if q.startswith('actor:='): events.append('actor_from_request')
                elif 'preview_replay_v1' in q:
                    calls+=1;replay=case.get('first_replay',False) if calls==1 else case.get('after_wait_replay',False)
                    events.append('replay_'+str(calls))
                elif 'health_basis_v1(p_args,true)' in q:
                    events.append('locked_basis')
                    if case.get('basis_stale'): raise Raised('REVIEW_STALE')
                elif 'health_review_prepare_v1' in q:
                    assert 'native_context_true' in events and 'actor_locked' in events and 'locked_basis' in events
                    assert events.index('replay_2')<events.index('locked_basis')
                    events.append('metadata_initializer');prepared=True
                else: raise AssertionError(q)
            elif kind=='PLpgSQL_stmt_if':
                q=query(data['cond'])
                if q=='replay is not null': yes=replay
                elif q.startswith("current_setting('transaction_isolation')"): yes=case.get('not_rc',False)
                elif q.startswith("prepared->'daily' is distinct from daily"): yes=case.get('prepared_mismatch',False)
                else: raise AssertionError(q)
                execute(data.get('then_body',[]) if yes else data.get('else_body',[]))
            elif kind=='PLpgSQL_stmt_raise': raise Raised(data['message'])
            elif kind=='PLpgSQL_stmt_return':
                q=query(data['expr']);events.append('return_replay' if "'replayed',true" in q else 'return_prepared')
                raise Returned()
            else: raise AssertionError(kind)
    outcome=None
    try: execute(body)
    except Returned: outcome='RETURNED'
    except Raised as error: outcome=str(error)
    if case.get('first_replay'): assert events[-1]=='return_replay' and 'native_context_true' not in events and not prepared
    elif case.get('after_wait_replay'): assert events[-1]=='return_replay' and 'native_context_true' in events and not prepared
    elif case.get('bad_request') or case.get('not_rc') or case.get('actor_lost') or case.get('basis_stale'): assert not prepared and outcome!='RETURNED'
    elif case.get('prepared_mismatch'): assert prepared and outcome=='REVIEW_STALE' and not any(e.startswith('return_') for e in events)
    else: assert prepared and events[-1]=='return_prepared'
    return {'case':case['name'],'events':events,'outcome':outcome,'nativeExecution':False}
cases=[{'name':'saved_identical','first_replay':True},{'name':'concurrent_identical_after_native_wait','after_wait_replay':True},{'name':'fresh_locked_enrollment'},{'name':'non_RC_refusal','not_rc':True},{'name':'postwait_actor_loss','actor_lost':True},{'name':'locked_context_stale','basis_stale':True},{'name':'initializer_changed_basis_rolls_back','prepared_mismatch':True},{'name':'invalid_request_before_locks','bad_request':True}]
results=[run(c) for c in cases]
out={'parser':pglast.__version__,'sourceSha256':hashlib.sha256(sql.encode()).hexdigest(),'paths':results,'native':'NOT_RUN','proofLimit':'Actual AST paths with controlled dependency outcomes; not native lock, FK or race execution.'}
(HERE/'ENROLLMENT_AST_PATHS.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps({'paths':len(results),'native':'NOT_RUN'}))
