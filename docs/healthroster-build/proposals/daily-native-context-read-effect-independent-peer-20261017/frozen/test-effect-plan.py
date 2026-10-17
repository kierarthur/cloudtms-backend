from pathlib import Path
import sys,runpy,copy,json
sys.dont_write_bytecode=True
HERE=Path(__file__).resolve().parent
plan=runpy.run_path(str(HERE/'effect-plan.py'));ast=runpy.run_path(str(HERE/'ast-census.py'))
base=json.loads((HERE/'AST_CENSUS.json').read_text(encoding='utf-8'));cases=[]
def check(label,test):test();cases.append({'case':label,'status':'PASS','native':'NOT_RUN'})
def refusal(value):
    try:plan['classify'](value)
    except ValueError:return
    raise AssertionError('MUTATION_ACCEPTED')
def query_refusal(sql):
    c=copy.deepcopy(base);c['owners'][0].update(ast['census']([ast['sqltree'](sql)]));refusal(c)
def mutate_refusal(edit):
    c=copy.deepcopy(base);edit(c);refusal(c)
result=plan['classify'](base)
check('complete actual18-owner source remains held for native/finance qualification',lambda:(_ for _ in ()).throw(AssertionError()) if result['nativePurity']!='NOT_PROVED' else None)
for label,sql in [
 ('data-changing CTE under SELECT','WITH x AS (DELETE FROM public.timesheets RETURNING timesheet_id) SELECT * FROM x'),
 ('direct UPDATE','UPDATE public.timesheets SET is_current=false'),
 ('SELECT row-lock side effect','SELECT * FROM public.timesheets FOR UPDATE'),
 ('SELECT INTO new table','SELECT 1 INTO public.unreviewed_table'),
 ('stable parent hides unknown volatile call','SELECT public.unreviewed_effect()'),
 ('sequence mutation hidden by SELECT',"SELECT nextval('public.unreviewed_sequence')"),
 ('session configuration hidden by SELECT',"SELECT set_config('search_path','public',false)"),
 ('advisory lock hidden by SELECT','SELECT pg_advisory_lock(123)'),
 ('network extension hidden by SELECT',"SELECT extensions.http_get('https://example.invalid')"),
 ('new relation not in complete source set','SELECT * FROM public.unreviewed_relation'),
 ('unreviewed custom cast','SELECT NULL::public.unreviewed_domain'),
 ('digest shadow namespace is not pgcrypto',"SELECT public.digest('a','sha256')")]:
    check(label,lambda sql=sql:query_refusal(sql))
for label,edit in [
 ('missing complete current owner',lambda c:c['owners'].pop()),
 ('unparsed nested SQL cannot be waived',lambda c:c['owners'][0]['unparsedFragments'].append({'query':'unparsed'})),
 ('dynamic PL cannot be waived',lambda c:c['owners'][0]['plStatementKinds'].append('PLpgSQL_stmt_dynexecute')),
 ('foreign/unreviewed language cannot be waived',lambda c:c['owners'][0].update(language='plpython3u'))]:check(label,lambda edit=edit:mutate_refusal(edit))
check('one-argument finance boundary remains explicit',lambda:(_ for _ in ()).throw(AssertionError()) if result['financeBoundaries']['sourceCallArity']!=1 else None)
check('unqualified digest resolution remains required',lambda:(_ for _ in ()).throw(AssertionError()) if not any(e['call']=='digest' and e['classification'].startswith('EXACT_PGCRYPTO')for e in result['edges']) else None)
check('default-VOLATILE PDF readers are source inspected, not assumed pure from parent STABLE',lambda:(_ for _ in ()).throw(AssertionError()) if not {'public.timesheet_pdf_reference_rows(uuid)','public.timesheet_pdf_reference_sig(uuid)'}<=plan['OWNERS'] else None)
out={'version':'DAILY_CONTEXT_SOURCE_EFFECT_COUNTEREXAMPLES_V1','cases':cases,'tests':len(cases),'native':'NOT_RUN','syntheticMutationsOnly':True}
(HERE/'TEST_RESULTS.json').write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8');print(json.dumps({'tests':len(cases),'native':'NOT_RUN'}))
