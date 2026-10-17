"""Complete PL/SQL structural inversion and finite statement-position refusals."""
import sys,json,pathlib,copy
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
from pglast import parser
P=pathlib.Path('D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/qt-audit-select-into-forward-20261017')
def read(f):return pathlib.Path('\\\\?\\'+str(P/f)).read_bytes().decode()
def pl(s):return json.loads(parser.parse_plpgsql_json(s))
def walk(n):
    if isinstance(n,dict):
        yield n
        for v in n.values():yield from walk(v)
    elif isinstance(n,list):
        for v in n:yield from walk(v)
old,new=read('reference/adapter/qt-assertions.sql'),read('payload/qt-assertions.sql')
o,n=pl(old),pl(new)
original=next(x for x in walk(o) if 'PLpgSQL_stmt_assign' in x and 'audits_before:=' in json.dumps(x))
candidate=next(x for x in walk(n) if 'PLpgSQL_stmt_execsql' in x and 'public.audit_events' in json.dumps(x))
assert candidate['PLpgSQL_stmt_execsql']['into'] and not candidate['PLpgSQL_stmt_execsql'].get('strict',False)
target=copy.deepcopy(candidate['PLpgSQL_stmt_execsql']['target'])
assert target['PLpgSQL_row']['fields']==[{'name':'audits_before','varno':original['PLpgSQL_stmt_assign']['varno']}]
candidate.clear();candidate.update(copy.deepcopy(original))
removed=0
def remove_target_datum(v):
    global removed
    if isinstance(v,list):
        for x in list(v):
            if x==target:v.remove(x);removed+=1
            else:remove_target_datum(x)
    elif isinstance(v,dict):
        for x in v.values():remove_target_datum(x)
remove_target_datum(n)
assert removed==1,'only compiler-created anonymous INTO target datum may differ'
assert n==o,'complete original PL body differs beyond binding node'
anchor=' if (select to_jsonb(s) from public.weekly_source_global_settings'
assert old[old.index(anchor):]==new[new.index(anchor):]
digest="md5(coalesce(string_agg(md5(to_jsonb(a)::text),'|' order by md5(to_jsonb(a)::text)),''))"
before=' audits_before:=(select '+digest+' from public.audit_events a);'
after=' select '+digest+' into audits_before from public.audit_events a;'
assert new.replace(after,before)==old and new.count(digest)==2
assert new.index('defaults_before:=')<new.index(after)<new.index(' insert into public.tms_users')
# Syntactically valid but materially different source placements/shape must not
# satisfy the complete-body inverse, even if they retain both digest strings.
mutations={
 'moveBeforeSettings':new.replace(after,'').replace(' global_before:=',after+'\n global_before:='),
 'moveAfterFirstMutation':new.replace(after,'').replace(' get_request:=',after+'\n get_request:='),
 'addedRead':new.replace(after,after+'\n perform 1 from public.audit_events;'),
 'foundConsumer':new.replace(after,after+"\n if not found then raise exception 'bad';end if;"),
 'rowCountConsumer':new.replace(after,after+'\n get diagnostics denials = row_count;'),
 'changedFinalIf':new.replace('is distinct from audits_before then','is not distinct from audits_before then'),
}
for name,s in mutations.items():pl(s);assert s.replace(after,before)!=old,name
print(json.dumps({'completePlBodyAstInverse':'PASS','exactCompilerAnonymousTargetDatum':1,'rawFinalIfAndFollowingAssertions':'PASS','unchangedReadPosition':'PASS','twoCompleteDigestExpressions':2,'validAstAdversarialRefusals':list(mutations),'native':'NOT_RUN','performance':'NOT_PROVED'}))
