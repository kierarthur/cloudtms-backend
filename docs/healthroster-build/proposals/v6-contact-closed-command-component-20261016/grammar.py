"""Offline PostgreSQL 17.7 outer grammar only; no catalog binding or execution."""
import json,sys
from pathlib import Path
sys.path.insert(0,'C:/tmp/healthroster-v6-invoice-parser-20261016')
from pglast import parse_sql, parser
root=Path(__file__).resolve().parent
assert parser.get_postgresql_version()==(17,7)
checks=[]
for mode in ('session-context','protected-owner-request'):
    d=root/'packets'/mode
    expected=json.loads((d/'EXPECTED.json').read_text())
    for name in ('COMPONENT.sql','FRESH_VERIFY.sql','READINESS_PREFLIGHT.sql','DISCOVERY_PREFLIGHT.sql','DISCOVERY.sql'):
        parsed=parse_sql((d/name).read_text())
        if name=='COMPONENT.sql':
            assert len(parsed)==expected['statementCount']
            assert sum(type(x.stmt).__name__=='CreateFunctionStmt' for x in parsed)==4
            assert sum(type(x.stmt).__name__=='TransactionStmt' for x in parsed)==0
            plpgsql=json.loads(parser.parse_plpgsql_json((d/name).read_text()))
            assert len(plpgsql)==(6 if mode=='session-context' else 7)
        else:
            assert all(type(x.stmt).__name__=='SelectStmt' for x in parsed)
        checks.append({'file':f'packets/{mode}/{name}','statements':len(parsed),'outerGrammar':'PASS',**({'offlinePlpgsqlSyntaxBodies':len(plpgsql)} if name=='COMPONENT.sql' else {})})
for p in sorted((root/'sources').glob('*.sqlinc')):
    checks.append({'file':p.relative_to(root).as_posix(),'statements':len(parse_sql(p.read_text())),'outerGrammar':'PASS','evidenceOnly':p.name.startswith(('04_','05_','06_','07_'))})
result={'version':'CONTACT_CLOSED_COMPONENT_OUTER_GRAMMAR_V1','postgresqlParser':'17.7','passed':len(checks),'checks':checks,'catalogBinding':'NOT_RUN','plpgsqlRuntime':'NOT_RUN','native':'NOT_RUN','dbConnections':0}
(root/'GRAMMAR_RESULTS.json').write_text(json.dumps(result,indent=2)+'\n',newline='\n')
print(json.dumps({'passed':len(checks),'qualification':'OUTER_GRAMMAR_ONLY','native':'NOT_RUN'}))
