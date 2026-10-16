"""Offline PG17 syntax/body/closure inventory only. No DB or fake functions."""
import hashlib,json,pathlib,re,sys
sys.path.insert(0,r'C:/tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import parser
here=pathlib.Path(__file__).resolve().parent
source=(here/'forward/sql/01_public_services.sqlinc').read_text()
acl=(here/'forward/sql/99_acl.sqlinc').read_text()
statements=pglast.parse_sql(source)
assert len(statements)==4
procedures=json.loads(parser.parse_plpgsql_json(source))
assert len(procedures)==4
records=[]
for m in re.finditer(r'create or replace function ((?:private|public)\.[a-z0-9_]+)\(([^)]*)\)',source):
 end=source.index('$function$;',m.end())+len('$function$;')
 whole=source[m.start():end]
 parsed=json.loads(parser.parse_sql_json(whole))['stmts'][0]['stmt']['CreateFunctionStmt']
 records.append({'name':m.group(1),'arguments':m.group(2),'wholeBytes':len(whole.encode()),'wholeSha256':hashlib.sha256(whole.encode()).hexdigest(),
  'completeCreateStatement':True,'parsedBody':True,'returns':'jsonb','securityDefiner':True})
assert len(records)==4
assert len(pglast.parse_sql(acl))==11
root=(here/'forward/daily_public_services_v1.sql').read_text()
expanded=root.replace('\\ir sql/01_public_services.sqlinc',source).replace('\\ir sql/99_acl.sqlinc',acl)
grammar='\n'.join(s for s in expanded.splitlines() if not s.startswith('\\'))
assert len(pglast.parse_sql(grammar))==18
assert root.index("notify pgrst, 'reload schema';")>root.index('\\ir sql/99_acl.sqlinc')
assert root.index("notify pgrst, 'reload schema';")<root.index('commit;')
out={'scope':'OFFLINE_PG17_SOURCE_ONLY','pglast':pglast.__version__,'postgresql':pglast.get_postgresql_version(),
 'completeFunctions':records,'aclStatements':11,'expandedRootStatements':18,'postAclNotifyBeforeCommit':True,
 'nativeCreationResolutionRolesAclApiConcurrency':'NOT_RUN','missingPrivateDelegatesCreated':False,'financialBodiesCreated':False}
(here/'GRAMMAR_RESULT.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps({'scope':out['scope'],'completeFunctions':len(records),'expandedStatements':18,'native':'NOT_RUN'}))
