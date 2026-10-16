import hashlib,json,pathlib,sys
sys.path.insert(0,r'C:/tmp/healthroster-v6-physical-parser')
import pglast
from pglast import parser
here=pathlib.Path(__file__).resolve().parent
sql=(here/'frozen/forward/sql/01_public_services.sqlinc').read_text()
acl=(here/'frozen/forward/sql/99_acl.sqlinc').read_text()
root=(here/'frozen/forward/daily_public_services_v1.sql').read_text()
assert len(pglast.parse_sql(sql))==4
assert len(json.loads(parser.parse_plpgsql_json(sql)))==4
assert len(pglast.parse_sql(acl))==11
expanded=root.replace('\\ir sql/01_public_services.sqlinc',sql).replace('\\ir sql/99_acl.sqlinc',acl)
expanded='\n'.join(line for line in expanded.splitlines() if not line.startswith('\\'))
assert len(pglast.parse_sql(expanded))==18
result={'scope':'INDEPENDENT_OFFLINE_PG17_GRAMMAR_ONLY','pglast':pglast.__version__,'postgresql':pglast.get_postgresql_version(),
 'completeFunctions':4,'completePlpgsqlBodies':4,'aclStatements':11,'expandedStatements':18,
 'sqlSha256':hashlib.sha256((here/'frozen/forward/sql/01_public_services.sqlinc').read_bytes()).hexdigest(),
 'nativeCatalogueTypeResolutionRolesAclApi':'NOT_RUN'}
(here/'GRAMMAR_INDEPENDENT.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result))
