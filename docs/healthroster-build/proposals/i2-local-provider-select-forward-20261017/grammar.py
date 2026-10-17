"""PG17 AST proof only. Never executes SQL, Docker or a provider script."""
import pathlib,json,re,sys
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
from pglast import parse_sql,ast
p=pathlib.Path(__file__).resolve().parent
f=json.loads((p/'FULL_PARENT_FORWARD.json').read_text())
def setup(source):
 m=re.search(r'execute\(`(alter role service_role.*?)`,database\);',source,re.S);assert m
 return m.group(1).replace('\\n','\n').replace('${owner}','cloudtms_hr_install2_seal_owner')
def signature(sql):
 a=parse_sql(sql);out=[]
 for x in a:
  if isinstance(x.stmt,ast.GrantStmt):
   z=x.stmt;objects=tuple((v.schemaname,v.relname) if isinstance(v,ast.RangeVar) else str(v) for v in z.objects)
   out.append((tuple(v.priv_name for v in z.privileges) if z.privileges else ('ALL',),objects,tuple((v.roletype.name,v.rolename) for v in z.grantees),z.grant_option,z.objtype.name))
 return out
old,new=setup(f['wholeBefore']),setup(f['wholeAfter']);a,b=signature(old),signature(new)
delta=[x for x in b if x not in a]
expected=(('select',),(('auth','users'),('vault','secrets')),(('ROLESPEC_CSTRING','cloudtms_hr_install2_seal_owner'),),False,'OBJECT_TABLE')
assert delta==[expected],delta
assert [x for x in b if x!=expected]==a
bad=[new.replace('grant select on auth.users,vault.secrets','grant all on auth.users,vault.secrets'),new.replace('auth.users,vault.secrets to','auth.users,vault.secrets,public.extra to'),new.replace('grant select on auth.users,vault.secrets to cloudtms_hr_install2_seal_owner','grant select on auth.users,vault.secrets to public'),new.replace('grant select on auth.users,vault.secrets to cloudtms_hr_install2_seal_owner','grant select on auth.users,vault.secrets to service_role'),new.replace('grant select on auth.users,vault.secrets to cloudtms_hr_install2_seal_owner;','grant select on auth.users,vault.secrets to cloudtms_hr_install2_seal_owner with grant option;')]
for sql in bad:
 assert [x for x in signature(sql) if x not in a]!=[expected]
fixture=(p/'reference/03-28082026_1229_banking_modal_local_pg17_prerequisites.sql').read_text();fixture='\n'.join(v for v in fixture.splitlines() if not v.startswith('\\set '));trees=parse_sql(fixture)
assert not any(isinstance(x.stmt,(ast.GrantStmt,ast.AlterDefaultPrivilegesStmt)) for x in trees)
tables={(x.stmt.relation.schemaname,x.stmt.relation.relname) for x in trees if isinstance(x.stmt,ast.CreateStmt)};assert tables=={('auth','users'),('vault','secrets')}
identity=re.search(r'execute\(`(select json_build_object.*?)`,database\)\);',f['wholeAfter'],re.S).group(1).replace('${owner}','cloudtms_hr_install2_seal_owner');assert len(parse_sql(identity))==1
print(json.dumps({'status':'PG17_AST_PASS_SOURCE_ONLY','setupStatementsBefore':len(parse_sql(old)),'setupStatementsAfter':len(parse_sql(new)),'grantDelta':1,'newBaseTableSelectPrivileges':2,'unchangedGrantStatements':len(a),'grantSurfaceNegativeVectors':len(bad),'fixtureStatements':len(trees),'fixtureBaseTables':2,'fixtureGrantOrDefaultAclStatements':0,'identitySelectStatements':1,'native':False}))
