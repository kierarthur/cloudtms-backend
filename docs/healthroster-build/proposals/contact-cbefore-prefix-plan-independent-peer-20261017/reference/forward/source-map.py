"""PG17 actual complete-query identifier census. No SQL/native execution."""
from pathlib import Path
import sys,json,hashlib
sys.dont_write_bytecode=True
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
HERE=Path(__file__).resolve().parent
def walk(x):
 if isinstance(x,dict):
  yield x
  for v in x.values():yield from walk(v)
 elif isinstance(x,list):
  for v in x:yield from walk(v)
def census(raw):
 tree=json.loads(pglast.parser.parse_sql_json(raw));ctes={n['CommonTableExpr']['ctename']for n in walk(tree)if'CommonTableExpr'in n};rels={}
 for n in walk(tree):
  if'RangeVar'not in n:continue
  r=n['RangeVar'];name=r['relname'];schema=r.get('schemaname')
  if schema is None and name in ctes:continue
  assert schema in ['pg_catalog','public','private'];rels.setdefault(name,set()).add(schema)
 return ctes,rels
first=json.loads((HERE/'PRIOR_RENDERED_CONTACT_CONSTRUCTORS_V1.json').read_text(encoding='utf-8'));second=json.loads((HERE/'PRIOR_RENDERED_QT_ADMIN_CONTROLS_V1.json').read_text(encoding='utf-8'));ctes,relations=census(first['wholeQuery']);assert (ctes,relations)==census(second['wholeQuery']);assert len(ctes)==16 and len(relations)==33
# Only expansions of the three actual SQL-referenced builtin system views.
# Documentary PG17.11 source names; this is not native body identity/effect proof.
expansions={'pg_roles':['pg_authid','pg_db_role_setting'],'pg_policies':['pg_authid','pg_policy','pg_class','pg_namespace'],'pg_stat_activity':['pg_database','pg_authid']}
for view in expansions:assert relations.get(view)=={'pg_catalog'}
sources={name:{'kind':'ACTUAL_QUERY_RANGEVAR_AST','viaViews':[]}for name in relations}
for view,names in expansions.items():
 for name in names:
  if name not in relations:relations[name]={'pg_catalog'};sources[name]={'kind':'PG17_11_SYSTEM_VIEW_SOURCE_EXPANSION','viaViews':[]}
  if sources[name]['kind']=='PG17_11_SYSTEM_VIEW_SOURCE_EXPANSION':sources[name]['viaViews'].append(view)
out={'version':'CONTACT_CANONICAL_PLAN_SOURCE_SCOPE_ORDINALS_V1','queryRender':'SYNTHETIC_SOURCE_ONLY_NOT_NATIVE','wholeQueryRenderSha256':hashlib.sha256(first['wholeQuery'].encode()).hexdigest(),'parser':pglast.__version__,'postgresql':list(pglast.get_postgresql_version()),'directRelationNames':33,'cteNames':16,'upstreamViewSource':'https://raw.githubusercontent.com/postgres/postgres/REL_17_11/src/backend/catalog/system_views.sql','upstreamViewScope':expansions,'nativeViewIdentityClaimed':False,'relations':[{'ordinal':i+1,'name':name,'schemas':sorted(relations[name]),**sources[name]}for i,name in enumerate(sorted(relations))],'ctes':[{'ordinal':i+1,'name':name}for i,name in enumerate(sorted(ctes))]}
file=HERE/'candidate/docs/healthroster-build/proposals/contact-qt-positive-native-proposal-20261017/PLAN_SOURCE_SCOPES.json';assert str(file).startswith('D:');file=Path('\\\\?\\'+str(file));raw=json.dumps(out,indent=2)+'\n'
if file.exists():assert json.loads(file.read_text(encoding='utf-8'))==out
else:file.write_text(raw,encoding='utf-8')
print(json.dumps({'directRelationNames':33,'closedRelationNames':len(relations),'ctes':16,'native':'NOT_RUN'}))
