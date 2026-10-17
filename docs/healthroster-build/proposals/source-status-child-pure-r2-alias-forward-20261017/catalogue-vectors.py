"""Evaluate exact PG17 admission predicates with synthetic catalogue boundaries.

Raw native definition hash/length and effective privileges are opaque supplied
tokens from the sealed receipt. This executes no SQL and proves no native state.
"""
from pathlib import Path
import copy,hashlib,json,sys
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import ast
HERE=Path(__file__).resolve().parent
sha=lambda b:hashlib.sha256(b).hexdigest()

def evaluate(node,c):
 if isinstance(node,ast.A_Const):
  if node.isnull:return None
  if isinstance(node.val,ast.String):return node.val.sval
  if isinstance(node.val,ast.Boolean):return node.val.boolval
  return node.val.ival
 if isinstance(node,ast.ColumnRef):
  value=c
  for part in node.fields:value=value.get(part.sval) if isinstance(value,dict) else None
  return value
 if isinstance(node,ast.TypeCast):return evaluate(node.arg,c)
 if isinstance(node,ast.A_ArrayExpr):return [evaluate(x,c) for x in node.elements]
 if isinstance(node,ast.NullTest):return (evaluate(node.arg,c) is None)==(node.nulltesttype.name=='IS_NULL')
 if isinstance(node,ast.BoolExpr):
  v=[evaluate(x,c) for x in node.args]
  if node.boolop.name=='NOT_EXPR':return None if v[0] is None else not v[0]
  if node.boolop.name=='OR_EXPR':return True if True in v else None if None in v else False
  return False if False in v else None if None in v else True
 if isinstance(node,ast.A_Expr):
  l,r=evaluate(node.lexpr,c),evaluate(node.rexpr,c)
  if node.kind.name=='AEXPR_DISTINCT':return l!=r
  if l is None or r is None:return None
  op=node.name[0].sval
  if op=='<>':return l!=r
  if op=='=':return l==r
  raise TypeError(op)
 if isinstance(node,ast.FuncCall):
  name='.'.join(x.sval for x in node.funcname);a=[evaluate(x,c) for x in node.args or []]
  if name=='pg_catalog.pg_get_userbyid':return a[0]
  if name=='pg_catalog.pg_get_functiondef':return c['raw_definition_token']
  if name=='octet_length':return a[0]['bytes']
  if name=='cardinality':return None if a[0] is None else len(a[0])
  if name=='pg_catalog.aclexplode':return a[0] or []
  if name=='pg_catalog.convert_to':
   assert a[1]=='UTF8';return a[0]
  if name=='pg_catalog.sha256':return a[0]['sha256'] if isinstance(a[0],dict) else sha(a[0].encode())
  if name=='pg_catalog.encode':
   assert a[1]=='hex';return a[0]
  if name=='pg_catalog.has_function_privilege':return c['effective_execute'][a[0]]
  if name=='pg_catalog.has_schema_privilege':return c['schema_usage'][a[0]]
  raise TypeError(name)
 if isinstance(node,ast.SubLink):
  s=node.subselect;source=s.fromClause[0]
  if isinstance(source,ast.RangeVar):
   assert source.schemaname=='pg_catalog' and source.relname=='pg_language'
   candidates=[{**c,**row} for row in c['languages']]
  else:
   assert isinstance(source,ast.RangeFunction)
   candidates=[{**c,source.alias.aliasname:row} for row in evaluate(source.functions[0][0],c)]
  rows=[r for r in candidates if evaluate(s.whereClause,r) is True]
  target=s.targetList[0].val
  if isinstance(target,ast.FuncCall) and target.agg_star:
   assert target.funcname[0].sval=='count';return len(rows)
  return evaluate(target,rows[0]) if rows else None
 raise TypeError(type(node))

def guard(file,message):
 t=(HERE/'programme'/file).read_text()
 start=t.index(' if p.prokind')+4;end=t.index(" then\n  raise exception '"+message,start)
 result=pglast.parse_sql('select '+t[start:end])[0].stmt.targetList[0].val
 assert isinstance(result,ast.BoolExpr) and result.boolop.name=='OR_EXPR'
 return result

key=(HERE/'programme/key-source.sqlinc').read_text().split('as $function$')[1].split('$function$;')[0]
owner='dee50tht'
acl=dict(grantee=owner,grantor=owner,privilege_type='EXECUTE',is_grantable=False)
context=dict(p=dict(oid='native-oid-boundary',pronamespace='private-namespace-boundary',prokind='f',pronargs=2,
 prorettype='boolean',proretset=False,proargtypes='3802 1009',proargnames=['p_value','p_allowed_keys'],
 prolang='sql-oid',provolatile='i',prosecdef=False,proowner=owner,
 proconfig=['search_path=pg_catalog, pg_temp'],prosrc=key,proacl=[acl]),
 languages=[dict(oid='sql-oid',lanname='sql'),dict(oid='plpgsql-oid',lanname='plpgsql')],
 raw_definition_token=dict(bytes=429,sha256='bb36d7e2d71abd7b9c0dfba29936d85edca54c8101591fd113e11d3ab7cf9189'),
 effective_execute=dict(anon=False,authenticated=False,service_role=False),
 schema_usage=dict(anon=False,authenticated=False,service_role=True))
pre=guard('preflight.sql','SOURCE_PURE_GENUINE_DEPENDENCY_CHANGED')
post=guard('postflight.sql','SOURCE_PURE_NEW_DEFINITION_OR_ACL_REFUSED')
checks=[]
def verify(name,node,c,expected):
 assert evaluate(node,c) is expected,name
 checks.append(name)
verify('existing-receipt-boundary-admitted',pre,context,False)
for field,value in dict(prokind='p',pronargs=1,prorettype='jsonb',proretset=True,proargtypes='3802',
 proargnames=['wrong'],prolang='plpgsql-oid',provolatile='v',prosecdef=True,proowner='other',
 proconfig=None,prosrc='changed').items():
 c=copy.deepcopy(context);c['p'][field]=value;verify('existing-changed-'+field,pre,c,True)
for field,value in [('bytes',430),('sha256','changed')]:
 c=copy.deepcopy(context);c['raw_definition_token'][field]=value;verify('raw-native-'+field+'-drift',pre,c,True)
for value in [None,[],[acl,acl], [dict(acl,grantee='public')], [dict(acl,grantor='other')],
 [dict(acl,privilege_type='USAGE')],[dict(acl,is_grantable=True)]]:
 c=copy.deepcopy(context);c['p']['proacl']=value;verify('missing-extra-or-changed-existing-ACL:'+str(value),pre,c,True)
for role in ['anon','authenticated','service_role']:
 c=copy.deepcopy(context);c['effective_execute'][role]=True;verify('effective-execute-'+role,pre,c,True)
 c=copy.deepcopy(context);c['schema_usage'][role]=not c['schema_usage'][role];verify('schema-usage-'+role,pre,c,True)

new=copy.deepcopy(context)
new['p'].update(pronargs=5,prorettype='jsonb',proargtypes='3802 2950 2950 2950 2950',
 proargnames=['p_source','p_family','p_run','p_decision','p_request'],prolang='plpgsql-oid',prosecdef=True,
 proconfig=['search_path=pg_catalog, private, public'],
 prosrc=(HERE/'programme/source-child.sqlinc').read_text().split('as $function$')[1].split('$function$;')[0])
verify('new-complete-definition-owner-ACL-admitted',post,new,False)
for field,value in dict(prokind='p',pronargs=4,prorettype='boolean',proretset=True,proargtypes='3802',
 proargnames=['wrong'],prolang='sql-oid',provolatile='v',prosecdef=False,proowner='other',
 proconfig=None,prosrc='changed').items():
 c=copy.deepcopy(new);c['p'][field]=value;verify('new-changed-'+field,post,c,True)
for value in [None,[],[acl,acl],[dict(acl,grantee='public')],[dict(acl,grantor='other')],[dict(acl,is_grantable=True)]]:
 c=copy.deepcopy(new);c['p']['proacl']=value;verify('missing-extra-or-changed-new-ACL:'+str(value),post,c,True)
for role in ['anon','authenticated','service_role']:
 c=copy.deepcopy(new);c['effective_execute'][role]=True;verify('new-effective-execute-'+role,post,c,True)

for file,expected in [('preflight.sql',0),('postflight.sql',1)]:
 t=(HERE/'programme'/file).read_text()
 clause=t[t.index(' if n<>')+4:t.index(' then raise exception',t.index(' if n<>'))]
 node=pglast.parse_sql('select '+clause)[0].stmt.targetList[0].val
 for count in [0,1,2]:verify(file+'-name-wide-count-'+str(count),node,dict(n=count),count!=expected)
print(json.dumps(dict(checks=len(checks),native='NOT_RUN',scope='EXACT_ACTUAL_ADMISSION_AST_SYNTHETIC_CATALOGUE_BOUNDARIES',
 rawNativeDefinitionAndEffectivePrivileges='OPAQUE_BOUNDARY_TOKENS_NOT_A_NATIVE_OBSERVATION')))
