"""Offline PostgreSQL grammar only; never opens a connection."""
import pathlib,re,json,sys
if len(sys.argv)>1:sys.path.insert(0,sys.argv[1])
import pglast
root=pathlib.Path(__file__).resolve().parent
checks=[];failures=[]
def expressions(node):
 if isinstance(node,dict):
  if 'PLpgSQL_expr' in node:yield node['PLpgSQL_expr']
  for value in node.values():yield from expressions(value)
 elif isinstance(node,list):
  for value in node:yield from expressions(value)
for f in sorted(list((root/'forward').glob('*.sqlinc'))+list((root/'fixtures').glob('*.sql'))):
 s=f.read_text(encoding='utf-8');s='\n'.join(x for x in s.splitlines() if not x.startswith('\\'))
 try:
  n=len(pglast.parse_sql(s));g=re.sub(r'\b(?:public|private)\.[a-z0-9_]+%rowtype\b','record',s,flags=re.I)
  procs=pglast.parse_plpgsql(g) if re.search(r'language\s+plpgsql|do\s+\$',s,re.I) else []
  count=0
  for expr in expressions(procs):
   q=expr['query'];mode=expr.get('parseMode',0)
   if mode==2:q='SELECT '+q
   elif mode in (3,4,5):q='SELECT '+re.split(r':=|=(?!=)',q,maxsplit=1)[1]
   pglast.parse_sql(q);count+=1
  checks.append({'path':str(f.relative_to(root)).replace('\\','/'),'statements':n,'plpgsql_functions':len(procs),'inner_sql_expressions':count,'rowtype_parser_alias_only':g!=s})
 except Exception as e:failures.append({'path':str(f),'error':str(e)})
r={'parser':'pglast '+pglast.__version__,'database_calls':False,'native':'NOT_RUN','checks':checks,'failures':failures}
(root/'SOURCE_PARSE_RESULT.json').write_text(json.dumps(r,indent=2)+'\n',encoding='utf-8');print(json.dumps(r));sys.exit(bool(failures))
