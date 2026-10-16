"""Offline PostgreSQL 17 grammar only. No connection or database execution."""
import pathlib,json,re,sys
sys.path.insert(0,sys.argv[1])
import pglast
root=pathlib.Path(__file__).resolve().parent
def exprs(v):
 if isinstance(v,dict):
  if 'PLpgSQL_expr' in v:yield v['PLpgSQL_expr']
  for value in v.values():yield from exprs(value)
 elif isinstance(v,list):
  for value in v:yield from exprs(value)
results=[]
for p in sorted([*(root/'forward').glob('*.sqlinc'),*(root/'fixtures').glob('*.sqlinc')]):
 s=p.read_text();statements=pglast.parse_sql(s)
 # libpg_query lacks the native catalogue for %ROWTYPE assignments; only its
 # parser declaration is aliased. Executable source remains unchanged.
 parser=re.sub(r'\b(?:public|private)\.[a-z0-9_]+%rowtype\b','record',s,flags=re.I)
 bodies=pglast.parse_plpgsql(parser);inner=0
 for e in exprs(bodies):
  q=e['query'];mode=e.get('parseMode',0)
  if mode==2:q='SELECT '+q
  elif mode in(3,4,5):q='SELECT '+re.split(r':=|=(?!=)',q,maxsplit=1)[1]
  pglast.parse_sql(q);inner+=1
 results.append({'path':p.relative_to(root).as_posix(),'statements':len(statements),'plpgsql':len(bodies),'inner_sql':inner,'rowtype_parser_alias_only':parser!=s})
out={'qualification':'OFFLINE_GRAMMAR_ONLY','native':'NOT_RUN','pglast':pglast.__version__,'checks':results}
(root/'SOURCE_PARSE_RESULT.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps(out))
