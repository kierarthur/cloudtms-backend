"""PG17 exact prefix and whole-query EXPLAIN proof; never executes SQL."""
from pathlib import Path
import sys,json
sys.dont_write_bytecode=True
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
HERE=Path(__file__).resolve().parent
def tree(text):return json.loads(pglast.parser.parse_sql_json(text))
def clean(x):
 if isinstance(x,dict):return{k:clean(v)for k,v in x.items()if k not in ['location','stmt_len','stmt_location']}
 if isinstance(x,list):return[clean(v)for v in x]
 return x
out=[]
for case in ['CONTACT_CONSTRUCTORS_V1','QT_ADMIN_CONTROLS_V1']:
 r=json.loads((HERE/('RENDERED_'+case+'.json')).read_text(encoding='utf-8'));base=tree(r['originalInput'])['stmts'];assert len(base)==6
 prefix=tree('\n'.join(l for l in r['prefixInput'].splitlines()if not l.startswith('\\echo ')))['stmts'];plan=tree('\n'.join(l for l in r['planInput'].splitlines()if not l.startswith('\\echo ')))['stmts'];assert len(prefix)==5 and len(plan)==6
 assert clean(prefix)==clean(base[:4]+base[5:]);assert clean(plan[:4]+plan[5:])==clean(base[:4]+base[5:])
 explain=plan[4]['stmt']['ExplainStmt'];assert clean(explain['query'])==clean(base[4]['stmt']);opts={d['DefElem']['defname']:d['DefElem']['arg']for d in explain['options']};assert set(opts)=={'analyze','verbose','costs','settings','summary','format'}
 for key in ['analyze','verbose','settings','summary']:assert opts[key]=={'String':{'sval':'false'}}
 assert opts['costs']=={'String':{'sval':'true'}};assert opts['format']=={'String':{'sval':'json'}}
 # The one PL body is the exact unchanged native boundary guard, not a surrogate.
 for stmt in [prefix[1],plan[1]]:pglast.parser.parse_plpgsql_json(r['originalInput'][base[1]['stmt_location']:base[1]['stmt_location']+base[1]['stmt_len']])
 out.append({'case':case,'originalStatements':6,'prefixStatements':5,'planStatements':6,'exactOriginalPrefixAndRollbackAst':True,'wholeCanonicalExplainQueryAstEqual':True,'analyze':False,'native':'NOT_RUN'})
print(json.dumps({'parser':pglast.__version__,'postgresql':list(pglast.get_postgresql_version()),'cases':out,'native':'NOT_RUN'},indent=2))
