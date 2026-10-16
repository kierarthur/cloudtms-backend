"""PG17 offline grammar; no client connection, secrets or native claim."""
import json,pathlib,sys
sys.path.insert(0,r'C:/tmp/healthroster-v6-invoice-parser-20261016')
import pglast
from pglast import parser
here=pathlib.Path(__file__).resolve().parent
programs=json.loads((here/'OFFLINE_PROGRAMS.json').read_text())['programs']
def clean(value):
 if isinstance(value,dict):return {k:clean(v) for k,v in value.items() if k not in ('location','stmt_location','stmt_len')}
 if isinstance(value,list):return [clean(v) for v in value]
 return value
def ast(sql):return clean(json.loads(parser.parse_sql_json(sql)))
rows=[]
for row in programs:
 statements=pglast.parse_sql(row['input'])
 assert len(statements)==6
 assert row['input'].count(row['canonicalSql'])==1
 guard=statements[1]
 # Anonymous DO source and every SQL statement are parsed by the PG17 parser.
 assert 'DoStmt' in ast(row['input'])['stmts'][1]['stmt']
 parser.parse_plpgsql_json(row['input'])
 assert ast(row['input'])['stmts'][4]['stmt']==ast(row['canonicalSql'])['stmts'][1]['stmt']
 rows.append({'caseId':row['caseId'],'statements':len(statements),'completeCanonicalQueryASTIdentical':True,'wholeCanonicalBytesOnce':True,'guardParsed':True})
out={'scope':'OFFLINE_POSTGRES17_GRAMMAR_ONLY','pglast':pglast.__version__,'postgresql':pglast.get_postgresql_version(),'cases':rows,'native':'NOT_RUN'}
(here/'GRAMMAR_RESULT.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps({'scope':out['scope'],'programs':len(rows),'native':'NOT_RUN'}))
