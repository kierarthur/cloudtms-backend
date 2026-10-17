"""Independent PG17 grammar only; writes this owned receipt, never SQL or DB."""
from pathlib import Path
import hashlib,json,subprocess,sys
HERE=Path(__file__).resolve().parent
OWN=HERE.parents[3]
PHYSICAL=OWN.parent/'healthroster-checklist-native-14102026'
ROOT=OWN.parent/'healthroster-install2-14102026'
PARSER=Path('D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
assert PARSER.is_dir()
sys.path.insert(0,str(PARSER))
import pglast
from pglast import ast,parser
proposal=PHYSICAL/'docs/healthroster-build/proposals/daily-health-support-managed-root-20261017'
manifest=json.loads((proposal/'PAYLOAD_MANIFEST.json').read_text(encoding='utf-8'))
contract=json.loads((proposal/'CATALOGUE_EXPECTATIONS.json').read_text(encoding='utf-8'))
programs=[];functions=[]
for row in manifest['files']:
 if not row['target'].endswith(('.sql','.sqlinc')):continue
 data=(proposal/row['source']).read_bytes()
 assert hashlib.sha256(data).hexdigest()==row['sha256']
 text='\n'.join(s for s in data.decode('utf-8').splitlines() if not s.startswith('\\'))
 parsed=pglast.parse_sql(text);bodies=json.loads(parser.parse_plpgsql_json(text))
 for item in parsed:
  if not isinstance(item.stmt,ast.CreateFunctionStmt):continue
  fn=item.stmt;name='.'.join(x.sval for x in fn.funcname)
  options={x.defname:x.arg for x in fn.options}
  assert options['security'].boolval is True
  assert 'search_path' in str(options['set'])
  assert options['language'].sval=='plpgsql'
  functions.append(name)
 programs.append({'file':row['target'],'statements':len(parsed),'parserPrograms':len(bodies),'status':'PASS_GRAMMAR_ONLY'})
assert sorted(functions)==sorted(r['signature'].split('(')[0] for r in contract['routines'])
reverse=pglast.parse_sql((proposal/'reverse/complete_additive_reverse.sqlinc').read_text(encoding='utf-8'))
assert len(reverse)==6
pure=ROOT/'docs/healthroster-build/proposals/daily-health-pure-native-proof-20261017'
source='import {buildPureProgram} from '+json.dumps((pure/'build-program.mjs').as_uri())+';process.stdout.write(buildPureProgram({root:process.cwd()}).sql);'
result=subprocess.run(['node','--input-type=module','-e',source],cwd=ROOT,capture_output=True,check=True)
sql=result.stdout.decode('utf-8')
parsed=pglast.parse_sql(sql);native_programs=json.loads(parser.parse_plpgsql_json(sql))
assert len(parsed)==7 and len(native_programs)==6 and len(result.stdout)==8029
record={'scope':'INDEPENDENT_POSTGRESQL17_GRAMMAR_ONLY','parser':pglast.__version__,
 'postgresqlGrammar':pglast.get_postgresql_version(),'managed':programs,'exactHelpers':len(functions),
 'reverseStatements':len(reverse),'pure':{'statements':len(parsed),'parserPrograms':len(native_programs),
 'sqlBytes':len(result.stdout),'sqlSha256':hashlib.sha256(result.stdout).hexdigest()},
 'databaseConnections':0,'nativeBodyCompilation':'NOT_PROVED','native':'NOT_RUN','restoration':'NOT_RUN'}
(HERE/'GRAMMAR_RESULTS.json').write_bytes((json.dumps(record,indent=2)+'\n').encode('utf-8'))
print(json.dumps({'managedSqlInputs':len(programs),'helpers':len(functions),'reverse':len(reverse),
 'pureStatements':len(parsed),'pureBytes':len(result.stdout),'native':'NOT_RUN'}))
