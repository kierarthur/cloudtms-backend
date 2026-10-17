"""Reproduce read-only peer checks; write receipts only beside this script."""
from pathlib import Path
import hashlib, json, os, re, subprocess, sys

HERE=Path(__file__).resolve().parent
OWN=HERE.parents[3]
CHECKLIST=OWN.parent/'healthroster-checklist-native-14102026'
INTEGRATION=OWN.parent/'healthroster-install2-14102026'
REL='docs/healthroster-build/proposals/daily-health-composition-20261017'
PIN='e938e54f81bb2416d4a281fa025fc8e714c8eb35'
SOURCE=CHECKLIST/REL
PARSER=Path(r'D:\Development\Legacy-Work\C-tmp\healthroster-v6-invoice-parser-20261016')

def digest(data): return hashlib.sha256(data).hexdigest()
def git(root,*args): return subprocess.check_output(['git','-C',str(root),*args])
def save(name,value): (HERE/name).write_text(json.dumps(value,indent=2)+'\n',encoding='utf-8')
def run(name,args,root):
    completed=subprocess.run(args,cwd=root,capture_output=True,text=True,encoding='utf-8')
    receipt={'command':args,'cwd':str(root),'exitCode':completed.returncode,'stdout':completed.stdout,'stderr':completed.stderr,'nativeDatabaseExecuted':False}
    save(name,receipt)
    if completed.returncode: raise RuntimeError(f'{name}: {completed.returncode}')
    return receipt

paths=[]
for root in [OWN,CHECKLIST,INTEGRATION]:
    physical=os.path.realpath(root,strict=True)
    assert physical.lower().startswith('d:\\development\\github\\')
    gd=git(root,'rev-parse','--absolute-git-dir').decode().strip()
    common=git(root,'rev-parse','--path-format=absolute','--git-common-dir').decode().strip()
    assert os.path.realpath(gd,strict=True).lower().startswith('d:\\development\\github\\')
    assert os.path.realpath(common,strict=True).lower().startswith('d:\\development\\github\\')
    paths.append({'root':physical,'gitDir':gd,'commonDir':common,'headObserved':git(root,'rev-parse','HEAD').decode().strip()})

files=[]
for p in sorted(SOURCE.iterdir()):
    actual=p.read_bytes(); committed=git(CHECKLIST,'show',PIN+':'+REL+'/'+p.name)
    assert actual==committed,p.name
    files.append({'file':p.name,'bytes':len(actual),'sha256':digest(actual),'savedEqualsFrozenCommit':True})

run('PACKAGE_TEST_RESULTS.json',['node','--test',REL+'/source-boundary.test.mjs',REL+'/reason-roundtrip.test.mjs'],CHECKLIST)
run('INDEPENDENT_TEST_RESULTS.json',['node','--test',str(HERE/'independent-boundaries.test.mjs')],OWN)
run('CURRENT_TRANSPORT_TEST_RESULTS.json',['node','--test','tests/daily-rate-http.test.mjs','tests/contract-correction-v3.test.mjs'],INTEGRATION)

sys.path.insert(0,str(PARSER))
import pglast
from pglast import ast
grammar=[]
for p in sorted(SOURCE.glob('*.sql')):
    sql=p.read_text(encoding='utf-8')
    # Remove psql controls and replace supplied JSON fixture parameters only for
    # grammar parsing. This does not execute/qualify a native fixture programme.
    sql='\n'.join(line for line in sql.splitlines() if not line.startswith('\\'))
    sql=re.sub(r":'daily_health_(?:test_args|reader_fixture)'","'{}'",sql)
    parsed=pglast.parse_sql(sql)
    bodies=pglast.parse_plpgsql(sql)
    grammar.append({'file':p.name,'statements':len(parsed),'plpgsqlBodies':len(bodies),'parse':'PASS','executed':False})
    if p.name=='17102026_0120_daily_health_composition.sql':
        functions=[s.stmt for s in parsed if isinstance(s.stmt,ast.CreateFunctionStmt)]
        names=['.'.join(v.sval for v in s.funcname) for s in functions]
        assert len(functions)==6 and all(name.startswith('private.daily_rate_correction_health_') for name in names)
        for function in functions:
            options={o.defname:o.arg for o in function.options}
            assert options['security'].boolval is True
            assert 'search_path' in str(options['set'])
        reverse=pglast.parse_sql((SOURCE/'17102026_0120_daily_health_composition_reverse.sql').read_text(encoding='utf-8'))
        drops=['.'.join(v.sval for v in s.stmt.objects[0].objname) for s in reverse]
        assert sorted(drops)==sorted(names) and len(set(drops))==6

save('GRAMMAR_RESULTS.json',{'parserVersion':pglast.__version__,'postgresqlGrammar':pglast.get_postgresql_version(),'programs':grammar,'fullForwardReverseNamesMatch':True,'definerCount':6,'nativeExecuted':False})
guards=json.loads((SOURCE/'SOURCE_GUARDS.json').read_text(encoding='utf-8'))
guardFiles=[]
for item in guards['files']:
    raw=(INTEGRATION/item['file']).read_bytes()
    actual=digest(raw.replace(b'\r\n',b'\n'))
    assert actual==item['canonicalSha256']
    guardFiles.append({'file':item['file'],'canonicalSha256':actual})
for p in SOURCE.iterdir():
    assert p.read_bytes()==git(CHECKLIST,'show',PIN+':'+REL+'/'+p.name)
save('REVIEW_EVIDENCE.json',{'reviewedCommit':PIN,'reviewedFolder':REL,'paths':paths,'packetFiles':files,'actualIntegrationSources':guardFiles,'packageFileCount':len(files),'packetUnchangedAfterChecks':True,'actionableSourceFindings':0,'publicComposers':'INTENTIONALLY_UNIMPLEMENTED_NOT_PASS','nativeProof':'REQUIRED_NOT_RUN','dbDockerReleaseBankingBodies':False})
print('PASS: frozen packet, offline tests, PG17 grammar, exact forward/reverse names, six definer bodies and unchanged read-only source')
