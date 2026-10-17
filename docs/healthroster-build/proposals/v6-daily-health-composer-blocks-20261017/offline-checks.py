"""Offline grammar/source/DTO checks; writes only local proposal receipts."""
from pathlib import Path
import hashlib, json, re, subprocess, sys

HERE=Path(__file__).resolve().parent
OWN=HERE.parents[3]
INTEGRATION=OWN.parent/'healthroster-install2-14102026'
PARSER=Path(r'D:\Development\Legacy-Work\C-tmp\healthroster-v6-invoice-parser-20261016')
def save(name,value): (HERE/name).write_text(json.dumps(value,indent=2)+'\n',encoding='utf-8')
def run(name,args,cwd):
    completed=subprocess.run(args,cwd=cwd,capture_output=True,text=True,encoding='utf-8')
    save(name,{'command':args,'cwd':str(cwd),'exitCode':completed.returncode,'stdout':completed.stdout,'stderr':completed.stderr,'nativeDatabaseExecuted':False})
    if completed.returncode: raise RuntimeError(name)
run('BLOCK_TEST_RESULTS.json',['node','--test',str(HERE/'blocks.test.mjs')],OWN)
run('CURRENT_TRANSPORT_TEST_RESULTS.json',['node','--test','tests/daily-rate-http.test.mjs','tests/contract-correction-v3.test.mjs'],INTEGRATION)
CHECKLIST=OWN.parent/'healthroster-checklist-native-14102026'
BASIS=CHECKLIST/'docs/healthroster-build/proposals/daily-health-composition-20261017'
PEER=HERE.parent/'v6-daily-health-independent-review-20261017'
run('CURRENT_HEALTH_BASIS_TEST_RESULTS.json',['node','--test',str(BASIS/'source-boundary.test.mjs'),str(BASIS/'reason-roundtrip.test.mjs'),str(PEER/'independent-boundaries.test.mjs')],OWN)
run('ASSEMBLY_STATE.json',['node',str(HERE/'assemble.mjs'),'--check'],OWN)
sys.path.insert(0,str(PARSER))
import pglast
from pglast import ast
entries=[]; forward=[]
for directory,pattern in [('health','*.sqlinc'),('templates','*.sql.tmpl'),('reverse','*.sqlinc')]:
    for file in sorted((HERE/directory).glob(pattern)):
        original=file.read_text(encoding='utf-8')
        # Grammar only: every unavailable native owner block becomes an
        # unconditional exception, never a fabricated successful native proof.
        sql=re.sub(r'@@BANK_(?:REVIEW|ACCEPT|RESULT)@@',"raise exception 'UNBOUND_OWNER_BLOCK_GRAMMAR_ONLY' using errcode='55000';",original)
        parsed=pglast.parse_sql(sql); bodies=pglast.parse_plpgsql(sql)
        entries.append({'file':file.relative_to(HERE).as_posix(),'statements':len(parsed),'plpgsqlBodies':len(bodies),'parse':'PASS','bankOwnerBlocksExpanded':0,'unboundSlotsReplacedWithRefusalForGrammarOnly':directory=='templates','nativeExecuted':False})
        if directory!='reverse':
            functions=[row.stmt for row in parsed if isinstance(row.stmt,ast.CreateFunctionStmt)]
            for function in functions:
                name='.'.join(v.sval for v in function.funcname)
                assert name.startswith('private.daily_rate_correction_')
                options={option.defname:option.arg for option in function.options}
                assert options['security'].boolval is True
                assert 'search_path' in str(options['set'])
                forward.append(name)
        else:
            reverse=['.'.join(v.sval for v in row.stmt.objects[0].objname) for row in parsed]
assert len(forward)==7 and len(set(forward))==7
assert sorted(forward)==sorted(reverse)
save('GRAMMAR_RESULTS.json',{'parserVersion':pglast.__version__,'postgresqlGrammar':pglast.get_postgresql_version(),'programs':entries,'exactAdditiveForwardReverseNamesMatch':True,'newPrivateDefiners':7,'boundBankingOwners':0,'completeRuntimeComposersEmitted':0,'nativeExecuted':False})
print('PASS: offline Health blocks/closed DTO tests and PG17 grammar; genuine Banking joins UNPROVIDED; runtime assembly HELD; native NOT_RUN')
