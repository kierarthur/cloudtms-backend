from pathlib import Path
import sys,json
HERE=Path(__file__).resolve().parent
PARSER=Path(r'D:\Development\Legacy-Work\C-tmp\healthroster-v6-invoice-parser-20261016')
assert PARSER.is_dir();sys.path.insert(0,str(PARSER))
import pglast
files=['health/05_review_enrollment.sqlinc','health/95_enrollment_acl.sqlinc','admission/ONE_TIME_ADMISSION.sqlinc','verification/enrollment_security.sql']
rows=[]
for f in files:
    sql=(HERE/f).read_text(encoding='utf-8')
    parsed=pglast.parse_sql(sql);bodies=pglast.parse_plpgsql(sql)
    rows.append({'file':f,'statements':len(parsed),'plpgsqlBodies':len(bodies),'grammar':'PASS','nativeBodyCompilation':'NOT_PROVED'})
result={'parser':pglast.__version__,'postgresqlGrammar':pglast.get_postgresql_version(),'files':rows,'heldComposerGrammar':'UNPROVIDED_BANK_SLOT_NOT_PARSED','substitutions':[],'native':'NOT_RUN','dbConnections':0}
(HERE/'PARTIAL_GRAMMAR.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8');print(json.dumps(result))
