from pathlib import Path
import sys,json
HERE=Path(__file__).resolve().parent
assert not (HERE/'MANIFEST.json').exists(), 'Frozen history; use a finite successor for new grammar evidence'
PARSER=Path(r'D:\Development\Legacy-Work\C-tmp\healthroster-v6-invoice-parser-20261016')
assert PARSER.is_dir();sys.path.insert(0,str(PARSER))
import pglast
files=['health/00_presentation.sqlinc','health/05_review_enrollment.sqlinc','health/10_revision_apply.sqlinc','health/99_acl.sqlinc','admission/ONE_TIME_ADMISSION.sqlinc','verification/mandatory_security.sql','ADDITIVE_REVERSE.sqlinc']
rows=[]
for f in files:
    sql=(HERE/f).read_text(encoding='utf-8')
    assert '@@BANK_' not in sql
    parsed=pglast.parse_sql(sql);bodies=pglast.parse_plpgsql(sql)
    rows.append({'file':f,'statements':len(parsed),'plpgsqlBodies':len(bodies),'grammar':'PASS','nativeBodyCompilation':'NOT_PROVED'})
result={'parser':pglast.__version__,'postgresqlGrammar':pglast.get_postgresql_version(),'files':rows,'closedHelperDefinitions':5,'heldComposerTemplates':3,'heldComposerGrammar':'NOT_PARSED_UNPROVIDED_BANK_SLOTS','substitutions':[],'native':'NOT_RUN','dbConnections':0}
(HERE/'GRAMMAR_RESULTS.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8');print(json.dumps(result))
