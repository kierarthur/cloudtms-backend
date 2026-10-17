from pathlib import Path
import sys,json,os
HERE=Path(__file__).resolve().parent
assert str(HERE).startswith('D:')
if os.name=='nt': HERE=Path('\\\\?\\'+str(HERE))
assert not (HERE/'MANIFEST.json').exists(), 'Frozen history; use a finite successor'
PARSER=Path(r'D:\Development\Legacy-Work\C-tmp\healthroster-v6-invoice-parser-20261016')
assert PARSER.is_dir();sys.path.insert(0,str(PARSER))
import pglast
files=['reference/current0910/supabase/repeatable/17102026_0910_nhsp_v6_daily_role_identity/01_complete_context.sqlinc','reference/current0910/supabase/repeatable/17102026_0910_nhsp_v6_daily_role_identity/99_acl.sqlinc','reference/current0910/supabase/verification/17102026_0910_daily_role_identity_security.sql']
rows=[]
for f in files:
    sql=(HERE/f).read_text(encoding='utf-8');directives=[]
    if f.endswith('daily_role_identity_security.sql'):
        directive='\\set ON_ERROR_STOP on\n';assert sql.startswith(directive)
        sql=sql[len(directive):];directives=[directive.rstrip('\n')]
    parsed=pglast.parse_sql(sql);bodies=pglast.parse_plpgsql(sql)
    rows.append({'file':f,'statements':len(parsed),'plpgsqlBodies':len(bodies),'grammar':'PASS','removedPsqlDirectives':directives,'nativeBodyCompilation':'NOT_PROVED'})
result={'parser':pglast.__version__,'postgresqlGrammar':pglast.get_postgresql_version(),'files':rows,'composerBodyChanges':0,'substitutions':[],'templates':'NOT_PARSED_UNPROVIDED_BANK_SLOTS','native':'NOT_RUN','dbConnections':0}
(HERE/'CURRENT0910_GRAMMAR.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8');print(json.dumps(result))
