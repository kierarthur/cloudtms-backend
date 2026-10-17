"""Independent PG17 parsing of exact selected Health declarations/FK lock premise.
No SQL executor or database connection. This does not perform catalogue typing.
"""
from pathlib import Path
import hashlib
import json
import subprocess
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
OWN = HERE.parents[3]
PARSER = Path('D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
assert str(OWN.resolve()).replace('\\', '/').startswith('D:/Development/GitHub/')
sys.path.insert(0, str(PARSER))
import pglast
from pglast import ast

def git(path):
    return subprocess.check_output(['git', '-c', 'gc.auto=0', '-c', 'maintenance.auto=false', 'show',
        '84738fe4057d9d693ccd723eb1835106f1091f57:' + path], cwd=OWN)

raw = git('supabase/migrations/16102026_0159_nhsp_v6_01_physical_source_review_schema.sql')
assert hashlib.sha256(raw).hexdigest() == 'd46481d0cd53880041b126c554d34dccc39854ff3bf31ac98883f85949cc1d82'
schema = '\n'.join(line for line in raw.decode().splitlines() if not line.startswith('\\'))
statement = next(item.stmt for item in pglast.parse_sql(schema)
    if isinstance(item.stmt, ast.CreateStmt) and item.stmt.relation.relname == 'timesheet_contract_move_operations')
column = next(item for item in statement.tableElts if isinstance(item, ast.ColumnDef) and item.colname == 'candidate_id')
fk = next(item for item in column.constraints if item.contype.name == 'CONSTR_FOREIGN')
assert fk.pktable.schemaname == 'public' and fk.pktable.relname == 'candidates'
assert not fk.deferrable and not fk.initdeferred
assert [item.sval for item in fk.pk_attrs] == ['id']
programs = []
for file in sorted((HERE / 'native-owners').glob('*.sqlinc')):
    source = file.read_text(encoding='utf-8')
    sql = pglast.parse_sql(source)
    declarations = [item.stmt for item in sql if isinstance(item.stmt, ast.CreateFunctionStmt)]
    assert len(declarations) == 1
    try:
        pl = pglast.parse_plpgsql(source)
        grammar = {'programs': len(pl), 'grammar': 'PASS'}
    except pglast.parser.ParseError as error:
        # Actual catalogue %ROWTYPE assignment cannot be typed by this parser.
        # Keep the error and NOT_PROVED status; no source rewriting/stubbing.
        grammar = {'programs': None, 'grammar': 'NOT_PROVED', 'actualParserError': str(error)}
    programs.append({'file': file.relative_to(HERE).as_posix(), 'statements': len(sql), **grammar,
        'name': '.'.join(item.sval for item in declarations[0].funcname), 'nativeExecuted': False})
assert [item['name'] for item in programs if item['grammar'] == 'NOT_PROVED'] == ['private.timesheet_contract_move_stage_v1']
result = {'parserVersion': pglast.__version__, 'PostgreSQLGrammar': pglast.get_postgresql_version(),
    'schemaRawSha256': hashlib.sha256(raw).hexdigest(),
    'operationCandidateForeignKeyAST': {'referencedRelation': 'public.candidates', 'columns': ['id'],
        'deferrable': bool(fk.deferrable), 'initiallyDeferred': bool(fk.initdeferred)},
    'completeSelectedPrograms': programs, 'native': 'NOT_RUN', 'catalogueTyping': 'NOT_PROVED',
    'deadlockNativeReproduction': 'NOT_RUN'}
(HERE / 'NATIVE_SOURCE_AST.json').write_bytes((json.dumps(result, indent=2) + '\n').encode())
print(json.dumps({'programs': len(programs), 'operationCandidateFK': 'IMMEDIATE_AST_PROVED', 'native': 'NOT_RUN'}))
