"""Grammar only. No database, file sink or business fixture."""
from pathlib import Path
import hashlib
import json
import subprocess
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
PARSER = Path('D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
assert PARSER.is_dir()
sys.path.insert(0, str(PARSER))
import pglast
from pglast import parser

source = (
    "import {buildPureProgram} from " + json.dumps((HERE / 'build-program.mjs').as_uri()) + ";"
    "process.stdout.write(buildPureProgram({root:process.cwd()}).sql);"
)
result = subprocess.run(
    ['node', '--input-type=module', '-e', source], cwd=ROOT,
    capture_output=True, check=True,
)
sql = result.stdout.decode('utf-8')
statements = pglast.parse_sql(sql)
programs = json.loads(parser.parse_plpgsql_json(sql))
assert len(statements) == 7
assert len(programs) == 6
print(json.dumps({
    'status': 'PASS_GRAMMAR_ONLY', 'parser': pglast.__version__,
    'statements': len(statements), 'parserPrograms': len(programs),
    'sqlBytes': len(result.stdout),
    'sqlSha256': hashlib.sha256(result.stdout).hexdigest(),
    'native': 'REQUIRED_NOT_RUN', 'restoration': 'REQUIRED_NOT_RUN',
    'databaseConnections': 0, 'sourceWrites': 0,
}, indent=2))
