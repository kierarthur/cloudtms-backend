"""Offline PG17 outer grammar only; never opens a database connection."""
from pathlib import Path
import argparse
import hashlib
import json
import sys

args = argparse.ArgumentParser()
args.add_argument('--pglast-root', default='C:/tmp/healthroster-v6-invoice-parser-20261016')
options = args.parse_args()
sys.path.insert(0, options.pglast_root)
import pglast
from pglast import parse_sql, parser

version = parser.get_postgresql_version()
if version[0] != 17:
    raise RuntimeError('POSTGRESQL_17_PARSER_REQUIRED')
folder = Path(__file__).resolve().parent
rows = []
for file in sorted((folder / 'harness').rglob('*')):
    if file.is_file() and file.suffix in ('.sql', '.sqlinc'):
        content = file.read_bytes()
        statements = parse_sql(content.decode())
        rows.append({'path': file.relative_to(folder).as_posix(), 'sha256': hashlib.sha256(content).hexdigest(),
                     'statements': len(statements), 'outerSQLGrammar': 'PASS', 'native': 'NOT_RUN'})
result = {'version': 'CONTACT_NATIVE_HARNESS_PG17_OUTER_SQL_GRAMMAR_V1', 'pglast': pglast.__version__,
          'postgresqlParser': list(version), 'passed': len(rows), 'failed': 0, 'rows': rows,
          'limit': 'Outer PostgreSQL grammar only. No installed catalog, PL/pgSQL rowtype binding, first use, behavior, effect or native qualification.',
          'native': 'NOT_RUN'}
(folder / 'HARNESS_GRAMMAR.json').write_text(json.dumps(result, indent=2) + '\n', newline='\n')
print(json.dumps({'outerSQLFiles': len(rows), 'postgresqlParser': list(version), 'failed': 0, 'native': 'NOT_RUN'}))
