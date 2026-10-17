"""Exact PostgreSQL 17 grammar only; no server, catalogue or SQL execution."""
from pathlib import Path
import sys, json, re, hashlib, os
sys.path.insert(0, 'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast

here = Path(__file__).resolve().parent.parent
repo = here.parents[3]
root = repo.parent / 'healthroster-install2-14102026'

def raw(path):
    return Path(('\\\\?\\' + str(path)) if os.name == 'nt' else path).read_bytes()

pins = json.loads(raw(here / 'SOURCE_PINS.json'))
files = {target: row['path'] for target, row in pins['selectedTargets'].items()
         if re.search(r'\.sql(?:inc)?$', target, re.I)}
rows = []
for target, source in files.items():
    data = raw(root / source)
    sql = re.sub(r'^\\[^\r\n]*', '', data.decode(), flags=re.M)
    statements = pglast.parse_sql(sql)
    programs = 0
    for statement in statements:
        if type(statement.stmt).__name__ in ('CreateFunctionStmt', 'DoStmt'):
            start = statement.stmt_location
            text = sql[start:start+statement.stmt_len] if statement.stmt_len else sql[start:]
            programs += len(pglast.parse_plpgsql(text))
    rows.append(dict(target=target, source=source, sha256=hashlib.sha256(data).hexdigest(),
                     statements=len(statements), exactPrograms=programs,
                     routineDefinitions=len(re.findall(r'create\s+or\s+replace\s+function\b', sql, re.I))))
result = dict(parser=pglast.__version__, PostgreSQLGrammar='17.7', inputs=rows,
              statements=sum(r['statements'] for r in rows), exactPrograms=sum(r['exactPrograms'] for r in rows),
              routineDefinitions=sum(r['routineDefinitions'] for r in rows),
              grammarOnly=True, native='NOT_RUN', catalogue='NOT_RUN', dbConnections=0)
(here / 'GRAMMAR_RESULTS.json').write_text(json.dumps(result, indent=2)+'\n', encoding='utf8', newline='\n')
print(json.dumps({k: result[k] for k in ['statements', 'exactPrograms', 'routineDefinitions', 'native']}))
