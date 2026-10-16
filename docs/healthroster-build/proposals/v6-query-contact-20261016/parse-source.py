"""Offline PostgreSQL grammar checks. This never connects to a database."""
import json
import pathlib
import sys
import re

if len(sys.argv) > 1:
    sys.path.insert(0, sys.argv[1])
import pglast

root = pathlib.Path(__file__).resolve().parent
checks = []
fixture_checks = []
failures = []

def grammar_source(sql):
    # Offline libpg_query has no catalog for %ROWTYPE field assignments.
    # RECORD substitutes only the parser's declaration; executable bytes stay
    # unchanged. This does not claim catalog/type binding or native execution.
    return re.sub(r"\b(?:public|private)\.[a-z0-9_]+%rowtype\b", "record", sql, flags=re.I)
def expressions(node):
    if isinstance(node, dict):
        if "PLpgSQL_expr" in node:
            yield node["PLpgSQL_expr"]
        for value in node.values():
            yield from expressions(value)
    elif isinstance(node, list):
        for value in node:
            yield from expressions(value)

for file in sorted((root / "forward").glob("*.sqlinc")):
    sql = file.read_text(encoding="utf-8")
    try:
        statements = pglast.parse_sql(sql)
        procedure_count = 0
        expression_count = 0
        if re.search(r"language\s+plpgsql|language\s+\"plpgsql\"", sql, re.I):
            procedures = pglast.parse_plpgsql(grammar_source(sql))
            procedure_count = len(procedures)
            for expression in expressions(procedures):
                query = expression["query"]
                mode = expression.get("parseMode", 0)
                if mode == 2:
                    query = "SELECT " + query
                elif mode in (3, 4, 5):
                    query = "SELECT " + re.split(r":=|=(?!=)", query, maxsplit=1)[1]
                pglast.parse_sql(query)
                expression_count += 1
        checks.append({"file": file.name, "sql_statements": len(statements),
                       "plpgsql_functions": procedure_count, "inner_sql_expressions": expression_count,
                       "rowtype_parser_alias_only": grammar_source(sql) != sql})
    except Exception as error:
        failures.append({"file": file.name, "error": str(error)})
for file in sorted((root / 'fixtures').glob('*.sqlinc')):
    try:
        sql=file.read_text(encoding='utf-8')
        statements=pglast.parse_sql(sql)
        procedures=pglast.parse_plpgsql(grammar_source(sql))
        count=0
        for expression in expressions(procedures):
            query=expression['query'];mode=expression.get('parseMode',0)
            if mode==2: query='SELECT '+query
            elif mode in(3,4,5): query='SELECT '+re.split(r':=|=(?!=)',query,maxsplit=1)[1]
            pglast.parse_sql(query);count+=1
        fixture_checks.append({'file':'fixtures/'+file.name,'sql_statements':len(statements),
          'plpgsql_functions':len(procedures),'inner_sql_expressions':count,'native':'REQUIRED_NOT_RUN'})
    except Exception as error:
        failures.append({'file':'fixtures/'+file.name,'error':str(error)})
result = {"parser": "pglast " + pglast.__version__, "database_execution": False,
          "fixture_grammar_checks": fixture_checks, "checks": checks, "failures": failures, "native": "REQUIRED_NOT_RUN"}
(root / "SOURCE_PARSE_RESULT.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
print(json.dumps({"files": len(checks), "failures": failures, "native": "REQUIRED_NOT_RUN"}))
sys.exit(bool(failures))
