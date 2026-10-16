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
failures = []
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
            procedures = pglast.parse_plpgsql(sql)
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
                       "plpgsql_functions": procedure_count, "inner_sql_expressions": expression_count})
    except Exception as error:
        failures.append({"file": file.name, "error": str(error)})
result = {"parser": "pglast " + pglast.__version__, "database_execution": False,
          "checks": checks, "failures": failures, "native": "REQUIRED_NOT_RUN"}
(root / "SOURCE_PARSE_RESULT.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
print(json.dumps({"files": len(checks), "failures": failures, "native": "REQUIRED_NOT_RUN"}))
sys.exit(bool(failures))
