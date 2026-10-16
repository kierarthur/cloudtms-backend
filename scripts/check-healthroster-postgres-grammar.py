"""Offline grammar only. No DB connection, installation, or binding proof."""
import argparse
import json
import pathlib
import re
import sys

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--parser-path", required=True)
parser.add_argument("files", nargs="+")
args = parser.parse_args()
sys.path.insert(0, args.parser_path)
import pglast

def expressions(node):
    if isinstance(node, dict):
        if "PLpgSQL_expr" in node:
            yield node["PLpgSQL_expr"]
        for value in node.values():
            yield from expressions(value)
    elif isinstance(node, list):
        for value in node:
            yield from expressions(value)

checks = []
failures = []
for name in args.files:
    file = pathlib.Path(name)
    try:
        if file.suffix not in (".sql", ".sqlinc"):
            raise ValueError("SQL source path required")
        sql = re.sub(r"^\\.*$", "", file.read_text(encoding="utf-8"), flags=re.M)
        statements = pglast.parse_sql(sql)
        procedures = pglast.parse_plpgsql(sql)
        expression_count = 0
        for expression in expressions(procedures):
            query = expression["query"]
            mode = expression.get("parseMode", 0)
            if mode == 2:
                query = "SELECT " + query
            elif mode in (3, 4, 5):
                query = "SELECT " + re.split(r":=|=(?!=)", query, maxsplit=1)[1]
            pglast.parse_sql(query)
            expression_count += 1
        checks.append({"file": name, "statements": len(statements),
                       "procedures": len(procedures), "expressions": expression_count})
    except Exception as error:
        failures.append({"file": name, "error": str(error)})
print(json.dumps({"parser": pglast.__version__, "database_execution": False,
                  "checks": checks, "failures": failures}))
sys.exit(bool(failures))
