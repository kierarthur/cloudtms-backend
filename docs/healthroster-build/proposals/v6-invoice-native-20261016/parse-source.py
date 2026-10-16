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
projection_proof = None
try:
    def ctes_for(file):
        sql = file.read_text(encoding="utf-8")
        for expression in expressions(pglast.parse_plpgsql(sql)):
            query = expression["query"]
            if "inserted_lines as (" in query and "line_union" in query:
                tree = json.loads(pglast.parser.parse_sql_json(query))
                statement = next(iter(tree["stmts"][0]["stmt"].values()))
                return {item["CommonTableExpr"]["ctename"]: item["CommonTableExpr"]["ctequery"]
                        for item in statement["withClause"]["ctes"]}
        raise AssertionError("native COMMIT line projection missing")

    def canonical(node):
        if isinstance(node, dict):
            return {key: canonical(value) for key, value in node.items()
                    if key not in ("location", "stmt_location", "stmt_len")}
        if isinstance(node, list):
            return [canonical(value) for value in node]
        return node

    original = ctes_for(root / "reference/private__invoice_generation_advance_core_v8.sqlinc")
    amended = ctes_for(root / "forward/070_generation_native.sqlinc")
    before = original["inserted_lines"]["InsertStmt"]["selectStmt"]["SelectStmt"]["targetList"]
    after = amended["native_planned_lines"]["SelectStmt"]["targetList"][1:]
    before_values = [canonical(item["ResTarget"]["val"]) for item in before]
    after_values = [canonical(item["ResTarget"]["val"]) for item in after]
    assert before_values == after_values, "native intended line operands or rounding changed"
    projection_proof = {"unchanged_native_projection": True, "operand_count": len(before_values),
                        "comparison": "PostgreSQL AST equality ignoring source locations"}
except Exception as error:
    failures.append({"file": "native_projection", "error": str(error)})

result = {"parser": "pglast " + pglast.__version__, "database_execution": False,
          "native_projection": projection_proof,
          "checks": checks, "failures": failures, "native": "REQUIRED_NOT_RUN"}
(root / "SOURCE_PARSE_RESULT.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
print(json.dumps({"files": len(checks), "failures": failures, "native": "REQUIRED_NOT_RUN"}))
sys.exit(bool(failures))
