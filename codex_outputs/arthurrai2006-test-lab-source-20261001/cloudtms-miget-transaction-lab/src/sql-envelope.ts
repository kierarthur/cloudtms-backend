export class SqlEnvelopeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SqlEnvelopeError";
  }
}

type ScanState =
  | { kind: "normal" }
  | { kind: "single" }
  | { kind: "double" }
  | { kind: "line-comment" }
  | { kind: "block-comment"; depth: number }
  | { kind: "dollar"; tag: string };

function dollarTagAt(sql: string, offset: number): string | null {
  const match = sql.slice(offset).match(/^\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$/);
  return match?.[0] ?? null;
}

export function splitSqlStatements(sql: string): string[] {
  const statements: string[] = [];
  let start = 0;
  let state: ScanState = { kind: "normal" };

  for (let index = 0; index < sql.length; index += 1) {
    const char = sql[index];
    const next = sql[index + 1] ?? "";

    if (state.kind === "line-comment") {
      if (char === "\n") state = { kind: "normal" };
      continue;
    }
    if (state.kind === "block-comment") {
      if (char === "/" && next === "*") {
        state.depth += 1;
        index += 1;
      } else if (char === "*" && next === "/") {
        state.depth -= 1;
        index += 1;
        if (state.depth === 0) state = { kind: "normal" };
      }
      continue;
    }
    if (state.kind === "single") {
      if (char === "'" && next === "'") {
        index += 1;
      } else if (char === "\\" && next) {
        index += 1;
      } else if (char === "'") {
        state = { kind: "normal" };
      }
      continue;
    }
    if (state.kind === "double") {
      if (char === "\"" && next === "\"") {
        index += 1;
      } else if (char === "\"") {
        state = { kind: "normal" };
      }
      continue;
    }
    if (state.kind === "dollar") {
      if (sql.startsWith(state.tag, index)) {
        index += state.tag.length - 1;
        state = { kind: "normal" };
      }
      continue;
    }

    if (char === "-" && next === "-") {
      state = { kind: "line-comment" };
      index += 1;
    } else if (char === "/" && next === "*") {
      state = { kind: "block-comment", depth: 1 };
      index += 1;
    } else if (char === "'") {
      state = { kind: "single" };
    } else if (char === "\"") {
      state = { kind: "double" };
    } else if (char === "$") {
      const tag = dollarTagAt(sql, index);
      if (tag) {
        state = { kind: "dollar", tag };
        index += tag.length - 1;
      }
    } else if (char === ";") {
      const statement = sql.slice(start, index + 1).trim();
      if (leadingWords(statement).length > 0) statements.push(statement);
      start = index + 1;
    }
  }

  if (state.kind === "single") throw new SqlEnvelopeError("SQL contains an unclosed string literal");
  if (state.kind === "double") throw new SqlEnvelopeError("SQL contains an unclosed quoted identifier");
  if (state.kind === "block-comment") throw new SqlEnvelopeError("SQL contains an unclosed block comment");
  if (state.kind === "dollar") throw new SqlEnvelopeError(`SQL contains an unclosed ${state.tag} body`);

  const tail = sql.slice(start).trim();
  if (leadingWords(tail).length > 0) statements.push(tail);
  return statements;
}

function leadingWords(statement: string, limit = 4): string[] {
  const words: string[] = [];
  let index = 0;
  while (index < statement.length && words.length < limit) {
    const char = statement[index];
    const next = statement[index + 1] ?? "";
    if (/\s|;/.test(char)) {
      index += 1;
      continue;
    }
    if (char === "-" && next === "-") {
      const newline = statement.indexOf("\n", index + 2);
      index = newline === -1 ? statement.length : newline + 1;
      continue;
    }
    if (char === "/" && next === "*") {
      let depth = 1;
      index += 2;
      while (index < statement.length && depth > 0) {
        if (statement[index] === "/" && statement[index + 1] === "*") {
          depth += 1;
          index += 2;
        } else if (statement[index] === "*" && statement[index + 1] === "/") {
          depth -= 1;
          index += 2;
        } else {
          index += 1;
        }
      }
      continue;
    }
    const word = statement.slice(index).match(/^[A-Za-z_][A-Za-z0-9_$]*/)?.[0];
    if (!word) break;
    words.push(word.toUpperCase());
    index += word.length;
  }
  return words;
}

export function assertOuterTransactionCannotEscape(statements: string[]): void {
  for (let index = 0; index < statements.length; index += 1) {
    const words = leadingWords(statements[index]);
    const first = words[0] ?? "";
    const second = words[1] ?? "";
    const third = words[2] ?? "";
    const rollbackToSavepoint = first === "ROLLBACK" && second === "TO";
    const forbidden =
      first === "BEGIN" ||
      first === "COMMIT" ||
      first === "END" ||
      first === "ABORT" ||
      (first === "ROLLBACK" && !rollbackToSavepoint) ||
      (first === "START" && second === "TRANSACTION") ||
      (first === "PREPARE" && second === "TRANSACTION") ||
      (first === "COMMIT" && second === "PREPARED") ||
      (first === "ROLLBACK" && second === "PREPARED") ||
      (first === "SET" && second === "TRANSACTION") ||
      (first === "SET" && second === "SESSION" && third === "CHARACTERISTICS");
    if (forbidden) {
      throw new SqlEnvelopeError(
        `Statement ${index + 1} starts with transaction control (${words.slice(0, 3).join(" ")}). ` +
          "The transaction lab owns the outer transaction and always rolls it back.",
      );
    }
  }
}

export function prepareSqlBatch(sql: string, maxStatements = 250): string[] {
  const statements = splitSqlStatements(sql);
  if (statements.length === 0) throw new SqlEnvelopeError("SQL must contain at least one statement");
  if (statements.length > maxStatements) {
    throw new SqlEnvelopeError(`SQL is limited to ${maxStatements} statements per rehearsal`);
  }
  assertOuterTransactionCannotEscape(statements);
  return statements;
}
