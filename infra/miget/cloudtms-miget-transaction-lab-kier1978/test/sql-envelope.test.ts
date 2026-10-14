import assert from "node:assert/strict";
import test from "node:test";
import {
  assertOuterTransactionCannotEscape,
  prepareSqlBatch,
  splitSqlStatements,
} from "../src/sql-envelope.ts";

test("splits broad SQL while preserving RPC bodies, comments, and strings", () => {
  const statements = splitSqlStatements(`
    -- semicolon ; in a comment
    create or replace function public.lab_rpc(p_value text)
    returns text language plpgsql as $body$
    begin
      return p_value || ';ok';
    end;
    $body$;
    do $$ begin perform public.lab_rpc('x;y'); end $$;
    select public.lab_rpc('done');
  `);
  assert.equal(statements.length, 3);
  assert.match(statements[0], /create or replace function/i);
  assert.match(statements[1], /^do/i);
  assert.match(statements[2], /^select/i);
});

test("allows DDL, DML, RPC calls, savepoints, and rollback-to-savepoint", () => {
  const statements = prepareSqlBatch(`
    create temporary table probe(id integer);
    insert into probe values (1);
    savepoint caller_probe;
    call public.some_procedure();
    rollback to savepoint caller_probe;
    release savepoint caller_probe;
  `);
  assert.equal(statements.length, 6);
  assert.doesNotThrow(() => assertOuterTransactionCannotEscape(statements));
});

for (const sql of [
  "begin; select 1;",
  "start transaction; select 1;",
  "commit;",
  "end;",
  "rollback;",
  "abort;",
  "prepare transaction 'escape';",
  "commit prepared 'escape';",
  "rollback prepared 'escape';",
  "set transaction isolation level serializable;",
  "set session characteristics as transaction read write;",
]) {
  test(`rejects outer transaction escape: ${sql}`, () => {
    assert.throws(() => prepareSqlBatch(sql), /transaction lab owns the outer transaction/i);
  });
}

test("does not mistake transaction words inside function bodies for transaction control", () => {
  assert.doesNotThrow(() =>
    prepareSqlBatch(`
      create or replace function public.words_only()
      returns text language sql as $$ select 'commit; rollback; prepare transaction'::text $$;
    `),
  );
});

test("rejects incomplete quoted SQL before any database call", () => {
  assert.throws(() => splitSqlStatements("select 'unfinished"), /unclosed string literal/i);
  assert.throws(() => splitSqlStatements("do $tag$ begin null; end;"), /unclosed \$tag\$ body/i);
  assert.throws(() => splitSqlStatements("select 1 /* unfinished"), /unclosed block comment/i);
});
