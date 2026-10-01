import assert from "node:assert/strict";
import test from "node:test";
import { Client } from "pg";
import { runRollbackTransaction } from "../src/transaction-runner.ts";

const databaseUrl = process.env.TRANSACTION_LAB_TEST_DATABASE_URL;

test(
  "replaces an RPC, exercises it with data changes, and restores everything",
  { skip: !databaseUrl },
  async () => {
    const createClient = () => new Client({ connectionString: databaseUrl });
    const setup = createClient();
    await setup.connect();
    await setup.query("drop schema if exists transaction_lab_it cascade");
    await setup.query("create schema transaction_lab_it");
    await setup.query("create table transaction_lab_it.probe(id integer primary key, value text not null)");
    await setup.query(`
      create function transaction_lab_it.probe_rpc()
      returns text language sql as $$ select 'original'::text $$
    `);
    await setup.end();

    try {
      const result = await runRollbackTransaction(createClient, {
        label: "local integration rollback proof",
        sql: `
          create or replace function transaction_lab_it.probe_rpc()
          returns text language sql as $$ select 'variant'::text $$;
          insert into transaction_lab_it.probe(id, value) values (1, 'temporary');
          select transaction_lab_it.probe_rpc() as function_value,
                 (select count(*)::integer from transaction_lab_it.probe) as row_count;
        `,
        verificationSql: `
          select transaction_lab_it.probe_rpc() as function_value,
                 (select count(*)::integer from transaction_lab_it.probe) as row_count;
        `,
        statementTimeoutMs: 10_000,
        lockTimeoutMs: 2_000,
        maxRowsPerStatement: 25,
        continueOnError: false,
      });

      assert.equal(result.accepted, true);
      assert.equal(result.rolledBack, true);
      assert.deepEqual(result.statements[2].rows, [{ function_value: "variant", row_count: 1 }]);
      assert.deepEqual(result.rollbackVerification.statements[0].rows, [
        { function_value: "original", row_count: 0 },
      ]);
    } finally {
      const cleanup = createClient();
      await cleanup.connect();
      await cleanup.query("drop schema if exists transaction_lab_it cascade");
      await cleanup.end();
    }
  },
);

test("continue-on-error uses savepoints and still rolls the whole rehearsal back", { skip: !databaseUrl }, async () => {
  const createClient = () => new Client({ connectionString: databaseUrl });
  const setup = createClient();
  await setup.connect();
  await setup.query("drop schema if exists transaction_lab_continue_it cascade");
  await setup.query("create schema transaction_lab_continue_it");
  await setup.query("create table transaction_lab_continue_it.probe(id integer primary key)");
  await setup.end();

  try {
    const result = await runRollbackTransaction(createClient, {
      label: "local continue-on-error proof",
      sql: `
        insert into transaction_lab_continue_it.probe values (1);
        select 1 / 0;
        insert into transaction_lab_continue_it.probe values (2);
        select count(*)::integer as row_count from transaction_lab_continue_it.probe;
      `,
      verificationSql: "select count(*)::integer as row_count from transaction_lab_continue_it.probe;",
      statementTimeoutMs: 10_000,
      lockTimeoutMs: 2_000,
      maxRowsPerStatement: 25,
      continueOnError: true,
    });

    assert.equal(result.accepted, true);
    assert.equal(result.statements[1].error?.code, "22012");
    assert.deepEqual(result.statements[3].rows, [{ row_count: 2 }]);
    assert.deepEqual(result.rollbackVerification.statements[0].rows, [{ row_count: 0 }]);
  } finally {
    const cleanup = createClient();
    await cleanup.connect();
    await cleanup.query("drop schema if exists transaction_lab_continue_it cascade");
    await cleanup.end();
  }
});
