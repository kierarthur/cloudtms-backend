// WP-18b: the installed-writer census verifier's escape shapes, as permanent
// bidirectional fixtures.
//
// HANDOVER 2 round-5 Part G ruled H2-040 NOT SATISFIED and named this verifier:
// "A verifier that misses genuine new writers cannot support a release
// guarantee. Repair it, add the twelve demonstrated escape shapes as
// negative/positive fixtures, prove they are detected, rerun it against the
// candidate and obtain independent review."
//
// WHAT THIS FILE IS. Every shape below is a real SQL statement that genuinely
// voids an item, terminalises or cancels a batch, releases a reservation, or
// removes evidence rows -- and that some version of
// supabase/verification/17092026_0900_weekly_source_installed_writer_census_v1.sql
// passed with ok:true. TWENTY-EIGHT demonstrated defects are pinned here, from
// three rounds of independent review and the repairs between them:
//
//   round 1 (reports/WP-18-19_REVIEW.md F1/F4/F5) : 12  -- M01-M10, N03, drop hazard
//   found while repairing round 1                 :  4  -- X01-X04
//   round 2 (reports/WP-18b_REVIEW.md F1/F2)      :  6  -- Y01-Y06
//   round 3 (reports/WP-18b_REVIEW_2.md G1/G2/G3) :  6  -- G1a-G1d, G2, G3
//
// Twenty-six of the twenty-eight are escapes, where a genuine evidence write
// passed the gate. One (N03) is a false block on a harmless comment. One is the
// drop hazard, where a file calling itself READ-ONLY destroyed permanent tables.
//
// Each shape is pinned in BOTH directions, which is the point of the ruling:
//
//   POSITIVE  the genuine evidence write. The verifier MUST fail.
//   NEGATIVE  the SAME syntax with a harmless effect -- the census column only
//             read in a predicate, or an unrelated column assigned. The verifier
//             MUST pass. Without these, a scan could pass every positive by
//             being indiscriminate, and would then block the release on
//             ordinary code.
//
// Expected, and asserted: 53 of 53 against the current verifier -- 33 positive
// (32 shapes plus the drop-hazard fixture) and 20 negative.
//
// The before-and-after that HANDOVER 2 round-5 Part G asks for. Reproduce each by
// pointing WP18B_VERIFIER at the earlier copy:
//
//   pre-WP-18b        (SHA 72df38d9...) : lowest
//   post-round-1      (SHA 42d5bf4d...) : the six Y shapes escape
//   post-round-2      (SHA 4e06aa6d...) : 46 of 53 -- the seven G shapes escape
//   current, round 3  (SHA 3e5a2ff6...) : 53 of 53, exit 0
//
// NOTE on M08b, which moved from the negative set to the positive set in round 3.
// It asserted that dynamic SQL naming no evidence table must not block. Review 2
// finding G1c falsified that: the table name can arrive in a variable. Section 8d
// now refuses ALL unacknowledged dynamic SQL, so it correctly blocks. The cost is
// real -- every new routine that executes run-time SQL blocks the release until
// it is classified or acknowledged -- and it is stated here rather than hidden.
//
// WHAT THIS SUITE CANNOT DO. It proves that the shapes people have thought of are
// caught. It cannot prove there are no others, and round 3 established why: an
// installed routine -- public.codex_debug_exec_sql, SECURITY DEFINER, granted
// EXECUTE to service_role -- takes the SQL to run AS A PARAMETER and will perform
// any evidence write it is asked to. Proved by execution on a disposable clone
// inside a rolled-back transaction. See reports/WP-18b_REPORT.md sections 6c
// and 12.
//
// Every fixture is installed inside BEGIN ... ROLLBACK, so nothing durable is
// written to the target database. The drop-hazard fixture is the exception: it
// creates and then removes five PERMANENT tables of its own.
//
// Because of that exception this driver is NOT safe to run twice concurrently
// against the same database -- two runs will drop each other's sentinels and the
// drop-hazard fixture will report a false failure. Give each concurrent run its
// own clone.
//
// How to run (local Docker only; never a hosted database):
//
//   export MSYS_NO_PATHCONV=1 PGOPTIONS='-c jit=off'
//   export PSQL_BIN='C:\Program Files\PostgreSQL\18\bin\psql.exe'
//   export WP18B_DB=<a disposable clone of a full build>
//   node tests/weekly-source/wp18b-installed-writer-census-fixtures.mjs
//
// Optional:
//   WP18B_URL       full connection URL (overrides WP18B_DB)
//   WP18B_VERIFIER  path to the verifier under test, to run the suite against an
//                   older copy and reproduce the baselines above
//
// Exit code 0 means every positive was detected and every negative was clean.

import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const PSQL = process.env.PSQL_BIN || 'psql';
const DB = process.env.WP18B_DB;
const URL = process.env.WP18B_URL
  || (DB ? `postgresql://postgres:localonly@127.0.0.1:55433/${DB}` : null);
const VERIFIER = process.env.WP18B_VERIFIER
  || path.join(process.cwd(), 'supabase/verification/17092026_0900_weekly_source_installed_writer_census_v1.sql');

if (!URL) {
  console.error('Set WP18B_DB (a disposable local clone) or WP18B_URL. Never point this at a hosted database.');
  process.exit(2);
}
if (!/127\.0\.0\.1|localhost|::1/.test(URL)) {
  console.error('Refusing to run: WP18B_URL is not a local address.');
  process.exit(2);
}
if (!fs.existsSync(VERIFIER)) {
  console.error(`Verifier not found: ${VERIFIER}`);
  process.exit(2);
}

const verifierSql = fs.readFileSync(VERIFIER, 'utf8');
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'wp18b-fx-'));
const results = [];

function record(id, ok, detail) {
  results.push({ id, ok, detail });
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${id.padEnd(50)} ${detail}`);
}

function runSql(text) {
  const file = path.join(tmp, `run-${Date.now()}-${Math.random().toString(36).slice(2)}.sql`);
  fs.writeFileSync(file, text, 'utf8');
  try {
    return execFileSync(PSQL, [URL, '-X', '-v', 'ON_ERROR_STOP=1', '-f', file],
      { encoding: 'utf8', maxBuffer: 256 * 1024 * 1024, stdio: ['ignore', 'pipe', 'pipe'] });
  } catch (error) {
    return `${error.stdout ?? ''}${error.stderr ?? ''}`;
  } finally {
    fs.rmSync(file, { force: true });
  }
}

// ---------------------------------------------------------------------------
// The fixtures.
// ---------------------------------------------------------------------------
const FIXTURES = [
  {
    id: "C01_control_plain_void",
    expect: "DETECTED",
    why: "Control. The textbook shape: a plain unconditional void. Detected by every version of the scan.",
    sql: `
create or replace function public.wp18rev_c01(p_id uuid) returns void language plpgsql as $$
begin
  update public.pay_batch_items set is_voided = true where id = p_id;
end $$;
`,
  },
  {
    id: "C02_control_multicolumn_set",
    expect: "DETECTED",
    why: "Control. The multi-column \"set (a, b) = (...)\" assignment form, tested on the unflattened slice.",
    sql: `
create or replace function public.wp18rev_c02(p_id uuid) returns void language plpgsql as $$
begin
  update public.pay_advance_reservations set (status, released_reason) = ('RELEASED', 'X') where id = p_id;
end $$;
`,
  },
  {
    id: "C03_control_cte_update",
    expect: "DETECTED",
    why: "Control. An UPDATE fed by a CTE, so the statement does not begin with the verb.",
    sql: `
create or replace function public.wp18rev_c03(p_id uuid) returns void language plpgsql as $$
begin
  with v as (select p_id as id)
  update public.pay_batch_items i set is_voided = true from v where i.id = v.id;
end $$;
`,
  },
  {
    id: "C04_control_literal_semicolon_after",
    expect: "DETECTED",
    why: "Control. A semicolon inside a literal AFTER the census column, which the original slice survived. Pairs with M02, where the literal comes first and it did not.",
    sql: `
create or replace function public.wp18rev_c04(p_id uuid) returns void language plpgsql as $$
begin
  update public.pay_batch_items set is_voided = true, description = 'a;b' where id = p_id;
end $$;
`,
  },
  {
    id: "C05_control_delete",
    expect: "DETECTED",
    why: "Control. A plain hard delete of evidence rows.",
    sql: `
create or replace function public.wp18rev_c05(p_id uuid) returns void language plpgsql as $$
begin
  delete from public.pay_advance_reservations where id = p_id;
end $$;
`,
  },
  {
    id: "G1a_dynamic_single_quoted_execute",
    expect: "DETECTED",
    why: "WP-18b-R3 shape 1, from WP-18b_REVIEW_2.md G1 (CRITICAL). The commonest form of dynamic SQL there is. Stage 1b masks the string literal away before the scan runs, so section 2 never sees the write; the old 8d fired only on a format() placeholder, so nothing refused it either. It is now REFUSED, not understood: section 8d fails closed on any unacknowledged routine that executes run-time SQL.",
    sql: `
create or replace function public.wp18b_g1a(p_id uuid) returns void language plpgsql as $$
begin
  execute 'update public.pay_batch_items set is_voided = true where id = $1' using p_id;
end $$;
`,
  },
  {
    id: "G1b_dynamic_concatenated_table_name",
    expect: "DETECTED",
    why: "WP-18b-R3 shape 2. The table name is split across a concatenation, so the raw definition never contains the token \"pay_batch_items\" at all. No text scan can resolve this; refusal is the only sound answer.",
    sql: `
create or replace function public.wp18b_g1b(p_id uuid) returns void language plpgsql as $$
begin
  execute 'update public.pay_batch' || '_items set is_voided = true where id = $1' using p_id;
end $$;
`,
  },
  {
    id: "G1c_dynamic_table_name_in_variable",
    expect: "DETECTED",
    why: "WP-18b-R3 shape 3. The table name arrives through a variable.",
    sql: `
create or replace function public.wp18b_g1c(p_id uuid) returns void language plpgsql as $$
declare v_t text := 'pay_batch_items';
begin
  execute 'update public.' || v_t || ' set is_voided = true where id = $1' using p_id;
end $$;
`,
  },
  {
    id: "G1d_dynamic_delete",
    expect: "DETECTED",
    why: "WP-18b-R3 shape 4. The same against evidence removal.",
    sql: `
create or replace function public.wp18b_g1d(p_id uuid) returns void language plpgsql as $$
begin
  execute 'delete from public.pay_advance_reservations where id = $1' using p_id;
end $$;
`,
  },
  {
    id: "G2_updatable_view_other_schema",
    expect: "DETECTED",
    why: "WP-18b-R3 shape 5, from WP-18b_REVIEW_2.md G2 (HIGH). An auto-updatable view over the evidence table, in a schema that is neither public nor private. PostgreSQL passes the write through to pay_batch_items. Stage 0 used to resolve views only in public/private, so the routine named no known target and the pre-filter skipped it entirely. The schema restriction is now gone.",
    sql: `
create schema if not exists wp18b_reporting;
create or replace view wp18b_reporting.bi_items as select * from public.pay_batch_items;
create or replace function public.wp18b_g2(p_id uuid) returns void language plpgsql as $$
begin
  update wp18b_reporting.bi_items set is_voided = true where id = p_id;
end $$;
`,
  },
  {
    id: "G3_inheritance_parent",
    expect: "DETECTED",
    why: "WP-18b-R3 shape 6, from WP-18b_REVIEW_2.md G3 (HIGH). An inheritance parent whose name contains no evidence-table token. UPDATE on the parent rewrites the child's rows, and the child's name never appears in the statement. Stage 0 now resolves inheritance and partition ancestry transitively.",
    sql: `
create table if not exists public.wp18b_ev_umbrella(id uuid, is_voided boolean);
alter table public.pay_batch_items inherit public.wp18b_ev_umbrella;
create or replace function public.wp18b_g3(p_id uuid) returns void language plpgsql as $$
begin
  update public.wp18b_ev_umbrella set is_voided = true where id = p_id;
end $$;
`,
  },
  {
    id: "M01_subquery_before_is_voided",
    expect: "DETECTED",
    why: "Review F1 M01. A subquery in an EARLIER SET expression. The original scan took the SET list as everything up to the first \"from\"/\"where\", and both appear inside the subquery, so the list was truncated before is_voided. The commonest shape in this codebase and the most dangerous. Fixed by flattening parentheses innermost-first before reading the SET list.",
    sql: `
create or replace function public.wp18rev_m01(p_id uuid) returns void language plpgsql as $$
begin
  update public.pay_batch_items
     set amount_inc_vat = (select coalesce(sum(x.amount_inc_vat), 0) from public.pay_batch_items x where x.id = p_id),
         is_voided = true
   where id = p_id;
end $$;
`,
  },
  {
    id: "M02_semicolon_in_literal_before_is_voided",
    expect: "DETECTED",
    why: "Review F1 M02. A semicolon inside a string literal ahead of the census column. The original statement slice was [^;]* over unmasked text, so it stopped inside the literal. Fixed by masking literals before slicing.",
    sql: `
create or replace function public.wp18rev_m02(p_id uuid) returns void language plpgsql as $$
begin
  update public.pay_batch_items
     set description = 'superseded; rebuilt by correction',
         is_voided = true
   where id = p_id;
end $$;
`,
  },
  {
    id: "M03_semicolon_in_comment",
    expect: "DETECTED",
    why: "Review F1 M03. A semicolon inside a block comment between the verb and the SET list. Same truncation as M02. Fixed by masking comments before slicing.",
    sql: `
create or replace function public.wp18rev_m03(p_id uuid) returns void language plpgsql as $$
begin
  update public.pay_batch_items /* step 2; void the item */
     set is_voided = true
   where id = p_id;
end $$;
`,
  },
  {
    id: "M04_quoted_identifiers",
    expect: "DETECTED",
    why: "Review F1 M04. update \"public\".\"pay_batch_items\" set \"is_voided\" = true. The original verb/table regex did not match quoted identifiers at all, so no slice was produced. Fixed by stripping identifier double-quotes during masking.",
    sql: `
create or replace function public.wp18rev_m04(p_id uuid) returns void language plpgsql as $$
begin
  update "public"."pay_batch_items" set "is_voided" = true where id = p_id;
end $$;
`,
  },
  {
    id: "M05_comment_between_verb_and_table",
    expect: "DETECTED",
    why: "Review F1 M05. A comment sitting between the verb and the table name broke the \\s+ between them. Fixed because masking turns the comment into a space.",
    sql: `
create or replace function public.wp18rev_m05(p_id uuid) returns void language plpgsql as $$
begin
  update /* items */ public.pay_batch_items set is_voided = true where id = p_id;
end $$;
`,
  },
  {
    id: "M06_terminalise_batch_after_subquery",
    expect: "DETECTED",
    why: "Review F1 M06. A SECOND TERMINALITY OWNER, hidden behind a subquery: it sets status to SETTLED and stamps completed_at_utc. It escaped checks 6a, 6b and 8 SIMULTANEOUSLY against the original file. Now caught by 6b, which admits no other writer of completed_at_utc and does not depend on a literal.",
    sql: `
create or replace function public.wp18rev_m06(p_id uuid) returns void language plpgsql as $$
begin
  update public.pay_batches b
     set total_bank_out = (select coalesce(sum(i.amount_inc_vat), 0)
                             from public.pay_batch_items i
                             join public.pay_batch_candidates c on c.id = i.pay_batch_candidate_id
                            where c.pay_batch_id = b.id),
         status = 'SETTLED',
         completed_at_utc = now()
   where b.id = p_id;
end $$;
`,
  },
  {
    id: "M07_truncate_reservations",
    expect: "DETECTED",
    why: "Review F1 M07. TRUNCATE removes every evidence row at once and was not scanned at all: the delete scan looked only for \"delete from\". Caught by the new section 8c.",
    sql: `
create or replace function public.wp18rev_m07() returns void language plpgsql as $$
begin
  truncate table public.pay_advance_reservations;
end $$;
`,
  },
  {
    id: "M08_dynamic_table_name",
    expect: "DETECTED",
    why: "Review F1 M08. The target relation is built at run time with format(...%I...), so no text scan can resolve it. Caught by the new section 8d, which fails closed on EXECUTE with a format placeholder in a routine naming an evidence relation.",
    sql: `
create or replace function public.wp18rev_m08(p_id uuid) returns void language plpgsql as $$
begin
  execute format('update public.%I set is_voided = true where id = $1', 'pay_batch_items') using p_id;
end $$;
`,
  },
  {
    id: "M08b_dynamic_sql_no_evidence_table_now_refused",
    expect: "DETECTED",
    why: "RECLASSIFIED from negative to positive in round 3, and the reclassification is the point. This routine executes format('update public.%I set updated_at_utc = now()', p_t) and names no evidence relation anywhere in its text. Until round 3 it was a NEGATIVE fixture, asserting that dynamic SQL which does not mention an evidence table must not block the release. That assumption was false, and WP-18b_REVIEW_2.md G1c is the proof: p_t can be 'pay_batch_items'. A routine that builds an identifier at run time can target anything, and no scan of its text can say otherwise. Section 8d now refuses all unacknowledged dynamic SQL, so this case blocks the release -- correctly. The cost is real and is stated rather than hidden: every new routine that executes run-time SQL now blocks the release until it is classified or acknowledged. Twenty-two existing routines are acknowledged by identity and definition hash in section 8d.",
    sql: `
create or replace function public.wp18b_m08b(p_t text) returns void language plpgsql as $$
begin
  execute format('update public.%I set updated_at_utc = now()', p_t);
end $$;
`,
  },
  {
    id: "M09_cancel_batch_after_subquery",
    expect: "DETECTED",
    why: "Review F1 M09. A FIFTH CANCELLATION-STAMP OWNER behind a subquery, escaping 6c and 8 together. Ruling 5 names exactly four. Now caught by 6c.",
    sql: `
create or replace function public.wp18rev_m09(p_id uuid) returns void language plpgsql as $$
begin
  update public.pay_batches
     set cancel_reason = (select string_agg(x.description, ', ') from public.pay_batch_items x where x.id = p_id),
         status = 'CANCELLED',
         cancelled_at_utc = now()
   where id = p_id;
end $$;
`,
  },
  {
    id: "M10_release_reservation_after_subquery",
    expect: "DETECTED",
    why: "Review F1 M10. A reservation released to WRITE_OFF behind a subquery. Same truncation as M01. Ruling 5 correction 5 turns on exactly this evidence.",
    sql: `
create or replace function public.wp18rev_m10(p_id uuid, p_actor uuid) returns void language plpgsql as $$
begin
  update public.pay_advance_reservations r
     set updated_by_user_id = (select u.id from (select p_actor as id) u where u.id is not null),
         status = 'RELEASED',
         released_reason = 'WRITE_OFF'
   where r.id = p_id;
end $$;
`,
  },
  {
    id: "X01_on_conflict_do_update",
    expect: "DETECTED",
    why: "WP-18b own shape 1. An upsert whose UPDATE half voids the item. ON CONFLICT ... DO UPDATE SET is the ordinary idempotent-writer pattern in this codebase (two installed routines already use it on evidence tables). The verb \"update\" is not followed by a table name, and is_voided is not in the INSERT column list, so neither the UPDATE scan nor the INSERT scan sees it.",
    sql: `
create or replace function public.wp18b_x01(p_id uuid) returns void language plpgsql as $$
begin
  insert into public.pay_batch_items (id, pay_batch_candidate_id)
  values (p_id, p_id)
  on conflict (id) do update
     set is_voided = true;
end $$;
`,
  },
  {
    id: "X02_nested_dollar_quoted_execute",
    expect: "DETECTED",
    why: "WP-18b own shape 2. Dynamic SQL held in a NESTED dollar-quoted string, with no format() placeholder, so section 8d's %I/%s test never fires. This shape defeats the review's own prototype, whose mask blanked nested dollar quotes outright; it is a regression that the fix itself would have introduced.",
    sql: `
create or replace function public.wp18b_x02(p_id uuid) returns void language plpgsql as $$
begin
  execute $q$ update public.pay_batch_items set is_voided = true where id = $q$ || quote_literal(p_id);
end $$;
`,
  },
  {
    id: "X03_write_through_updatable_view",
    expect: "DETECTED",
    why: "WP-18b own shape 3. A write through an auto-updatable view over the evidence table. PostgreSQL passes the UPDATE through to pay_batch_items, so the item is genuinely voided, but the base table name never appears in the statement.",
    sql: `
create or replace view public.wp18b_x03_view as select * from public.pay_batch_items;
create or replace function public.wp18b_x03(p_id uuid) returns void language plpgsql as $$
begin
  update public.wp18b_x03_view set is_voided = true where id = p_id;
end $$;
`,
  },
  {
    id: "X04_whitespace_around_schema_dot",
    expect: "DETECTED",
    why: "WP-18b own shape 4. Whitespace around the schema qualifier. Legal SQL; the original table regex requires \"public.\" to be adjacent to the table name.",
    sql: `
create or replace function public.wp18b_x04(p_id uuid) returns void language plpgsql as $$
begin
  update public . pay_batches
     set status = 'CANCELLED', cancelled_at_utc = now()
   where id = p_id;
end $$;
`,
  },
  {
    id: "Y01_unicode_escape_identifier_update",
    expect: "DETECTED",
    why: "WP-18b-R2 shape 1, from WP-18b_REVIEW.md F1 (CRITICAL). PostgreSQL's Unicode-escape identifier U&\"...\" is a standard, documented quoted-identifier form that resolves to exactly the real table. The previous scan stripped the quotes but left the U& glued to the name, which broke the verb/table adjacency it required, so no slice was produced and this genuine void passed the gate.",
    sql: `
create or replace function public.wp18b_y01(p_id uuid) returns void language plpgsql as $$
begin
  update U&"pay_batch_items" set is_voided = true where id = p_id;
end $$;
`,
  },
  {
    id: "Y02_unicode_escape_schema_qualified",
    expect: "DETECTED",
    why: "WP-18b-R2 shape 2. The same bypass with the schema spelled out.",
    sql: `
create or replace function public.wp18b_y02(p_id uuid) returns void language plpgsql as $$
begin
  update public.U&"pay_batch_items" set is_voided = true where id = p_id;
end $$;
`,
  },
  {
    id: "Y03_unicode_escape_delete",
    expect: "DETECTED",
    why: "WP-18b-R2 shape 3. The same bypass against the DELETE scan, which in the reviewed version read raw text and so was defeated the same way.",
    sql: `
create or replace function public.wp18b_y03(p_id uuid) returns void language plpgsql as $$
begin
  delete from U&"pay_advance_reservations" where id = p_id;
end $$;
`,
  },
  {
    id: "Y04_unicode_escape_truncate",
    expect: "DETECTED",
    why: "WP-18b-R2 shape 4. The same bypass against TRUNCATE.",
    sql: `
create or replace function public.wp18b_y04() returns void language plpgsql as $$
begin
  truncate table U&"pay_batch_items";
end $$;
`,
  },
  {
    id: "Y05_rule_writes_evidence",
    expect: "DETECTED",
    why: "WP-18b-R2 shape 5, from WP-18b_REVIEW.md F2 (HIGH). A rewrite RULE whose action voids items. A rule's action lives in pg_rewrite.ev_action, not pg_proc, so the routine scan structurally cannot see it: a whole class of installed writer that was neither scanned nor disclosed. Caught now by section 11a.",
    sql: `
create table if not exists public.wp18b_y05_shim(id uuid primary key);
create or replace rule wp18b_y05_void as on insert to public.wp18b_y05_shim
  do also update public.pay_batch_items set is_voided = true where id = new.id;
`,
  },
  {
    id: "Y06_unicode_escape_with_body_escapes",
    expect: "DETECTED",
    why: "WP-18b-R2 shape 6. A Unicode-escape identifier whose BODY carries a backslash escape: U&\"pay_\\0062atch_items\" resolves to pay_batch_items, but the relation name is never spelled in the text, so no lexical scan can resolve it. This must FAIL CLOSED (section 8f), not be understood. It is the honest boundary of the method, and it blocks the release rather than passing it.",
    sql: `
create or replace function public.wp18b_y06(p_id uuid) returns void language plpgsql as $$
begin
  update U&"pay_\\0062atch_items" set is_voided = true where id = p_id;
end $$;
`,
  },
  {
    id: "N02_negative_control_reader",
    expect: "CLEAN",
    why: "Negative control. Reads is_voided, status and cancelled_at_utc in predicates while writing an unrelated pay_batches column. Passed before and after; it is the precision side, and it must stay clean or ordinary code blocks the release.",
    sql: `
create or replace function public.wp18rev_n02(p_id uuid) returns void language plpgsql as $$
begin
  -- reads is_voided, status and cancelled_at_utc in predicates; writes an unrelated column only
  update public.pay_batches
     set last_status_checked_at_utc = now()
   where id = p_id
     and status = 'DRAFT'
     and cancelled_at_utc is null
     and exists (select 1 from public.pay_batch_items i
                  where i.pay_batch_candidate_id is not null and i.is_voided = false);
end $$;
`,
  },
  {
    id: "N03_negative_control_unrelated",
    expect: "CLEAN",
    why: "Review F4. An evidence write that exists ONLY inside a comment. The original file FLAGGED it, a false release block on a harmless comment, and it showed the \"statement-scoped\" narrative was textual rather than lexical. Clean now that comments are masked.",
    sql: `
create or replace function public.wp18rev_n03(p_x integer) returns integer language plpgsql as $$
begin
  return p_x * 2; -- 'update public.pay_batch_items set is_voided = true' only inside this comment
end $$;
`,
  },
  {
    id: "N04_negative_control_dollar_message",
    expect: "CLEAN",
    why: "WP-18b negative control. A routine that names an evidence table only in a dollar-quoted message string and reads it in a predicate. Must NOT be flagged.",
    sql: `
create or replace function public.wp18b_n04(p_id uuid) returns integer language plpgsql as $$
declare v_n integer;
begin
  select count(*) into v_n from public.pay_batch_items where id = p_id and is_voided = false;
  if v_n = 0 then
    raise exception $m$no live rows in pay_batch_items for this id$m$;
  end if;
  return v_n;
end $$;
`,
  },
  {
    id: "NG1_dynamic_execute_read_only",
    expect: "CLEAN",
    why: "Paired negative for G1a-G1d. A routine that executes run-time SQL but is ACKNOWLEDGED in section 8d with a matching definition hash must not block. public.codex_debug_select_sql is one of the twenty-two acknowledged routines and is installed unchanged here, so the release must pass. This fixture pins the acknowledgement mechanism itself: if the pin list or its hashes drift, it fails.",
    sql: `
select 1;
`,
  },
  {
    id: "NG2_view_other_schema_read_only",
    expect: "CLEAN",
    why: "Paired negative for G2. An auto-updatable view over the evidence table in another schema, READ only. Widening stage 0 to all schemas must not turn every mention of such a view into a write.",
    sql: `
create schema if not exists wp18b_reporting_ro;
create or replace view wp18b_reporting_ro.bi_items as select * from public.pay_batch_items;
create or replace function public.wp18b_ng2(p_id uuid) returns integer language plpgsql as $$
declare v_n integer;
begin
  select count(*) into v_n from wp18b_reporting_ro.bi_items where id = p_id and is_voided = false;
  return v_n;
end $$;
`,
  },
  {
    id: "NG3_inheritance_parent_read_only",
    expect: "CLEAN",
    why: "Paired negative for G3. An inheritance parent that is only READ from.",
    sql: `
create table if not exists public.wp18b_ev_umbrella_ro(id uuid, is_voided boolean);
alter table public.pay_batch_items inherit public.wp18b_ev_umbrella_ro;
create or replace function public.wp18b_ng3(p_id uuid) returns integer language plpgsql as $$
declare v_n integer;
begin
  select count(*) into v_n from public.wp18b_ev_umbrella_ro where id = p_id and is_voided = false;
  return v_n;
end $$;
`,
  },
  {
    id: "NM01_subquery_before_unrelated_column",
    expect: "CLEAN",
    why: "Paired negative for M01. Same shape: a subquery earlier in the SET list. The assigned column is NOT census-relevant and is_voided is only READ, in the subquery's predicate. Must NOT be flagged.",
    sql: `
create or replace function public.wp18b_nm01(p_id uuid) returns void language plpgsql as $$
begin
  update public.pay_batch_items
     set amount_inc_vat = (select coalesce(sum(x.amount_inc_vat), 0)
                             from public.pay_batch_items x
                            where x.id = p_id and x.is_voided = false)
   where id = p_id;
end $$;
`,
  },
  {
    id: "NM02_semicolon_in_literal_unrelated_column",
    expect: "CLEAN",
    why: "Paired negative for M02. Semicolon inside a string literal; no census column assigned. Must NOT be flagged.",
    sql: `
create or replace function public.wp18b_nm02(p_id uuid) returns void language plpgsql as $$
begin
  update public.pay_batch_items
     set description = 'superseded; rebuilt by correction'
   where id = p_id and is_voided = true;
end $$;
`,
  },
  {
    id: "NM03_semicolon_in_comment_unrelated_column",
    expect: "CLEAN",
    why: "Paired negative for M03. Semicolon inside a comment; no census column assigned.",
    sql: `
create or replace function public.wp18b_nm03(p_id uuid) returns void language plpgsql as $$
begin
  update public.pay_batch_items /* step 2; touch the description only */
     set description = 'x'
   where id = p_id;
end $$;
`,
  },
  {
    id: "NM04_quoted_identifiers_unrelated_column",
    expect: "CLEAN",
    why: "Paired negative for M04. Quoted identifiers throughout; no census column assigned, is_voided read in the predicate only.",
    sql: `
create or replace function public.wp18b_nm04(p_id uuid) returns void language plpgsql as $$
begin
  update "public"."pay_batch_items" set "description" = 'x'
   where "id" = p_id and "is_voided" = false;
end $$;
`,
  },
  {
    id: "NM05_comment_between_verb_and_table_unrelated",
    expect: "CLEAN",
    why: "Paired negative for M05. Comment between the verb and the table name; no census column assigned.",
    sql: `
create or replace function public.wp18b_nm05(p_id uuid) returns void language plpgsql as $$
begin
  update /* items */ public.pay_batch_items set description = 'x' where id = p_id;
end $$;
`,
  },
  {
    id: "NM06_subquery_then_non_terminal_status",
    expect: "CLEAN",
    why: "Paired negative for M06. The same subquery-first shape on pay_batches, but the only assigned column (total_bank_out) is not census-relevant and neither status nor completed_at_utc is written. The flattening that makes M06 detectable must not turn this into a false positive.",
    sql: `
create or replace function public.wp18b_nm06(p_id uuid) returns void language plpgsql as $$
begin
  update public.pay_batches b
     set total_bank_out = (select coalesce(sum(i.amount_inc_vat), 0)
                             from public.pay_batch_items i
                            where i.pay_batch_candidate_id = b.id
                              and i.is_voided = false)
   where b.id = p_id and b.status = 'DRAFT';
end $$;
`,
  },
  {
    id: "NM07_truncate_non_evidence_table",
    expect: "CLEAN",
    why: "Paired negative for M07. A TRUNCATE of a relation that is not census evidence.",
    sql: `
create table if not exists public.wp18b_scratch_not_evidence(x int);
create or replace function public.wp18b_nm07() returns void language plpgsql as $$
begin
  truncate table public.wp18b_scratch_not_evidence;
end $$;
`,
  },
  {
    id: "NM09_subquery_then_no_cancel_stamp",
    expect: "CLEAN",
    why: "Paired negative for M09. Subquery first, then a write that touches neither cancelled_at_utc nor a cancellation status. Must not trip 6c.",
    sql: `
create or replace function public.wp18b_nm09(p_id uuid) returns void language plpgsql as $$
begin
  update public.pay_batches
     set cancel_reason = (select string_agg(x.description, ', ')
                            from public.pay_batch_items x where x.id = p_id)
   where id = p_id;
end $$;
`,
  },
  {
    id: "NM10_subquery_then_unrelated_reservation_column",
    expect: "CLEAN",
    why: "Paired negative for M10. Subquery first on pay_advance_reservations, but the assigned columns are not census-relevant; status and released_reason are read in the predicate only.",
    sql: `
create or replace function public.wp18b_nm10(p_id uuid, p_actor uuid) returns void language plpgsql as $$
begin
  update public.pay_advance_reservations r
     set updated_by_user_id = (select u.id from (select p_actor as id) u where u.id is not null)
   where r.id = p_id and r.status = 'RESERVED' and r.released_reason is null;
end $$;
`,
  },
  {
    id: "NX01_on_conflict_do_nothing",
    expect: "CLEAN",
    why: "Paired negative for X01. An upsert whose conflict action writes no census column: DO NOTHING, and an explicit column list that omits is_voided.",
    sql: `
create or replace function public.wp18b_nx01(p_id uuid) returns void language plpgsql as $$
begin
  insert into public.pay_batch_items (id, pay_batch_candidate_id, description)
  values (p_id, p_id, 'x')
  on conflict (id) do nothing;
end $$;
`,
  },
  {
    id: "NX03_read_through_view",
    expect: "CLEAN",
    why: "Paired negative for X03. A view over the evidence table that is only READ.",
    sql: `
create or replace view public.wp18b_nx03_view as select * from public.pay_batch_items;
create or replace function public.wp18b_nx03(p_id uuid) returns integer language plpgsql as $$
declare v_n integer;
begin
  select count(*) into v_n from public.wp18b_nx03_view where id = p_id and is_voided = false;
  return v_n;
end $$;
`,
  },
  {
    id: "NX04_whitespace_dot_unrelated_column",
    expect: "CLEAN",
    why: "Paired negative for X04. Whitespace around the schema dot; no census column assigned.",
    sql: `
create or replace function public.wp18b_nx04(p_id uuid) returns void language plpgsql as $$
begin
  update public . pay_batches
     set total_bank_out = 0
   where id = p_id and status = 'DRAFT' and cancelled_at_utc is null;
end $$;
`,
  },
  {
    id: "NY01_unicode_escape_read_only",
    expect: "CLEAN",
    why: "Paired negative for Y01-Y04. The same Unicode-escape identifier, read only. Removing the adjacency requirement must not turn every mention into a write.",
    sql: `
create or replace function public.wp18b_ny01(p_id uuid) returns integer language plpgsql as $$
declare v_n integer;
begin
  select count(*) into v_n from U&"pay_batch_items" where id = p_id and is_voided = false;
  return v_n;
end $$;
`,
  },
  {
    id: "NY05_rule_on_non_evidence_table",
    expect: "CLEAN",
    why: "Paired negative for Y05. A rule whose action writes a relation that is not census evidence. Section 11a must not fire on it.",
    sql: `
create table if not exists public.wp18b_ny05_shim(id uuid primary key);
create table if not exists public.wp18b_ny05_log(id uuid);
create or replace rule wp18b_ny05_log as on insert to public.wp18b_ny05_shim
  do also insert into public.wp18b_ny05_log(id) values (new.id);
`,
  },];

// ---------------------------------------------------------------------------
// Run every fixture inside BEGIN ... ROLLBACK with the verifier in the same
// transaction, so nothing durable is written.
// ---------------------------------------------------------------------------
for (const fixture of FIXTURES) {
  const out = runSql(`begin;\n${fixture.sql}\n${verifierSql}\nrollback;\n`);
  const code = out.match(/WEEKLY_SOURCE_[A-Z_]+/)?.[0] ?? null;
  const got = code ? 'DETECTED' : (/"ok": true/.test(out) ? 'CLEAN' : 'ERROR');
  const detail = got === 'DETECTED' ? code
    : got === 'CLEAN' ? 'ok:true'
    : (out.split('\n').find((line) => /error/i.test(line)) ?? 'no verifier verdict').trim().slice(0, 100);
  const marker = fixture.expect === 'DETECTED' ? '[+]' : '[-]';
  record(`${marker} ${fixture.id}`, got === fixture.expect,
         got === fixture.expect ? detail : `expected ${fixture.expect}, got ${got}: ${detail}`);
}

// ---------------------------------------------------------------------------
// Drop-hazard fixture (review finding F5). The verifier calls itself READ-ONLY.
// An UNQUALIFIED "drop table if exists wp18_observed" resolves to a PERMANENT
// table of that name when no temporary one exists yet, and the first version of
// the file destroyed three real tables and still reported ok:true. Every drop is
// now qualified with pg_temp., and these five sentinels must survive a run.
// ---------------------------------------------------------------------------
{
  const names = ['wp18_observed', 'wp18_inventory', 'wp18_census_columns', 'wp18_defs', 'wp18_write_targets'];
  const dropAll = names.map((n) => `drop table if exists public.${n};`).join('\n');
  runSql(dropAll);
  runSql(names.map((n) => `create table public.${n}(sentinel int);`).join('\n'));
  runSql(verifierSql);
  const out = runSql(`select count(*) as survivors from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'public' and c.relkind = 'r'
      and c.relname in (${names.map((n) => `'${n}'`).join(', ')});`);
  const survived = Number((out.match(/^\s*(\d+)\s*$/m) ?? [0, 0])[1]);
  runSql(dropAll);
  record('[+] F5_drop_hazard_permanent_tables_survive', survived === names.length,
         survived === names.length
           ? `all ${names.length} permanent tables survived`
           : `only ${survived}/${names.length} survived - the verifier destroyed real tables`);
}

fs.rmSync(tmp, { recursive: true, force: true });

const failed = results.filter((r) => !r.ok);
const positives = results.filter((r) => r.id.startsWith('[+]')).length;
const negatives = results.filter((r) => r.id.startsWith('[-]')).length;
console.log(`\n=== ${results.length - failed.length}/${results.length} fixtures passed `
  + `(${positives} positive, ${negatives} negative)`);
console.log(`=== verifier under test: ${VERIFIER}`);
if (failed.length) {
  console.log('\nFAILURES:');
  console.log(JSON.stringify(failed, null, 2));
  process.exitCode = 1;
}
