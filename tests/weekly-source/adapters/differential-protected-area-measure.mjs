// Weekly Source Plan 6.2 - measuring one protected area's database surface (WP-16d).
//
// This is the measurement half of the 29-area differential. The rules live in
// `differential-protected-area-rules.mjs`; this file turns a rule into one SQL statement,
// runs it against a database, and returns the measured rows and their digest.
//
// WHAT IS MEASURED, EXACTLY.
//
// The `23A` section 12 surface "database rows and projections", as the installed shape of the
// area's owners: for a routine, its identity arguments, owner role, security mode, SET
// configuration, privileges, declared result type and the SHA-256 of its installed definition;
// for a relation, its kind, row-level-security flag, privileges and the SHA-256 of its column
// list; plus every non-internal trigger and every row-level policy attached to those relations.
//
// Carriage returns are removed from every definition before hashing, because installed TEST
// holds CRLF inside routine bodies while a clean local build holds LF and the environment
// report records 38 false differences when that is not done.
//
// WHAT IS NOT MEASURED. Seven of the eight `23A` section 12 surfaces are outside a database
// phase: Office command availability and visible layout, Candidate actions/evidence/expense
// availability, invoice membership/value/document/export, report/export FIELDS as rendered,
// the C1 request shape, the backend response VALUE, and the Workbench/Banking-owned output
// supplied by the separate boundary evidence. A pass here is a database pass, never a full one.

import { createHash } from 'node:crypto';

/** Literal for a PostgreSQL string, doubling single quotes. */
export function sqlLiteral(value) {
  return `'${String(value).replaceAll("'", "''")}'`;
}

function routineSelect({ regex, definitionContains, excludePattern }) {
  const predicates = ['n.nspname in (\'public\',\'private\')', "p.prokind in ('f','p')"];
  if (regex) predicates.push(`(n.nspname||'.'||p.proname) ~ ${sqlLiteral(regex)}`);
  if (definitionContains) {
    predicates.push(`pg_catalog.pg_get_functiondef(p.oid) like ${sqlLiteral(`%${definitionContains}%`)}`);
  }
  if (excludePattern) predicates.push(`not ((n.nspname||'.'||p.proname) ~ ${sqlLiteral(excludePattern)})`);
  return `
    select 'routine|'||n.nspname||'.'||p.proname
           ||'('||pg_catalog.pg_get_function_identity_arguments(p.oid)||')'
           ||'|owner='||pg_catalog.pg_get_userbyid(p.proowner)
           ||'|secdef='||p.prosecdef::text
           ||'|config='||coalesce(pg_catalog.array_to_string(p.proconfig,','),'-')
           ||'|acl='||coalesce(pg_catalog.array_to_string(p.proacl::text[],','),'-')
           ||'|ret='||pg_catalog.pg_get_function_result(p.oid)
           ||'|def='||pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
               pg_catalog.replace(pg_catalog.pg_get_functiondef(p.oid),chr(13),''),'UTF8')),'hex')
           as row_text
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where ${predicates.join(' and ')}`;
}

function relationSelects({ regex, excludePattern }) {
  if (!regex) return [];
  const base = ["n.nspname in ('public','private')", "c.relkind in ('r','p','v','m')",
    `(n.nspname||'.'||c.relname) ~ ${sqlLiteral(regex)}`];
  if (excludePattern) base.push(`not ((n.nspname||'.'||c.relname) ~ ${sqlLiteral(excludePattern)})`);
  const where = base.join(' and ');
  return [
    `
    select 'relation|'||n.nspname||'.'||c.relname
           ||'|kind='||c.relkind::text
           ||'|rls='||c.relrowsecurity::text
           ||'|acl='||coalesce(pg_catalog.array_to_string(c.relacl::text[],','),'-')
           ||'|cols='||pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(coalesce((
               select pg_catalog.string_agg(
                 a.attname||' '||pg_catalog.format_type(a.atttypid,a.atttypmod)
                 ||' notnull='||a.attnotnull::text
                 ||' default='||coalesce(pg_catalog.pg_get_expr(d.adbin,d.adrelid),'-'),
                 chr(10) order by a.attname)
               from pg_catalog.pg_attribute a
               left join pg_catalog.pg_attrdef d on d.adrelid=a.attrelid and d.adnum=a.attnum
               where a.attrelid=c.oid and a.attnum>0 and not a.attisdropped),''),'UTF8')),'hex')
           as row_text
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where ${where}`,
    `
    select 'trigger|'||n.nspname||'.'||c.relname||'|'||t.tgname||'|'
           ||pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
               pg_catalog.replace(pg_catalog.pg_get_triggerdef(t.oid),chr(13),''),'UTF8')),'hex')
           as row_text
    from pg_catalog.pg_trigger t
    join pg_catalog.pg_class c on c.oid=t.tgrelid
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where not t.tgisinternal and ${where}`,
    `
    select 'policy|'||n.nspname||'.'||c.relname||'|'||pol.polname
           ||'|cmd='||pol.polcmd::text
           ||'|roles='||coalesce((select pg_catalog.string_agg(pg_catalog.pg_get_userbyid(r),',' order by pg_catalog.pg_get_userbyid(r))
                                  from pg_catalog.unnest(pol.polroles) as t(r)),'-')
           ||'|'||pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
               coalesce(pg_catalog.pg_get_expr(pol.polqual,pol.polrelid),'-')
               ||coalesce(pg_catalog.pg_get_expr(pol.polwithcheck,pol.polrelid),'-'),'UTF8')),'hex')
           as row_text
    from pg_catalog.pg_policy pol
    join pg_catalog.pg_class c on c.oid=pol.polrelid
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where ${where}`,
  ];
}

/**
 * PROT-SEC-001. The protected subject is the reachable surface itself, so the rule is the
 * privilege catalogue: every routine a browser role may EXECUTE and every relation a browser
 * role may SELECT, with its privileges and its row-level security.
 */
function browserReachableSelects() {
  return [
    `
    select 'browser_routine|'||n.nspname||'.'||p.proname
           ||'('||pg_catalog.pg_get_function_identity_arguments(p.oid)||')'
           ||'|anon='||pg_catalog.has_function_privilege('anon',p.oid,'EXECUTE')::text
           ||'|auth='||pg_catalog.has_function_privilege('authenticated',p.oid,'EXECUTE')::text
           ||'|public='||pg_catalog.has_function_privilege('public',p.oid,'EXECUTE')::text
           ||'|secdef='||p.prosecdef::text
           ||'|owner='||pg_catalog.pg_get_userbyid(p.proowner)
           as row_text
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where n.nspname in ('public','private')
      and (pg_catalog.has_function_privilege('anon',p.oid,'EXECUTE')
        or pg_catalog.has_function_privilege('authenticated',p.oid,'EXECUTE'))`,
    `
    select 'browser_relation|'||n.nspname||'.'||c.relname
           ||'|anonS='||pg_catalog.has_table_privilege('anon',c.oid,'SELECT')::text
           ||'|anonW='||pg_catalog.has_table_privilege('anon',c.oid,'INSERT,UPDATE,DELETE')::text
           ||'|authS='||pg_catalog.has_table_privilege('authenticated',c.oid,'SELECT')::text
           ||'|authW='||pg_catalog.has_table_privilege('authenticated',c.oid,'INSERT,UPDATE,DELETE')::text
           ||'|rls='||c.relrowsecurity::text
           ||'|forcerls='||c.relforcerowsecurity::text
           as row_text
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname in ('public','private') and c.relkind in ('r','p','v','m')
      and (pg_catalog.has_table_privilege('anon',c.oid,'SELECT')
        or pg_catalog.has_table_privilege('authenticated',c.oid,'SELECT'))`,
    `
    select 'schema_grant|'||n.nspname
           ||'|anon='||pg_catalog.has_schema_privilege('anon',n.oid,'USAGE')::text
           ||'|auth='||pg_catalog.has_schema_privilege('authenticated',n.oid,'USAGE')::text
           ||'|public='||pg_catalog.has_schema_privilege('public',n.oid,'USAGE')::text
           as row_text
    from pg_catalog.pg_namespace n
    where n.nspname in ('public','private','extensions','auth','vault')`,
  ];
}

/**
 * PROT-INFRA-001. Timing and resource limits live in routine SET clauses and in
 * per-database/per-role settings, not in a name family.
 */
function configurationSelects() {
  return [
    `
    select 'routine_config|'||n.nspname||'.'||p.proname
           ||'('||pg_catalog.pg_get_function_identity_arguments(p.oid)||')|'
           ||pg_catalog.array_to_string(p.proconfig,',')
           as row_text
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where n.nspname in ('public','private')
      and pg_catalog.array_to_string(coalesce(p.proconfig,'{}'),',') ~ '(timeout|lease|lock|work_mem|idle)'`,
    `
    select 'db_role_setting|'||case when s.setdatabase=0 then '<cluster>' else '<this database>' end
           ||'|'||coalesce(pg_catalog.pg_get_userbyid(s.setrole),'-')
           ||'|'||pg_catalog.array_to_string(s.setconfig,',')
           as row_text
    from pg_catalog.pg_db_role_setting s
    -- Scoped to this database and to cluster-wide rows only. pg_db_role_setting is a shared
    -- catalogue: reading it unscoped on a shared local cluster makes the measurement move
    -- whenever another package creates or drops a database of its own.
    where s.setdatabase=0
       or s.setdatabase=(select d.oid from pg_catalog.pg_database d where d.datname=pg_catalog.current_database())`,
    `
    select 'server_major|'||(pg_catalog.current_setting('server_version_num')::integer/10000)::text as row_text`,
  ];
}

/** Build the one SQL statement that returns this area's measured rows, one per line. */
export function measurementSqlFor(rule, newOwnerPattern) {
  const excludePattern = rule.excludeNewSourceOwners ? newOwnerPattern : null;
  let parts;
  if (rule.browserReachable) parts = browserReachableSelects();
  else if (rule.configurationSurface) parts = configurationSelects();
  else {
    parts = [routineSelect({
      regex: rule.routineRegex,
      definitionContains: rule.definitionContains,
      excludePattern,
    }), ...relationSelects({ regex: rule.relationRegex, excludePattern })];
  }
  return `select row_text from (\n${parts.join('\n    union all\n')}\n) all_rows order by row_text;`;
}

/**
 * How many owners the area's rule matched but the stated exclusion removed. Reported for every
 * area so the exclusion is visible rather than hidden.
 */
export function excludedOwnerSqlFor(rule, newOwnerPattern) {
  if (!rule.excludeNewSourceOwners || rule.browserReachable || rule.configurationSurface) return null;
  const parts = [routineSelect({
    regex: rule.routineRegex,
    definitionContains: rule.definitionContains,
    excludePattern: null,
  })];
  if (rule.relationRegex) parts.push(relationSelects({ regex: rule.relationRegex, excludePattern: null })[0]);
  return `select count(*)::text from (\n${parts.join('\n    union all\n')}\n) all_rows
          where row_text ~ ${sqlLiteral(`\\|(public|private)\\._?weekly_(source|exceptional)_`)}
             or row_text like '%tsfin_weekly_source_hours_v1%';`;
}

/** The digest the differential capture carries for a measured surface. */
export function digestRows(rows) {
  const body = rows.join('\n');
  return {
    measuredRowCount: rows.length,
    digest: createHash('sha256').update(body, 'utf8').digest('hex'),
  };
}
