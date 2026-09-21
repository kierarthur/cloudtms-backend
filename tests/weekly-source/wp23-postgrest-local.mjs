// Weekly Source Plan 6.2 — WP-23.  A local stand-in for PostgREST.
//
// WP-23 has to prove that the Office Authorise route reaches
// `public.weekly_source_first_authorise_v1`, and the Gate 13 hostile finance
// review's finding F2 is precisely a report of source that was never exercised.
// A stub `rpc` proves nothing, so the broker must be driven as it is: it reaches
// PostgreSQL only through `${SUPABASE_URL}/rest/v1/...`, over HTTP, as
// `service_role`.  This module is that endpoint, backed by the local disposable
// PostgreSQL 17.11 build.
//
// It implements only the subset the driven routes use, and it is deliberately
// thin: every business decision still happens inside the database owners.
//
//   POST /rest/v1/rpc/<function>   named arguments, cast to the INSTALLED
//                                  parameter types read from `pg_proc`; one
//                                  transaction per request; errors returned in
//                                  PostgREST's `{code, message, details, hint}`
//                                  shape with the real SQLSTATE.
//   GET  /rest/v1/<relation>       `select`, `limit`, `offset`, `order` and the
//                                  `col=<op>.<value>` filters the driven routes
//                                  use.
//   PATCH/POST /rest/v1/<relation> the narrow writes the driven routes make.
//
// `request.jwt.claim.role` is declared `service_role` on every connection, which
// is what every Weekly Source owner's in-function role check reads.
//
// Local only.  The target database name is validated and the host is pinned to
// 127.0.0.1; no hosted database can be reached through this module.

import { createServer } from 'node:http';
import { spawnSync } from 'node:child_process';

const DATABASE_PATTERN = /^[a-z][a-z0-9_]{0,62}$/;
const TAG = '$wp23rest$';
const FILTER_OPERATORS = new Map([
  ['eq', '='], ['neq', '<>'], ['gt', '>'], ['gte', '>='], ['lt', '<'], ['lte', '<='],
  ['like', 'like'], ['ilike', 'ilike'],
]);
const RESERVED = new Set(['select', 'limit', 'offset', 'order', 'on_conflict', 'columns']);

function quoteLiteral(value) {
  const text = String(value);
  if (text.includes(TAG)) throw new Error('value collides with the dollar-quote tag');
  return `${TAG}${text}${TAG}`;
}

function quoteIdentifier(name) {
  if (!/^[a-z_][a-z0-9_]*$/i.test(String(name))) throw new Error(`unsafe identifier: ${name}`);
  return `"${name}"`;
}

function qualifiedRelation(name) {
  const [schema, relation] = String(name).includes('.')
    ? String(name).split('.')
    : ['public', String(name)];
  return `${quoteIdentifier(schema)}.${quoteIdentifier(relation)}`;
}

export function createLocalPostgrest({
  database,
  psqlBin = process.env.PSQL_BIN || 'psql',
  port = 0,
  databasePort = Number.parseInt(process.env.WP23_DATABASE_PORT || '55433', 10),
} = {}) {
  if (!DATABASE_PATTERN.test(String(database ?? ''))) {
    throw new Error('a local proof database name must be a short lower-case identifier');
  }
  if (!Number.isInteger(databasePort) || databasePort < 1024 || databasePort > 65535) {
    throw new Error('a local proof database port is required');
  }
  const connection = `postgresql://postgres@127.0.0.1:${databasePort}/${database}`;
  const calls = [];

  // `request.jwt.claim.role` is declared through PGOPTIONS rather than a `set`
  // statement, so the connection carries it from the first statement, exactly as
  // PostgREST's own connection does, and nothing but the query's own rows
  // reaches stdout.
  const PG_OPTIONS = '-c jit=off -c request.jwt.claim.role=service_role';

  function run(statements) {
    const argv = ['-X', '-tA', '-v', 'ON_ERROR_STOP=1', '-v', 'VERBOSITY=verbose', connection];
    for (const statement of statements) argv.push('-c', statement);
    return spawnSync(psqlBin, argv, {
      encoding: 'utf8',
      env: {
        ...process.env,
        PGPASSWORD: process.env.PGPASSWORD || 'localonly',
        PGOPTIONS: PG_OPTIONS,
      },
      maxBuffer: 128 * 1024 * 1024,
    });
  }

  function postgrestError(stderr) {
    const head = /^ERROR:\s+([0-9A-Z]{5}):\s*(.*)$/m.exec(stderr);
    const detail = /^DETAIL:\s*([\s\S]*?)(?:\nHINT:|\nLOCATION:|\nCONTEXT:|\nSTATEMENT:|$)/m.exec(stderr);
    const hint = /^HINT:\s*(.*)$/m.exec(stderr);
    return {
      code: head ? head[1] : 'XX000',
      message: head ? head[2].trim() : String(stderr).trim().slice(0, 500),
      details: detail ? detail[1].trim() : null,
      hint: hint ? hint[1].trim() : null,
    };
  }

  const signatures = new Map();
  function installedSignature(functionName) {
    if (signatures.has(functionName)) return signatures.get(functionName);
    const sql = `select coalesce(string_agg(entry.name||'~'||entry.type, '|' order by entry.ordinality), '')
      from pg_catalog.pg_proc p
      join pg_catalog.pg_namespace n on n.oid = p.pronamespace
      cross join lateral unnest(p.proargnames, p.proargtypes::oid[])
        with ordinality as raw(name, typeoid, ordinality)
      cross join lateral (select raw.name as name, pg_catalog.format_type(raw.typeoid, null) as type,
                                 raw.ordinality as ordinality) entry
      where n.nspname='public' and p.proname=${quoteLiteral(functionName)}`;
    const result = run([sql]);
    if (result.status !== 0) throw new Error(result.stderr || result.stdout);
    const raw = String(result.stdout).trim();
    const parsed = raw
      ? raw.split('|').map((entry) => {
        const [name, type] = entry.split('~');
        return { name, type };
      })
      : [];
    signatures.set(functionName, parsed);
    return parsed;
  }

  // WP-37: PostgREST selects FROM a set-returning function and returns its rows
  // as an array. Wrapping one in `to_jsonb(...)` raises
  // `0A000 set-returning functions are not allowed in COALESCE`, so the shape of
  // the call has to follow `pg_proc.proretset` exactly as PostgREST's does.
  const setReturning = new Map();
  function returnsSet(functionName) {
    if (setReturning.has(functionName)) return setReturning.get(functionName);
    const sql = `select coalesce(bool_or(p.proretset), false)::text
      from pg_catalog.pg_proc p
      join pg_catalog.pg_namespace n on n.oid = p.pronamespace
      where n.nspname='public' and p.proname=${quoteLiteral(functionName)}`;
    const result = run([sql]);
    if (result.status !== 0) throw new Error(result.stderr || result.stdout);
    // `boolean::text` is 'true'/'false', never 't'/'f'.
    const value = String(result.stdout).trim().toLowerCase() === 'true';
    setReturning.set(functionName, value);
    return value;
  }

  function encodeArgument(value, type) {
    if (value === null || value === undefined) return `null::${type}`;
    // WP-37: a PostgreSQL ARRAY parameter. Real PostgREST turns a JSON array
    // into a PostgreSQL array; `'["…"]'::uuid[]` is JSON syntax and raises
    // `22P02 malformed array literal`. Every element is quoted and the whole
    // constructor is explicitly cast, so an untyped literal never reaches the
    // array type.
    if (/\[\]$/.test(type) && Array.isArray(value)) {
      const elementType = type.slice(0, -2);
      if (value.length === 0) return `array[]::${type}`;
      const elements = value.map((element) => (element === null || element === undefined
        ? `null::${elementType}`
        : `${quoteLiteral(typeof element === 'object' ? JSON.stringify(element) : element)}::${elementType}`));
      return `array[${elements.join(', ')}]::${type}`;
    }
    if (type === 'jsonb' || type === 'json' || /\[\]$/.test(type) || typeof value === 'object') {
      return `${quoteLiteral(JSON.stringify(value))}::${type === 'json' ? 'json' : (/\[\]$/.test(type) ? type : 'jsonb')}`;
    }
    return `${quoteLiteral(value)}::${type}`;
  }

  function handleRpc(functionName, body) {
    const signature = installedSignature(functionName);
    if (signature.length === 0 && Object.keys(body || {}).length > 0) {
      return {
        status: 404,
        payload: { code: 'PGRST202', message: `Could not find the function public.${functionName}`, details: null, hint: null },
      };
    }
    const encoded = signature
      .filter((parameter) => Object.prototype.hasOwnProperty.call(body || {}, parameter.name))
      .map((parameter) => `${quoteIdentifier(parameter.name)} := ${encodeArgument(body[parameter.name], parameter.type)}`);
    const call = `public.${quoteIdentifier(functionName)}(${encoded.join(', ')})`;
    const sql = returnsSet(functionName)
      ? `select coalesce(pg_catalog.json_agg(pg_catalog.to_jsonb(rows))::text, '[]'::text) from ${call} rows`
      : `select coalesce(pg_catalog.to_jsonb(${call})::text, 'null'::text)`;
    const result = run([sql]);
    if (result.status !== 0) {
      return { status: 400, payload: postgrestError(result.stderr || result.stdout) };
    }
    const text = String(result.stdout).trim();
    let value = null;
    try { value = text ? JSON.parse(text) : null; } catch { value = text; }
    return { status: 200, payload: value };
  }

  function buildWhere(searchParams) {
    const clauses = [];
    for (const [key, raw] of searchParams.entries()) {
      if (RESERVED.has(key)) continue;
      const value = String(raw);
      const dot = value.indexOf('.');
      const operator = dot === -1 ? 'eq' : value.slice(0, dot);
      const operand = dot === -1 ? value : value.slice(dot + 1);
      const column = quoteIdentifier(key);
      if (operator === 'is') {
        clauses.push(operand === 'null' ? `${column} is null` : `${column} is ${operand}`);
        continue;
      }
      if (operator === 'not') {
        const innerDot = operand.indexOf('.');
        const innerOperator = operand.slice(0, innerDot);
        const innerOperand = operand.slice(innerDot + 1);
        if (innerOperator === 'is') {
          clauses.push(innerOperand === 'null' ? `${column} is not null` : `${column} is not ${innerOperand}`);
        } else {
          const sqlOperator = FILTER_OPERATORS.get(innerOperator);
          if (!sqlOperator) throw new Error(`unsupported filter: ${key}=${value}`);
          clauses.push(`not (${column} ${sqlOperator} ${quoteLiteral(innerOperand)})`);
        }
        continue;
      }
      if (operator === 'in') {
        const items = operand.replace(/^\(/, '').replace(/\)$/, '');
        const list = items.length
          ? items.split(',').map((item) => quoteLiteral(item.replace(/^"|"$/g, ''))).join(',')
          : 'null';
        clauses.push(`${column} in (${list})`);
        continue;
      }
      const sqlOperator = FILTER_OPERATORS.get(operator);
      if (!sqlOperator) throw new Error(`unsupported filter: ${key}=${value}`);
      clauses.push(`${column} ${sqlOperator} ${quoteLiteral(operand)}`);
    }
    return clauses.length ? `where ${clauses.join(' and ')}` : '';
  }

  function buildSelect(searchParams) {
    const select = searchParams.get('select');
    if (!select || select === '*') return '*';
    return select.split(',')
      .map((column) => column.trim())
      .filter(Boolean)
      .map((column) => quoteIdentifier(column.split(':').pop()))
      .join(', ');
  }

  function handleRead(relation, searchParams) {
    const columns = buildSelect(searchParams);
    const where = buildWhere(searchParams);
    const order = searchParams.get('order');
    const orderBy = order
      ? `order by ${order.split(',').map((entry) => {
        const [column, ...modifiers] = entry.split('.');
        const direction = modifiers.includes('desc') ? 'desc' : 'asc';
        const nulls = modifiers.includes('nullslast') ? ' nulls last'
          : (modifiers.includes('nullsfirst') ? ' nulls first' : '');
        return `${quoteIdentifier(column)} ${direction}${nulls}`;
      }).join(', ')}`
      : '';
    const limit = searchParams.get('limit');
    const offset = searchParams.get('offset');
    const sql = `select coalesce(pg_catalog.json_agg(source)::text, '[]'::text)
      from (select ${columns} from ${qualifiedRelation(relation)} ${where} ${orderBy}
            ${limit ? `limit ${Number.parseInt(limit, 10) || 0}` : ''}
            ${offset ? `offset ${Number.parseInt(offset, 10) || 0}` : ''}) as source`;
    const result = run([sql]);
    if (result.status !== 0) {
      return { status: 400, payload: postgrestError(result.stderr || result.stdout) };
    }
    let rows = [];
    try { rows = JSON.parse(String(result.stdout).trim() || '[]'); } catch { rows = []; }
    return { status: 200, payload: rows };
  }

  function handleWrite(method, relation, searchParams, body) {
    const where = buildWhere(searchParams);
    if (method === 'PATCH') {
      const assignments = Object.entries(body || {})
        .map(([column, value]) => `${quoteIdentifier(column)} = ${value === null
          ? 'null'
          : (typeof value === 'object' ? `${quoteLiteral(JSON.stringify(value))}::jsonb` : quoteLiteral(value))}`)
        .join(', ');
      if (!assignments) return { status: 400, payload: { code: 'PGRST102', message: 'empty patch' } };
      const sql = `with updated as (update ${qualifiedRelation(relation)} set ${assignments} ${where} returning *)
        select coalesce(pg_catalog.json_agg(updated)::text, '[]'::text) from updated`;
      const result = run([sql]);
      if (result.status !== 0) return { status: 400, payload: postgrestError(result.stderr || result.stdout) };
      let rows = [];
      try { rows = JSON.parse(String(result.stdout).trim() || '[]'); } catch { rows = []; }
      return { status: 200, payload: rows };
    }
    const records = Array.isArray(body) ? body : [body || {}];
    const columns = [...new Set(records.flatMap((record) => Object.keys(record)))];
    if (columns.length === 0) return { status: 400, payload: { code: 'PGRST102', message: 'empty insert' } };
    const values = records.map((record) => `(${columns.map((column) => {
      const value = record[column];
      if (value === null || value === undefined) return 'null';
      if (typeof value === 'object') return `${quoteLiteral(JSON.stringify(value))}::jsonb`;
      return quoteLiteral(value);
    }).join(', ')})`).join(', ');
    const sql = `with inserted as (insert into ${qualifiedRelation(relation)} (${columns.map(quoteIdentifier).join(', ')})
      values ${values} returning *)
      select coalesce(pg_catalog.json_agg(inserted)::text, '[]'::text) from inserted`;
    const result = run([sql]);
    if (result.status !== 0) return { status: 400, payload: postgrestError(result.stderr || result.stdout) };
    let rows = [];
    try { rows = JSON.parse(String(result.stdout).trim() || '[]'); } catch { rows = []; }
    return { status: 201, payload: rows };
  }

  const server = createServer((req, res) => {
    let raw = '';
    req.on('data', (chunk) => { raw += chunk; });
    req.on('end', () => {
      let outcome;
      try {
        const url = new URL(req.url, 'http://127.0.0.1');
        const parts = url.pathname.replace(/^\/+/, '').split('/');
        if (parts[0] !== 'rest' || parts[1] !== 'v1') {
          outcome = { status: 404, payload: { code: 'PGRST000', message: 'not a PostgREST path' } };
        } else if (parts[2] === 'rpc') {
          const body = raw ? JSON.parse(raw) : {};
          calls.push({ kind: 'rpc', name: parts[3], body });
          outcome = handleRpc(parts[3], body);
        } else if (req.method === 'GET') {
          calls.push({ kind: 'read', relation: parts[2], query: url.search });
          outcome = handleRead(parts[2], url.searchParams);
        } else {
          const body = raw ? JSON.parse(raw) : {};
          calls.push({ kind: 'write', method: req.method, relation: parts[2] });
          outcome = handleWrite(req.method, parts[2], url.searchParams, body);
        }
      } catch (error) {
        outcome = { status: 500, payload: { code: 'XXLOC', message: String(error?.message || error), details: null, hint: null } };
      }
      const text = JSON.stringify(outcome.payload ?? null);
      res.writeHead(outcome.status, {
        'content-type': 'application/json; charset=utf-8',
        'content-length': Buffer.byteLength(text),
      });
      res.end(text);
    });
  });

  return {
    calls,
    listen: () => new Promise((resolve) => {
      server.listen(port, '127.0.0.1', () => resolve(`http://127.0.0.1:${server.address().port}`));
    }),
    close: () => new Promise((resolve) => server.close(() => resolve())),
  };
}
