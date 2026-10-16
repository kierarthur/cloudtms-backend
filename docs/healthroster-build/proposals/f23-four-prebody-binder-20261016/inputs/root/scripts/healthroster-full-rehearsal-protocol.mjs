import {scanManagedSql, digest, canonical} from './cloudtms-managed-release-sql.mjs';
import {fixtureObservationSql} from './healthroster-live-test-restoration.mjs';

// psql's gset terminates its pending SQL command without a semicolon. The
// managed packaging lexer intentionally leaves that client command opaque;
// the rehearsal census must still locate a following conditional DO exactly.
// Mask only lexer-proved top-level gset, preserving every character offset.
export function scanRehearsalSql(sql){
  // The shared lexer returns canonical-LF offsets. Refuse another rendering
  // rather than apply those offsets to different bytes and hide a command.
  if(typeof sql!=='string'||canonical(sql)!==sql)throw Error('REHEARSAL_CANONICAL_SQL_REQUIRED');
  const parsed=scanManagedSql(sql);
  const terminators=parsed.meta.filter(row=>/^\\gset(?:\s+[A-Za-z_][A-Za-z0-9_]*)?\s*$/i.test(row.text));
  if(!terminators.length)return parsed;
  let masked=sql;
  for(const row of [...terminators].reverse()){
    const token=sql.slice(row.start,row.end);
    masked=masked.slice(0,row.start)+';'+token.slice(1).replace(/[^\r\n]/g,' ')+masked.slice(row.end);
  }
  const statements=scanManagedSql(masked).statements.map(row=>{
    const terminator=terminators.find(meta=>row.end===meta.start+1);
    return terminator?{...row,end:terminator.end}:row;
  });
  return {...parsed,statements};
}

// A source safety reviewer authorises exact envelope removals before rendering.
// Hash-bound full SQL remains retained; this never opens a connection or grants
// permission to execute a verifier, or substitutes for its effect review.
export function neutraliseReviewedVerifierEnvelope(unit, review) {
  if (!['verifier','final_browser_verifier'].includes(unit?.kind)
    || typeof unit.sql !== 'string' || canonical(unit.sql)!==unit.sql || digest(unit.sql) !== unit.sqlHash
    || review?.sourceSqlHash !== unit.sqlHash || !Array.isArray(review.removedCommands)) {
    throw Error('REHEARSAL_ENVELOPE_REVIEW_REFUSED');
  }
  const parsed=scanManagedSql(unit.sql), removals=[];
  for (const statement of parsed.statements) {
    const first=statement.tokens[0], command=statement.tokens.join(' ');
    if (!['BEGIN','COMMIT','END','START','ABORT','PREPARE','ROLLBACK'].includes(first)
      || first==='ROLLBACK' && statement.tokens[1]==='TO') continue;
    if (!['BEGIN','BEGIN READ ONLY','BEGIN ISOLATION LEVEL REPEATABLE READ',
      'BEGIN ISOLATION LEVEL SERIALIZABLE','COMMIT','ROLLBACK','END'].includes(command)
      || command==='BEGIN READ ONLY' && review.readOnlyBodyReviewed!==true) {
      throw Error('REHEARSAL_ENVELOPE_COMMAND_REFUSED');
    }
    removals.push({...statement,command});
  }
  for (const meta of parsed.meta) {
    if (!/^\\set\s+ON_ERROR_(?:STOP|ROLLBACK)\b/i.test(meta.text)) continue;
    if (!/^\\set\s+ON_ERROR_STOP\s+(?:on|off)\s*$/i.test(meta.text)) {
      throw Error('REHEARSAL_ENVELOPE_ERROR_CONTROL_REFUSED');
    }
    removals.push({...meta,command:meta.text.trim()});
  }
  removals.sort((a,b)=>a.start-b.start);
  if (JSON.stringify(removals.map(x=>x.command))!==JSON.stringify(review.removedCommands)) {
    throw Error('REHEARSAL_ENVELOPE_CENSUS_REFUSED');
  }
  let sql=unit.sql;
  for (const removal of [...removals].reverse()) sql=sql.slice(0,removal.start)+sql.slice(removal.end);
  const envelope=removals.map(x=>({command:x.command,start:x.start,end:x.end,
    sha256:digest(unit.sql.slice(x.start,x.end))}));
  return {...unit,sql,sqlHash:digest(sql),sourceSqlHash:unit.sqlHash,envelope,
    readOnlyBodyReviewed:review.readOnlyBodyReviewed===true};
}

// Source-only renderer. This module opens no database/network connection.
// The post-Banking coordinator must supply exact pushed, independently reviewed
// units, hold SEND_7 before its before probes, and retain it through fresh proof.
export function renderFullRehearsalUnits(units, {outerIsolation,observeLiveBusiness=false} = {}) {
  if (!Array.isArray(units) || !units.length) throw Error('REHEARSAL_UNITS_REQUIRED');
  if (outerIsolation !== undefined && !['READ COMMITTED','REPEATABLE READ','SERIALIZABLE'].includes(outerIsolation)) {
    throw Error('REHEARSAL_ISOLATION_VALUE_REFUSED');
  }
  // Removing a verifier's transaction wrapper must not discard its snapshot
  // requirement. Do not assume SERIALIZABLE substitutes for an exact native
  // REPEATABLE READ guard. The coordinator must review one compatible mode.
  const isolationRequirements = units.flatMap(unit => (unit.envelope ?? [])
    .filter(item => item.command?.startsWith('BEGIN ISOLATION LEVEL '))
    .map(item => ({file:unit.file, sourceSqlHash:unit.sourceSqlHash,
      isolation:item.command.slice('BEGIN ISOLATION LEVEL '.length)})));
  if (isolationRequirements.some(item => item.isolation !== outerIsolation)) {
    throw Error('REHEARSAL_ISOLATION_REQUIREMENT_REFUSED');
  }
  const rendered = [], census = [];
  for (const [index, unit] of units.entries()) {
    const verifier = ['verifier','final_browser_verifier'].includes(unit.kind);
    const isolateClientVariables=verifier||unit.isolateClientVariables===true;
    const pathAllowed = typeof unit.file === 'string' &&
      (/^supabase\/(?:migrations|repeatable|verification)\/[A-Za-z0-9_./-]+\.sql$/.test(unit.file)
        || verifier && /^tests\/[A-Za-z0-9_./-]+\.sql$/.test(unit.file));
    if (!['migration','repeatable','verifier','final_browser_verifier'].includes(unit.kind)
      || !pathAllowed
      || unit.file.split('/').includes('..') || typeof unit.sql !== 'string'
      || digest(unit.sql) !== unit.sqlHash) throw Error('REHEARSAL_UNIT_IDENTITY_REFUSED');
    const parsed = scanRehearsalSql(unit.sql);
    if (parsed.statements.some(s => ['BEGIN','COMMIT','END','START','ABORT','PREPARE'].includes(s.tokens[0])
      || s.tokens[0] === 'ROLLBACK' && s.tokens[1] !== 'TO')) {
      throw Error('REHEARSAL_OUTER_TRANSACTION_ESCAPE_REFUSED');
    }
    for (const meta of parsed.meta) {
      if (!/^\\(?:echo|if|elif|else|endif|set|unset|gset|pset)\b/i.test(meta.text)
        || meta.text.includes('`')
        || /^\\pset\b/i.test(meta.text) && !/^\\pset\s+pager\s+off\s*$/i.test(meta.text)
        || /^\\gset\b/i.test(meta.text) && !/^\\gset(?:\s+[A-Za-z_][A-Za-z0-9_]*)?\s*$/i.test(meta.text)) {
        throw Error('REHEARSAL_META_ESCAPE_REFUSED');
      }
      if (/^\\set\s+ON_ERROR_(?:STOP|ROLLBACK)\b/i.test(meta.text)) {
        throw Error('REHEARSAL_ERROR_CONTROL_OVERRIDE_REFUSED');
      }
      if (/^\\(?:set|unset)\s+(?:AUTOCOMMIT|ECHO|ECHO_HIDDEN|VERBOSITY|SHOW_CONTEXT|FETCH_COUNT|SINGLELINE|SINGLESTEP|HISTFILE)\b/i.test(meta.text)) {
        throw Error('REHEARSAL_CLIENT_CONTROL_OVERRIDE_REFUSED');
      }
    }
    const parts = [`\\echo HR_FILE_BEGIN ${index}`, 'reset role;', 'reset session authorization;',
      // Managed release verifiers normally get fresh backends. The single outer
      // rehearsal transaction must not retain every previous unit's plan cache.
      // PLANS preserves the transaction, locks, objects, roles and every body.
      'discard plans;',
      'set local jit=off;', 'set local pg_show_plans.is_enabled=off;', 'set local statement_timeout=0;',
      'set local work_mem=\'4MB\';', 'set local hash_mem_multiplier=1;',
      'set local max_parallel_workers_per_gather=0;', 'set local client_connection_check_interval=\'1s\';'];
    // psql variables are client state: SQL savepoint rollback cannot clear them.
    // Keep each verifier's own internal set/if behavior, with no predecessor leak.
    const clientVariables = [...new Set(['weekly_source_verification_outer_transaction',
      ...(unit.clientVariables ?? []), ...parsed.meta.flatMap(row => {
        const assigned=row.text.match(/^\\(?:set|unset)\s+([A-Za-z_][A-Za-z0-9_]*)\b/i);
        const referenced=[...row.text.matchAll(/:\{\?([A-Za-z_][A-Za-z0-9_]*)\}|:(?:'|")?([A-Za-z_][A-Za-z0-9_]*)/g)].map(x=>x[1]??x[2]);
        return [...assigned?[assigned[1]]:[],...referenced];
      })])].filter(name=>!['ON_ERROR_STOP','ON_ERROR_ROLLBACK'].includes(name)).sort();
    if (clientVariables.some(name=>!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name))) throw Error('REHEARSAL_CLIENT_VARIABLE_REFUSED');
    if (isolateClientVariables) parts.push(...clientVariables.map(name=>'\\unset '+name));
    if (verifier) parts.push(`savepoint hr_verifier_${index};`);
    parts.push(unit.sql);
    if (verifier&&observeLiveBusiness) parts.push(fixtureObservationSql(index));
    if (verifier) parts.push(`rollback to savepoint hr_verifier_${index};`, `release savepoint hr_verifier_${index};`);
    if (isolateClientVariables) parts.push(...clientVariables.map(name=>'\\unset '+name));
    parts.push(`\\echo HR_FILE_END ${index}`);
    rendered.push(parts.join('\n'));
    census.push({index, file:unit.file, kind:unit.kind, sqlHash:unit.sqlHash, statements:parsed.statements.length,
      clientVariableIsolation:isolateClientVariables, sourceSqlHash:unit.sourceSqlHash??unit.sqlHash,
      reviewedEnvelope:unit.envelope??[],clientVariables:isolateClientVariables?clientVariables:[],
      ...(unit.reviewedManagedEnvelope?{reviewedManagedEnvelope:unit.reviewedManagedEnvelope}:{})});
  }
  const sql = rendered.join('\n')+'\n';
  return {sql, sha256:digest(sql), bytes:Buffer.byteLength(sql), units:census,
    outerIsolation:outerIsolation??null, isolationRequirements};
}

// Diagnostics never retain server DETAIL/CONTEXT or arbitrary primary text.
// Source object identity comes from the independently sealed statement census.
export function closedRehearsalDiagnostic(line, lineMap, allowedCodes = new Set()) {
  const client = line.match(/^psql:<stdin>:(\d+):\s+(?:error|fatal):/);
  if(client){
    const number=Number(client[1]),location=lineMap.find(x=>number>=x.startLine&&number<=x.endLine);
    return {file:location?.file??'HARNESS_CONTROL',kind:location?.kind??'control',statement:location?.statement??null,
      object:location?.object??null,line:number,sqlstate:null,error:'PSQL_CLIENT_ERROR'};
  }
  const match = line.match(/^psql:<stdin>:(\d+):\s+(ERROR|FATAL):\s+([0-9A-Z]{5}):\s*(.*)$/);
  if (!match) return null;
  const number = Number(match[1]);
  const location = lineMap.find(x => number >= x.startLine && number <= x.endLine);
  const code = match[4].match(/^([A-Z][A-Z0-9_]{5,})(?:\s|:|$)/)?.[1] ?? null;
  return {file:location?.file ?? 'HARNESS_CONTROL', kind:location?.kind ?? 'control',
    statement:location?.statement ?? null, object:location?.object ?? null,
    line:number, sqlstate:match[3], error:code && allowedCodes.has(code) ? code : 'DATABASE_DIAGNOSTIC_WITHHELD'};
}
