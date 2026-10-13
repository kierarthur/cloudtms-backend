import {scanManagedSql, digest} from './cloudtms-managed-release-sql.mjs';

// Source-only renderer. This module opens no database/network connection.
// The post-Banking coordinator must supply exact pushed, independently reviewed
// units, hold SEND_7 before its before probes, and retain it through fresh proof.
export function renderFullRehearsalUnits(units) {
  if (!Array.isArray(units) || !units.length) throw Error('REHEARSAL_UNITS_REQUIRED');
  const rendered = [], census = [];
  for (const [index, unit] of units.entries()) {
    const verifier = ['verifier','final_browser_verifier'].includes(unit.kind);
    const pathAllowed = typeof unit.file === 'string' &&
      (/^supabase\/(?:migrations|repeatable|verification)\/[A-Za-z0-9_./-]+\.sql$/.test(unit.file)
        || verifier && /^tests\/[A-Za-z0-9_./-]+\.sql$/.test(unit.file));
    if (!['migration','repeatable','verifier','final_browser_verifier'].includes(unit.kind)
      || !pathAllowed
      || unit.file.split('/').includes('..') || typeof unit.sql !== 'string'
      || digest(unit.sql) !== unit.sqlHash) throw Error('REHEARSAL_UNIT_IDENTITY_REFUSED');
    const parsed = scanManagedSql(unit.sql);
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
    }
    const parts = [`\\echo HR_FILE_BEGIN ${index}`, 'reset role;', 'reset session authorization;',
      'set local jit=off;', 'set local pg_show_plans.is_enabled=off;', 'set local statement_timeout=0;'];
    // psql variables are client state: SQL savepoint rollback cannot clear them.
    // Keep each verifier's own internal set/if behavior, with no predecessor leak.
    if (verifier) parts.push('\\unset weekly_source_verification_outer_transaction', `savepoint hr_verifier_${index};`);
    parts.push(unit.sql);
    if (verifier) parts.push(`rollback to savepoint hr_verifier_${index};`, `release savepoint hr_verifier_${index};`,
      '\\unset weekly_source_verification_outer_transaction');
    parts.push(`\\echo HR_FILE_END ${index}`);
    rendered.push(parts.join('\n'));
    census.push({index, file:unit.file, kind:unit.kind, sqlHash:unit.sqlHash, statements:parsed.statements.length,
      clientVariableIsolation:verifier});
  }
  const sql = rendered.join('\n')+'\n';
  return {sql, sha256:digest(sql), bytes:Buffer.byteLength(sql), units:census};
}

// Diagnostics never retain server DETAIL/CONTEXT or arbitrary primary text.
// Source object identity comes from the independently sealed statement census.
export function closedRehearsalDiagnostic(line, lineMap, allowedCodes = new Set()) {
  const match = line.match(/^psql:<stdin>:(\d+):\s+(ERROR|FATAL):\s+([0-9A-Z]{5}):\s*(.*)$/);
  if (!match) return null;
  const number = Number(match[1]);
  const location = lineMap.find(x => number >= x.startLine && number <= x.endLine);
  const code = match[4].match(/^([A-Z][A-Z0-9_]{5,})(?:\s|$)/)?.[1] ?? null;
  return {file:location?.file ?? 'HARNESS_CONTROL', kind:location?.kind ?? 'control',
    statement:location?.statement ?? null, object:location?.object ?? null,
    line:number, sqlstate:match[3], error:code && allowedCodes.has(code) ? code : 'DATABASE_DIAGNOSTIC_WITHHELD'};
}
