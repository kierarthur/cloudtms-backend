// Offline full-byte verifier/include read and execution-call census.
// This does not decide safety from routine names; the review supplies decisions
// after examining executed paths, dynamic SQL and actual callee definitions.
import { readFileSync, writeFileSync } from 'node:fs';
import { resolve, dirname, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
const root = resolve(process.argv[2]);
const out = dirname(fileURLToPath(import.meta.url));
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const release = JSON.parse(readFileSync(resolve(root, 'supabase/release/current-release.json')));
function scan(input) {
  let code = ''; const strings = []; let i = 0;
  while (i < input.length) {
    if (input.startsWith('--', i)) { const end = input.indexOf('\n', i); i = end < 0 ? input.length : end; continue; }
    if (input.startsWith('/*', i)) {
      let depth = 1; i += 2;
      while (i < input.length && depth) { if (input.startsWith('/*', i)) { depth++; i += 2; } else if (input.startsWith('*/', i)) { depth--; i += 2; } else i++; }
      continue;
    }
    if (input[i] === "'") {
      const start = i++; let value = ''; const escape = /(?:^|[^a-z_0-9])e$/i.test(input.slice(Math.max(0,start-2),start));
      while (i < input.length) { if (escape && input[i] === '\\') { value += input[i] + (input[i+1] ?? ''); i += 2; }
        else if (input[i] === "'" && input[i + 1] === "'") { value += "'"; i += 2; }
        else if (input[i] === "'") { i++; break; } else value += input[i++]; }
      strings.push({ start, value }); code += ' STRING_LITERAL '; continue;
    }
    code += input[i++];
  }
  const names = text => [...new Set([...text.matchAll(/\b((?:public|private|pg_temp|net|extensions|http|pg_catalog)\.[a-z_][a-z_0-9]*)\s*\(/gi)].map(m => m[1]))].sort();
  const executableStrings = strings.filter(s => /^\s*(?:select|perform|call|execute|insert|update|delete|create|alter|drop)\b/i.test(s.value));
  const dynamic = executableStrings.flatMap(s => names(s.value).map(call => ({ call, line: input.slice(0, s.start).split('\n').length,
    preview: s.value.slice(0, 180).replace(/\s+/g, ' ') })));
  return { calls: names(code), dynamicOrInspectedSqlLiterals: dynamic,
    directCodeLines: code.split('\n').filter(line => /\b(?:perform|call|execute)\b|\b(?:public|private|net|http)\.[a-z_][a-z_0-9]*\s*\(/i.test(line)).map(line => line.trim()) };
}
const all = new Map();
function read(path, ancestors = []) {
  const full = resolve(root, path); if (ancestors.includes(full)) throw new Error(`Include cycle: ${path}`);
  if (all.has(full)) return all.get(full);
  const bytes = readFileSync(full); const body = bytes.toString('utf8');
  const item = { path: relative(root, full).replaceAll('\\', '/'), bytes: bytes.length, sha256: hash(bytes),
    fullBytesRead: true, lines: body.split('\n').length, ...scan(body), includes: [] };
  all.set(full, item);
  for (const match of body.matchAll(/^\s*\\ir\s+([^\r\n]+)$/gm)) {
    const target = match[1].trim().replace(/^['"]|['"]$/g, '');
    item.includes.push(read(relative(root, resolve(dirname(full), target)), [...ancestors, full]).path);
  }
  return item;
}
const verifiers = release.verificationFiles.slice(112, 168).map((path, i) => ({ index: i + 112, ...read(path) }));
writeFileSync(resolve(out, 'VERIFIER_EXECUTION_CENSUS_112_167.json'), JSON.stringify({ sourceCommit: '547c74da',
  releaseSha256: hash(readFileSync(resolve(root, 'supabase/release/current-release.json'))), verifiers,
  includeBodies: [...all.values()].filter(item => !verifiers.some(v => v.path === item.path)) }, null, 2) + '\n');
for (const v of verifiers) console.log(JSON.stringify({ index: v.index, path: v.path, includes: v.includes,
  calls: v.calls.filter(n => !n.startsWith('pg_catalog.')), dynamic: [...new Set(v.dynamicOrInspectedSqlLiterals.map(d => d.call))] }));
