import { spawnSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';

const TRANSIENT_AUDIT_FAILURE = /(?:audit endpoint returned an error|\b(?:ECONNRESET|ECONNREFUSED|EAI_AGAIN|ENETUNREACH|ETIMEDOUT)\b|\b(?:408|425|429|500|502|503|504)\s+(?:Bad Gateway|Gateway Timeout|Internal Server Error|Service Unavailable|Too Many Requests|Request Timeout))/i;

export const MAX_AUDIT_ATTEMPTS = 3;
export const AUDIT_FETCH_TIMEOUT_MS = 60_000;

export function auditArguments({ omitDev = false } = {}) {
  return [
    'audit',
    ...(omitDev ? ['--omit=dev'] : []),
    '--audit-level=high',
    '--fetch-retries=0',
    `--fetch-timeout=${AUDIT_FETCH_TIMEOUT_MS}`
  ];
}

export function classifyAuditResult(result) {
  if (result.status === 0) return 'PASS';
  const output = `${result.stdout ?? ''}\n${result.stderr ?? ''}`;
  return TRANSIENT_AUDIT_FAILURE.test(output) ? 'TRANSIENT_SERVICE_FAILURE' : 'SECURITY_FAILURE';
}

function executeAudit(args) {
  const npmCli = process.env.npm_execpath;
  if (npmCli) {
    return spawnSync(process.execPath, [npmCli, ...args], { encoding: 'utf8' });
  }
  return spawnSync(process.platform === 'win32' ? 'npm.cmd' : 'npm', args, { encoding: 'utf8' });
}

function wait(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

export async function runNpmAuditWithRetry({
  omitDev = false,
  execute = executeAudit,
  delay = wait,
  writeStdout = (value) => process.stdout.write(value),
  writeStderr = (value) => process.stderr.write(value)
} = {}) {
  const args = auditArguments({ omitDev });
  for (let attempt = 1; attempt <= MAX_AUDIT_ATTEMPTS; attempt += 1) {
    const result = execute(args);
    if (result.stdout) writeStdout(result.stdout);
    if (result.stderr) writeStderr(result.stderr);
    if (result.error) {
      writeStderr(`npm audit could not start: ${result.error.message}\n`);
      return 1;
    }
    const classification = classifyAuditResult(result);
    if (classification === 'PASS') return 0;
    if (classification === 'SECURITY_FAILURE') return Number.isInteger(result.status) ? result.status : 1;
    if (attempt === MAX_AUDIT_ATTEMPTS) {
      writeStderr(`npm audit service remained unavailable after ${MAX_AUDIT_ATTEMPTS} attempts; failing closed.\n`);
      return 1;
    }
    const delayMs = attempt * 15_000;
    writeStderr(`npm audit service was temporarily unavailable (attempt ${attempt}/${MAX_AUDIT_ATTEMPTS}); retrying in ${delayMs / 1000} seconds.\n`);
    await delay(delayMs);
  }
  return 1;
}

const isMain = process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url;
if (isMain) {
  const unsupported = process.argv.slice(2).filter((value) => value !== '--omit=dev');
  if (unsupported.length) {
    process.stderr.write(`Unsupported npm audit option: ${unsupported[0]}\n`);
    process.exitCode = 1;
  } else {
    process.exitCode = await runNpmAuditWithRetry({ omitDev: process.argv.includes('--omit=dev') });
  }
}
