import crypto from 'node:crypto';
import { spawn } from 'node:child_process';
import { StringDecoder } from 'node:string_decoder';

export class ManagedPsqlError extends Error {
  constructor(code, sqlState = null) {
    super(`Managed database writer failed: ${code}${sqlState ? ` (${sqlState})` : ''}`);
    this.code = code;
    this.sqlState = sqlState;
  }
}

// No credential, SQL, server DETAIL/CONTEXT, or raw argv is returned in errors.
// One connection is both the writer and the session advisory-lock owner.
export class ManagedPsqlSession {
  constructor({ bin = 'psql', args, cwd, env, spawnImpl = spawn,
    timeoutMs = 1_800_000, maxOutputBytes = 8 * 1024 * 1024,
    maxCommandBytes = 32 * 1024 * 1024, closeTimeoutMs = 10_000 } = {}) {
    if (!Array.isArray(args) || !Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 7_200_000
      || !Number.isSafeInteger(maxOutputBytes) || maxOutputBytes < 1
      || !Number.isSafeInteger(maxCommandBytes) || maxCommandBytes < 1) throw new ManagedPsqlError('CONFIG_INVALID');
    this.timeoutMs = timeoutMs; this.maxOutputBytes = maxOutputBytes;
    this.maxCommandBytes = maxCommandBytes; this.closeTimeoutMs = closeTimeoutMs;
    this.decoder = new StringDecoder('utf8');
    this.pending = null; this.failure = null; this.finished = false; this.closing = false;
    this.line = ''; this.stderr = ''; this.number = 0;
    try { this.child = spawnImpl(bin, args, { cwd, env, stdio: ['pipe', 'pipe', 'pipe'], windowsHide: true }); }
    catch { throw new ManagedPsqlError('START_FAILED'); }
    this.closed = new Promise(resolve => this.child.once('close', code => {
      this.finished = true;
      if (!this.closing || code !== 0) this.fail(new ManagedPsqlError('CONNECTION_CLOSED', this.sqlState()));
      resolve(code);
    }));
    this.child.once('error', () => this.fail(new ManagedPsqlError('START_FAILED')));
    this.child.stdin.on('error', () => this.fail(new ManagedPsqlError('WRITE_FAILED')));
    this.child.stderr.on('data', chunk => {
      this.stderr = (this.stderr + chunk.toString('utf8')).slice(-65_536);
      if(this.pending){this.pending.bytes+=chunk.length;if(this.pending.bytes>this.maxOutputBytes)this.fail(new ManagedPsqlError('OUTPUT_LIMIT'));}
    });
    this.child.stdout.on('data', chunk => this.receive(chunk));
  }

  sqlState() {
    // VERBOSITY=sqlstate emits only the five-character SQLSTATE after ERROR.
    return this.stderr.match(/(?:ERROR|FATAL):\s*([0-9A-Z]{5})(?:\s|$)/)?.[1] ?? null;
  }

  fail(error) {
    this.failure ??= error;
    if (this.pending) {
      const pending = this.pending; this.pending = null;
      clearTimeout(pending.timer); pending.reject(error);
    }
    if (!this.finished && !this.closing) { this.closing = true; this.child.kill(); }
  }

  receive(chunk) {
    if (!this.pending) return;
    this.pending.bytes += chunk.length;
    if (this.pending.bytes > this.maxOutputBytes) { this.fail(new ManagedPsqlError('OUTPUT_LIMIT')); return; }
    this.line += this.decoder.write(chunk);
    let end;
    while ((end = this.line.indexOf('\n')) >= 0) {
      const line = this.line.slice(0, end).replace(/\r$/, ''); this.line = this.line.slice(end + 1);
      const pending = this.pending;
      if (!pending) return;
      if (line === pending.start) { pending.started = true; continue; }
      if (line === pending.end) {
        if (!pending.started) { this.fail(new ManagedPsqlError('ACK_INVALID')); return; }
        this.pending = null; clearTimeout(pending.timer); this.line = '';
        pending.resolve(pending.lines.join('\n').trim()); return;
      }
      if (pending.started) pending.lines.push(line);
    }
  }

  execute(script) {
    if (this.failure) return Promise.reject(this.failure);
    if (this.finished || this.closing) return Promise.reject(new ManagedPsqlError('CONNECTION_CLOSED'));
    if (this.pending) return Promise.reject(new ManagedPsqlError('CONCURRENT_COMMAND'));
    if (typeof script !== 'string' || Buffer.byteLength(script) > this.maxCommandBytes) {
      return Promise.reject(new ManagedPsqlError('COMMAND_LIMIT'));
    }
    const nonce = crypto.randomBytes(16).toString('hex');
    const start = `__CLOUDTMS_MANAGED_${++this.number}_${nonce}_START__`;
    const end = `__CLOUDTMS_MANAGED_${this.number}_${nonce}_END__`;
    this.stderr = ''; this.line = ''; this.decoder = new StringDecoder('utf8');
    return new Promise((resolve, reject) => {
      this.pending = { start, end, started: false, bytes: 0, lines: [], resolve, reject,
        timer: setTimeout(() => this.fail(new ManagedPsqlError('COMMAND_TIMEOUT')), this.timeoutMs) };
      this.child.stdin.write(`\\echo ${start}\n${script}\n\\echo ${end}\n`, error => {
        if (error) this.fail(new ManagedPsqlError('WRITE_FAILED'));
      });
    });
  }

  async close() {
    if (!this.finished && !this.closing) {
      if (this.pending) this.fail(new ManagedPsqlError('CLOSED_WITH_PENDING_COMMAND'));
      else { this.closing = true; this.child.stdin.end('\\q\n'); }
    }
    let timer;
    try {
      const code = await Promise.race([this.closed, new Promise((_, reject) => {
        timer = setTimeout(() => { this.child.kill(); reject(new ManagedPsqlError('CLOSE_TIMEOUT')); }, this.closeTimeoutMs);
      })]);
      if (code !== 0 && !this.failure) throw new ManagedPsqlError('CLOSE_FAILED');
    } finally { clearTimeout(timer); }
  }
}
