import { createHash } from 'node:crypto';
import { createReadStream, createWriteStream } from 'node:fs';
import { mkdir, open, readFile, rm, stat, statfs } from 'node:fs/promises';
import { createServer } from 'node:http';
import { pipeline } from 'node:stream/promises';
import { spawn } from 'node:child_process';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

const PORT = Number(process.env.PORT || 8080);
const MAX_HEADER_BYTES = 2 * 1024 * 1024;
const MAX_INPUTS = 64;
const MAX_SINGLE_INPUT_BYTES = 512 * 1024 * 1024;
const MAX_AGGREGATE_BYTES = 1024 * 1024 * 1024;
const MAX_CAPTURE_BYTES = 64 * 1024;
const PROCESSOR_VERSION = 'cloudtms-native-qpdf-poppler-v4';

function json(res, status, body) {
  if (res.headersSent) return res.destroy();
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
  res.end(JSON.stringify(body));
}

function boundedAppend(current, chunk) {
  if (current.length >= MAX_CAPTURE_BYTES) return current;
  return (current + chunk.toString()).slice(0, MAX_CAPTURE_BYTES);
}

async function run(command, args, options = {}) {
  const timeoutMs = Math.max(1000, Math.min(300000, Number(options.timeoutMs || 120000)));
  return new Promise((resolve, reject) => {
    let settled = false;
    let stdout = '';
    let stderr = '';
    const child = spawn(command, args, { cwd: options.cwd, stdio: ['ignore','pipe','pipe'], env: { ...process.env, MAGICK_THREAD_LIMIT: '1', OMP_NUM_THREADS: '1' } });
    const finish = (error, value) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer); clearTimeout(killTimer);
      child.removeAllListeners(); child.stdout?.removeAllListeners(); child.stderr?.removeAllListeners();
      error ? reject(error) : resolve(value);
    };
    child.stdout.on('data', chunk => { stdout = boundedAppend(stdout, chunk); });
    child.stderr.on('data', chunk => { stderr = boundedAppend(stderr, chunk); });
    child.once('error', error => finish(error));
    child.once('close', code => {
      const allowed = options.allowedExitCodes || [0];
      if (allowed.includes(code)) finish(null, { stdout, stderr, code });
      else finish(Object.assign(new Error(`${command} exited ${code}: ${stderr.slice(0, 500)}`), { code: `${command.toUpperCase()}_FAILED` }));
    });
    let killTimer = null;
    const timer = setTimeout(() => {
      child.kill('SIGTERM');
      killTimer = setTimeout(() => child.kill('SIGKILL'), 2000);
      finish(Object.assign(new Error('PROCESSOR_NATIVE_TIMEOUT'), { code: 'PROCESSOR_NATIVE_TIMEOUT', category: 'PROCESSOR_TIMEOUT' }));
    }, timeoutMs);
  });
}

async function sha256File(path) {
  const hash = createHash('sha256');
  await pipeline(createReadStream(path), hash);
  return hash.digest('hex');
}

async function actualInputMetadata(file, timeoutMs) {
  const info = await pdfMetadata(file.path, timeoutMs);
  return { input_order: Number(file.input_order), input_chunk_id: file.input_chunk_id || null, r2_key: file.r2_key, sha256: await sha256File(file.path), size_bytes: (await stat(file.path)).size, page_count: info.page_count };
}

async function pdfMetadata(path, timeoutMs) {
  try { await run('qpdf', ['--check', path], { timeoutMs }); }
  catch (error) {
    if (/password|encrypted/i.test(error.message)) throw Object.assign(new Error('ASSET_PDF_ENCRYPTED'), { code: 'ASSET_PDF_ENCRYPTED', category: 'PERMANENT_INPUT' });
    throw Object.assign(new Error('ASSET_CORRUPT'), { code: 'ASSET_CORRUPT', category: 'PERMANENT_INPUT' });
  }
  const { stdout } = await run('pdfinfo', [path], { timeoutMs });
  const match = /^Pages:\s+(\d+)$/mi.exec(stdout);
  if (!match || Number(match[1]) < 1) throw Object.assign(new Error('ASSET_CORRUPT'), { code: 'ASSET_CORRUPT', category: 'PERMANENT_INPUT' });
  return { page_count: Number(match[1]), parse_verified: true, is_encrypted: false };
}

async function fileContains(path, needle) {
  const target = Buffer.from(needle);
  let overlap = Buffer.alloc(0);
  for await (const chunk of createReadStream(path)) {
    const combined = Buffer.concat([overlap, chunk]);
    if (combined.indexOf(target) >= 0) return true;
    overlap = combined.subarray(Math.max(0, combined.length - target.length + 1));
  }
  return false;
}

async function imageMetadata(path, timeoutMs) {
  const { stdout } = await run('identify', ['-format', '%m|%w|%h|%[orientation]|%n', path], { timeoutMs });
  const [format, width, height, orientation, frames] = stdout.trim().split('|');
  if (Number(frames || 1) !== 1) throw Object.assign(new Error('ASSET_MEDIA_TYPE_UNSUPPORTED'), { code: 'ASSET_MEDIA_TYPE_UNSUPPORTED', category: 'PERMANENT_INPUT' });
  const orientationMap = { TopLeft: 1, TopRight: 2, BottomRight: 3, BottomLeft: 4, LeftTop: 5, RightTop: 6, RightBottom: 7, LeftBottom: 8, Undefined: 1 };
  return { format: String(format).toUpperCase(), width_pixels: Number(width), height_pixels: Number(height), exif_orientation: orientationMap[orientation] || 1, orientation_name: orientation || 'Undefined', estimated_decoded_bytes: Number(width) * Number(height) * 4, frame_count: 1, decode_verified: true };
}

function detectMediaType(bytes) {
  if (!bytes.length) return { detected_kind: 'empty', detected_media_type: 'application/octet-stream' };
  if (bytes.length < 5) return { detected_kind: 'truncated', detected_media_type: 'application/octet-stream' };
  const ascii = bytes.subarray(0, 16).toString('latin1');
  if (ascii.startsWith('%PDF-')) return { detected_kind: 'pdf', detected_media_type: 'application/pdf' };
  if (bytes[0] === 0xff && bytes[1] === 0xd8 && bytes[2] === 0xff) return { detected_kind: 'jpeg', detected_media_type: 'image/jpeg' };
  if (bytes.length >= 8 && bytes.subarray(0, 8).equals(Buffer.from([137,80,78,71,13,10,26,10]))) return { detected_kind: 'png', detected_media_type: 'image/png' };
  if ((ascii.startsWith('RIFF') && ascii.slice(8,12) === 'WEBP') || ascii.startsWith('II*\0') || ascii.startsWith('MM\0*') || ascii.slice(4,8) === 'ftyp' || ascii.startsWith('GIF87a') || ascii.startsWith('GIF89a') || ascii.startsWith('BM')) return { detected_kind: 'unsupported', detected_media_type: 'application/octet-stream' };
  return { detected_kind: 'unknown', detected_media_type: 'application/octet-stream' };
}

async function readFramedRequest(req, directory) {
  let pending = Buffer.alloc(0);
  const iterator = req[Symbol.asyncIterator]();
  const ensure = async length => {
    while (pending.length < length) {
      const next = await iterator.next();
      if (next.done) throw Object.assign(new Error('PROCESSOR_REQUEST_TRUNCATED'), { code: 'PROCESSOR_REQUEST_TRUNCATED' });
      pending = Buffer.concat([pending, Buffer.from(next.value)]);
      if (pending.length > Math.max(MAX_HEADER_BYTES + 8, length + 1024 * 1024)) throw Object.assign(new Error('PROCESSOR_FRAME_BUFFER_EXCEEDED'), { code: 'PROCESSOR_FRAME_BUFFER_EXCEEDED' });
    }
  };
  await ensure(8);
  const headerLength = Number(pending.readBigUInt64BE(0));
  if (!Number.isSafeInteger(headerLength) || headerLength < 2 || headerLength > MAX_HEADER_BYTES) throw Object.assign(new Error('PROCESSOR_HEADER_INVALID'), { code: 'PROCESSOR_HEADER_INVALID' });
  pending = pending.subarray(8); await ensure(headerLength);
  let header;
  try { header = JSON.parse(pending.subarray(0, headerLength).toString('utf8')); }
  catch { throw Object.assign(new Error('PROCESSOR_HEADER_JSON_INVALID'), { code: 'PROCESSOR_HEADER_JSON_INVALID' }); }
  pending = pending.subarray(headerLength);
  const inputHeaders = Array.isArray(header.inputs) ? header.inputs : [];
  if (!inputHeaders.length || inputHeaders.length > MAX_INPUTS) throw Object.assign(new Error('PROCESSOR_INPUT_COUNT_INVALID'), { code: 'PROCESSOR_INPUT_COUNT_INVALID' });
  let aggregate = 0;
  for (const input of inputHeaders) {
    const size = Number(input.size_bytes);
    if (!Number.isSafeInteger(size) || size < 1 || size > MAX_SINGLE_INPUT_BYTES) throw Object.assign(new Error('PROCESSOR_INPUT_SIZE_INVALID'), { code: 'PROCESSOR_INPUT_SIZE_INVALID' });
    aggregate += size;
    if (!Number.isSafeInteger(aggregate) || aggregate > MAX_AGGREGATE_BYTES) throw Object.assign(new Error('PROCESSOR_AGGREGATE_INPUT_LIMIT'), { code: 'PROCESSOR_AGGREGATE_INPUT_LIMIT' });
  }
  const disk = await statfs(directory);
  if (Number(disk.bavail) * Number(disk.bsize) < aggregate * 2 + 128 * 1024 * 1024) throw Object.assign(new Error('PROCESSOR_TEMP_DISK_LIMIT'), { code: 'PROCESSOR_TEMP_DISK_LIMIT', category: 'POLICY_VIOLATION' });
  const files = [];
  for (let index = 0; index < inputHeaders.length; index += 1) {
    const input = inputHeaders[index];
    const path = join(directory, `input-${String(index + 1).padStart(4,'0')}.bin`);
    const output = createWriteStream(path, { flags: 'wx' });
    let remaining = Number(input.size_bytes);
    while (remaining > 0) {
      if (!pending.length) {
        const next = await iterator.next();
        if (next.done) throw Object.assign(new Error('PROCESSOR_INPUT_TRUNCATED'), { code: 'PROCESSOR_INPUT_TRUNCATED' });
        pending = Buffer.from(next.value);
      }
      const take = Math.min(remaining, pending.length);
      if (!output.write(pending.subarray(0, take))) await new Promise((resolve, reject) => { output.once('drain', resolve); output.once('error', reject); });
      pending = pending.subarray(take); remaining -= take;
    }
    output.end(); await new Promise((resolve, reject) => { output.once('finish', resolve); output.once('error', reject); });
    files.push({ ...input, path });
  }
  if (pending.length) throw Object.assign(new Error('PROCESSOR_TRAILING_DATA'), { code: 'PROCESSOR_TRAILING_DATA' });
  const trailing = await iterator.next();
  if (!trailing.done && Buffer.from(trailing.value).length) throw Object.assign(new Error('PROCESSOR_TRAILING_DATA'), { code: 'PROCESSOR_TRAILING_DATA' });
  return { header, files };
}

async function inspectAsset(file, context, timeoutMs) {
  const size = (await stat(file.path)).size;
  const prefix = Buffer.alloc(Math.min(32, size));
  const handle = await open(file.path, 'r'); let bytesRead = 0;
  try { ({ bytesRead } = await handle.read(prefix, 0, prefix.length, 0)); } finally { await handle.close(); }
  const detected = detectMediaType(prefix.subarray(0, bytesRead));
  const base = { ...detected, original_size_bytes: size, original_sha256: await sha256File(file.path), is_encrypted: false };
  if (detected.detected_kind === 'empty') throw Object.assign(new Error('ASSET_EMPTY'), { code: 'ASSET_EMPTY', category: 'PERMANENT_INPUT' });
  if (detected.detected_kind === 'truncated') throw Object.assign(new Error('ASSET_TRUNCATED'), { code: 'ASSET_TRUNCATED', category: 'PERMANENT_INPUT' });
  if (!['application/pdf','image/jpeg','image/png'].includes(detected.detected_media_type)) throw Object.assign(new Error('ASSET_MEDIA_TYPE_UNSUPPORTED'), { code: 'ASSET_MEDIA_TYPE_UNSUPPORTED', category: 'PERMANENT_INPUT' });
  if (detected.detected_media_type === 'application/pdf') return { ...base, ...await pdfMetadata(file.path, timeoutMs) };
  if (detected.detected_media_type === 'image/png' && await fileContains(file.path, 'acTL')) throw Object.assign(new Error('ASSET_MEDIA_TYPE_UNSUPPORTED'), { code: 'ASSET_MEDIA_TYPE_UNSUPPORTED', category: 'PERMANENT_INPUT' });
  const image = await imageMetadata(file.path, timeoutMs);
  const limits = context.output_profile || {};
  if (safePositive(limits.max_pixels) && image.width_pixels * image.height_pixels > Number(limits.max_pixels)) throw Object.assign(new Error('ASSET_PIXEL_LIMIT_EXCEEDED'), { code: 'ASSET_PIXEL_LIMIT_EXCEEDED', category: 'POLICY_VIOLATION' });
  if (safePositive(limits.max_decoded_bytes) && image.estimated_decoded_bytes > Number(limits.max_decoded_bytes)) throw Object.assign(new Error('ASSET_DECODE_POLICY_EXCEEDED'), { code: 'ASSET_DECODE_POLICY_EXCEEDED', category: 'POLICY_VIOLATION' });
  return { ...base, ...image };
}

function safePositive(value) { const number = Number(value); return Number.isSafeInteger(number) && number > 0 ? number : null; }

async function assertExpectedInput(file) {
  const actual = { r2_key: file.r2_key, sha256: await sha256File(file.path), size_bytes: (await stat(file.path)).size };
  if (file.expected_sha256 && file.expected_sha256 !== actual.sha256) throw Object.assign(new Error('ASSET_SOURCE_IDENTITY_CHANGED'), { code: 'ASSET_SOURCE_IDENTITY_CHANGED', category: 'IDENTITY_MISMATCH' });
  if (Number(file.size_bytes) !== Number(actual.size_bytes)) throw Object.assign(new Error('INPUT_SIZE_MISMATCH'), { code: 'INPUT_SIZE_MISMATCH', category: 'IDENTITY_MISMATCH' });
  return actual;
}

async function normaliseAsset(header, file, outputPath, timeoutMs) {
  const consumed = await assertExpectedInput(file);
  const mediaType = String(header.context?.expected_original_media_type || header.context?.detected_media_type || file.media_type || '').toLowerCase();
  if (!['application/pdf','image/jpeg','image/png'].includes(mediaType)) throw Object.assign(new Error('ASSET_MEDIA_TYPE_UNSUPPORTED'), { code: 'ASSET_MEDIA_TYPE_UNSUPPORTED', category: 'PERMANENT_INPUT' });
  if (mediaType === 'application/pdf') {
    const source = await pdfMetadata(file.path, timeoutMs);
    const start = Number(header.context?.page_range?.start || 1);
    const end = Number(header.context?.page_range?.end || header.context?.expected_source_page_count || start);
    if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start < 1 || end < start || end > source.page_count) throw Object.assign(new Error('ASSET_PAGE_RANGE_INVALID'), { code: 'ASSET_PAGE_RANGE_INVALID', category: 'PERMANENT_INPUT' });
    await run('qpdf', ['--empty','--pages',file.path,`${start}-${end}`,'--',outputPath], { timeoutMs });
    const output = await pdfMetadata(outputPath, timeoutMs);
    if (output.page_count !== end - start + 1) throw Object.assign(new Error('ASSET_OUTPUT_PAGE_COUNT_MISMATCH'), { code: 'ASSET_OUTPUT_PAGE_COUNT_MISMATCH' });
    return { ...output, actual_inputs: [{ input_order: 1, ...consumed, page_count: source.page_count }] };
  }
  if (mediaType === 'image/png' && await fileContains(file.path, 'acTL')) throw Object.assign(new Error('ASSET_MEDIA_TYPE_UNSUPPORTED'), { code: 'ASSET_MEDIA_TYPE_UNSUPPORTED', category: 'PERMANENT_INPUT' });
  const image = await imageMetadata(file.path, timeoutMs);
  const expectedFormat = mediaType === 'image/jpeg' ? 'JPEG' : 'PNG';
  if (image.format !== expectedFormat) throw Object.assign(new Error('ASSET_SOURCE_IDENTITY_CHANGED'), { code: 'ASSET_SOURCE_IDENTITY_CHANGED', category: 'IDENTITY_MISMATCH' });
  const normalisedImage = `${outputPath}.png`;
  await run('convert', [file.path,'-auto-orient','-units','PixelsPerInch','-density','150','-resize','2480x3508>','-background','white','-alpha','remove',normalisedImage], { timeoutMs });
  await run('img2pdf', ['--output',outputPath,normalisedImage], { timeoutMs });
  const output = await pdfMetadata(outputPath, timeoutMs);
  if (output.page_count !== 1) throw Object.assign(new Error('ASSET_OUTPUT_PAGE_COUNT_MISMATCH'), { code: 'ASSET_OUTPUT_PAGE_COUNT_MISMATCH' });
  return { ...output, actual_inputs: [{ input_order: 1, ...consumed, page_count: 1 }] };
}

async function mergePdfs(files, outputPath, timeoutMs) {
  const actualInputs = [];
  for (const [index, file] of files.entries()) {
    const actual = await actualInputMetadata(file, timeoutMs);
    if (actual.input_order !== index + 1) throw Object.assign(new Error('MERGE_INPUT_ORDER_MISMATCH'), { code: 'MERGE_INPUT_ORDER_MISMATCH' });
    if (file.expected_sha256 && file.expected_sha256 !== actual.sha256) throw Object.assign(new Error('INPUT_SHA256_MISMATCH'), { code: 'INPUT_SHA256_MISMATCH' });
    actualInputs.push(actual);
  }
  const args = ['--empty','--pages']; for (const file of files) args.push(file.path,'1-z'); args.push('--',outputPath);
  await run('qpdf', args, { timeoutMs });
  const output = await pdfMetadata(outputPath, timeoutMs);
  const expectedPages = actualInputs.reduce((sum, input) => sum + input.page_count, 0);
  if (output.page_count !== expectedPages) throw Object.assign(new Error('MERGE_OUTPUT_PAGE_COUNT_MISMATCH'), { code: 'MERGE_OUTPUT_PAGE_COUNT_MISMATCH' });
  return { ...output, actual_inputs: actualInputs };
}

async function verifyPdf(file, timeoutMs) {
  const consumed = await assertExpectedInput(file);
  const pdf = await pdfMetadata(file.path, timeoutMs);
  return { verified_candidate_r2_key: file.r2_key, verified_candidate_sha256: consumed.sha256, verified_candidate_size_bytes: consumed.size_bytes, actual_page_count: pdf.page_count, parse_verified: true, processor_version: PROCESSOR_VERSION };
}

function classify(error) {
  if (error?.category) return error.category;
  const code = String(error?.code || error?.message || '').toUpperCase();
  if (/TIMEOUT/.test(code)) return 'PROCESSOR_TIMEOUT';
  if (/UNSUPPORTED|EMPTY|TRUNCATED|CORRUPT|ENCRYPTED/.test(code)) return 'PERMANENT_INPUT';
  if (/LIMIT|POLICY|DISK/.test(code)) return 'POLICY_VIOLATION';
  if (/MISMATCH|IDENTITY|DUPLICATE|TRAILING/.test(code)) return 'IDENTITY_MISMATCH';
  return 'PROCESSOR_BUG';
}

async function processRequest(req, res) {
  const directory = join(tmpdir(), `cloudtms-invoice-${crypto.randomUUID()}`);
  await mkdir(directory, { recursive: false });
  try {
    const { header, files } = await readFramedRequest(req, directory);
    const action = String(header.action || '').toUpperCase();
    const timeoutMs = Math.max(1000, Math.min(300000, Number(header.action_timeout_ms || 120000)));
    if (action === 'ASSET_INSPECT') return json(res, 200, { ok: true, result: await inspectAsset(files[0], header.context || {}, timeoutMs) });
    if (action === 'DOCUMENT_VERIFY') return json(res, 200, { ok: true, result: await verifyPdf(files[0], timeoutMs) });
    const outputPath = join(directory, 'output.pdf');
    const metadata = action === 'ASSET_NORMALISE'
      ? await normaliseAsset(header, files[0], outputPath, timeoutMs)
      : action === 'PDF_MERGE' ? await mergePdfs(files, outputPath, timeoutMs) : (() => { throw Object.assign(new Error('UNSUPPORTED_ACTION'), { code: 'UNSUPPORTED_ACTION' }); })();
    const outputStat = await stat(outputPath);
    const result = { ok: true, sha256: await sha256File(outputPath), size_bytes: outputStat.size, ...metadata, processor_version: PROCESSOR_VERSION };
    res.writeHead(200, { 'content-type': 'application/pdf', 'content-length': String(outputStat.size), 'x-cloudtms-result': Buffer.from(JSON.stringify(result)).toString('base64url') });
    await pipeline(createReadStream(outputPath), res);
  } catch (error) {
    json(res, ['PERMANENT_INPUT','POLICY_VIOLATION','IDENTITY_MISMATCH'].includes(classify(error)) ? 422 : 500, { ok: false, code: String(error?.code || error?.message || 'PROCESSOR_FAILED').slice(0,120), category: classify(error), message: String(error?.message || error || 'Processor failed').slice(0,500) });
  } finally { await rm(directory, { recursive: true, force: true }); }
}

createServer((req,res) => {
  if (req.method === 'GET' && req.url === '/health') return json(res,200,{ ok: true, processor_version: PROCESSOR_VERSION });
  if (req.method === 'POST' && req.url === '/process') return void processRequest(req,res);
  return json(res,404,{ ok: false, code: 'NOT_FOUND' });
}).listen(PORT,'0.0.0.0');