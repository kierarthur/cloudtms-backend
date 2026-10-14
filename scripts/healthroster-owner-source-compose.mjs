import { createHash } from 'node:crypto';

export const sourceSha256 = value => createHash('sha256').update(value).digest('hex');

// Whole-owner source composition only. This does not execute SQL or certify
// database canonical hashes, permissions, concurrency or financial behavior.
export function composeOwnerSource({ source, expectedSha256, hunks }) {
  if (typeof source !== 'string' || sourceSha256(source) !== expectedSha256)
    throw new Error('OWNER_SOURCE_PIN_MISMATCH');
  if (!Array.isArray(hunks) || !hunks.length) throw new Error('OWNER_HUNKS_REQUIRED');
  let output = source;
  const applied = [];
  for (const hunk of hunks) {
    if (typeof hunk?.name !== 'string' || !hunk.name
      || typeof hunk.before !== 'string' || !hunk.before
      || typeof hunk.after !== 'string' || !hunk.after || hunk.before === hunk.after)
      throw new Error('OWNER_HUNK_INVALID');
    if (output.split(hunk.before).length !== 2) throw new Error(`OWNER_ANCHOR_NOT_UNIQUE: ${hunk.name}`);
    const beforeOffset = output.indexOf(hunk.before);
    output = output.replace(hunk.before, () => hunk.after);
    applied.push({ ...hunk, byte_offset: Buffer.byteLength(output.slice(0, beforeOffset)) });
  }
  // Reverse from exact byte offsets, rather than depending on an after-string
  // remaining globally unique in a large SQL function or whole JavaScript file.
  let restored = Buffer.from(output);
  for (const hunk of [...applied].reverse()) {
    const after = Buffer.from(hunk.after), at = hunk.byte_offset;
    if (!restored.subarray(at, at + after.length).equals(after)) throw new Error('OWNER_REVERSE_OFFSET_MISMATCH');
    restored = Buffer.concat([restored.subarray(0, at), Buffer.from(hunk.before), restored.subarray(at + after.length)]);
  }
  if (!restored.equals(Buffer.from(source))) throw new Error('OWNER_WHOLE_REVERSE_MISMATCH');
  return { output, reverse: { schema: 'HEALTHROSTER_WHOLE_OWNER_COMPOSITION_V1',
    source_only: true, native: false, before_bytes: Buffer.byteLength(source), before_sha256: expectedSha256,
    after_bytes: Buffer.byteLength(output), after_sha256: sourceSha256(output), hunks: applied,
    whole_reverse_verified: true } };
}
