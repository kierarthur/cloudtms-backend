import assert from 'node:assert/strict';

const nodeMajor = Number.parseInt(process.versions.node.split('.')[0], 10);
assert.equal(nodeMajor, 24, `Node 24 is required; received ${process.versions.node}`);

const userAgent = String(process.env.npm_config_user_agent || '');
const npmMatch = userAgent.match(/(?:^|\s)npm\/(\d+)\./);
assert.ok(npmMatch, 'Run this check through npm so the npm version is authoritative');
assert.equal(Number.parseInt(npmMatch[1], 10), 11, `npm 11 is required; received ${userAgent}`);

console.log(`Approved toolchain: Node ${process.versions.node}, npm ${npmMatch[1]}.x`);
