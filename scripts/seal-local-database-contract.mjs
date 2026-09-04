import {
  canonicalContractHash,
  exportContract,
  psql,
  writeJson,
} from './cloudtms-db-release-lib.mjs';

const outputPath = 'supabase/release/current-contract.json';
const localHosts = new Set(['localhost', '127.0.0.1', '::1', 'host.docker.internal']);

function requireLocalPostgres17() {
  const rawUrl = process.env.CLOUDTMS_DATABASE_URL;
  if (!rawUrl) throw new Error('CLOUDTMS_DATABASE_URL is required');

  const url = new URL(rawUrl);
  if (!localHosts.has(url.hostname)) {
    throw new Error('Contract sealing is local-only; a hosted database was refused');
  }

  process.env.CLOUDTMS_ALLOW_LOCAL = '1';
  const versionNumber = Number(psql({ sql: "select current_setting('server_version_num')" }));
  if (!Number.isInteger(versionNumber) || Math.floor(versionNumber / 10000) !== 17) {
    throw new Error('Contract sealing requires the already-proved local PostgreSQL 17 database');
  }

  const ownsPrivateSchema = psql({
    sql: "select (nspowner=(select oid from pg_catalog.pg_roles where rolname=current_user))::text from pg_catalog.pg_namespace where nspname='private'",
  });
  if (ownsPrivateSchema !== 'true') {
    throw new Error('Connect as the local proof release owner so provider-neutral ownership is generated correctly');
  }
}

try {
  requireLocalPostgres17();
  const contract = exportContract();
  writeJson(outputPath, contract);
  console.log(`Local PostgreSQL 17 contract sealed: ${canonicalContractHash(contract)}`);
} catch (error) {
  console.error(`Local contract sealing failed: ${error.message}`);
  process.exitCode = 1;
}
