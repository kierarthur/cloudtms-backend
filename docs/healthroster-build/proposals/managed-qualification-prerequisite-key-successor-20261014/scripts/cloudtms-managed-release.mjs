import { buildQualification, domainHash } from './weekly-source-protected-release-qualification.mjs';
import { buildPrerequisiteQualification, assertManagedPrerequisiteEvidence, proveCommittedSources, installedQualificationCatalogueEvidence } from './weekly-source-protected-prerequisite-evidence.mjs';
import { assertManagedOperationalEvidence } from './weekly-source-protected-operational-evidence.mjs';
import { buildTestSetupAuthorization } from './weekly-source-protected-test-setup-evidence.mjs';
import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { canonical, digest, exactAtomicBody, literal, MANAGED_RELEASE_LOCK,
  MANAGED_RELEASE_VERSION, managedManifestHash, readManagedClosure, scanManagedSql } from './cloudtms-managed-release-sql.mjs';
import { BOOTSTRAP_PIN, BASELINE_MIGRATION_PREFIX, MANAGED_EXCEPTIONS, executionPolicy } from './cloudtms-managed-execution-policy.mjs';
import { READER_PREPARATION, readerReleasePhases, readerActivationSql } from './weekly-source-reader-release.mjs';
import { logicalPostgresExecutablePolicy, mapExecutableSqlSource } from './cloudtms-db-release-lib.mjs';
import { closeFinalDefinerSelection, routineDefinitions } from './bpay-next-final-definers.mjs';

const json = value => `${literal(JSON.stringify(value))}::jsonb`;
const readJson = (root, file) => JSON.parse(fs.readFileSync(path.join(root, file), 'utf8'));
// One historical seed writes after its predecessor enabled FORCE RLS but
// before the later permanent Miget owner policy. Keep both original migrations
// immutable. This owner-only policy exists solely inside that seed's atomic
// DDL/data/ledger transaction and is removed before its receipt can commit.
export const OWNER_RLS_ENVELOPE = Object.freeze({
  version: 'CLOUDTMS_MANAGED_OWNER_RLS_ENVELOPE_V1',
  path: 'supabase/migrations/23082026_1337_manager_email_candidate_identity_defaults.sql',
  contentSha256: '37ae95afec1d5c11a7f61ea127538910360d194fb0457f57c209c3414c0bd389',
  closureSha256: '8f09eabdcba2d936db4aebf87fa2ed59dd1b0b5b91c0375dbfdbc7ba117f3790',
  schema: 'public', table: 'candidate_manager_email_template_versions',
  policy: 'cloudtms_managed_installer_owner_seed', expectedPolicyCount: 0,
});
// Two immutable migrations name this existing table-free guard before the
// normal repeatable phase. Declare its exact authority in their atomic packet;
// never replay the whole guards file early or use a permissive trigger shim.
export const IMMUTABLE_GUARD_DEPENDENCY = Object.freeze({
  version: 'CLOUDTMS_IMMUTABLE_GUARD_DEPENDENCY_V1',
  source: 'supabase/repeatable/03102026_1342_banking_pay_next_guards_v1.sql',
  sourceSha256: 'd29decd59c88c2620fe658b5d96f0deace19063cd0eaddf150ea7cf09c7b60af',
  sourceClosureSha256: '82ac1db0302dd5d77c1acbe6b708edc811a64dd8ac5f99dbe441a0cb5f7a180c',
  signature: 'private.bpay_next_effect_immutable_v1()',
  statementSha256: 'b73804883751b33d8f200643319ebd95309292b6b3cdf8ae1d8f9fcb6121052b',
  consumers: Object.freeze({
    'supabase/migrations/11102026_2129_banking_pay_next_case_hold_audit.sql': Object.freeze({
      contentSha256: '41b429dd1447d1dbc4e63f72a91bddbde3d8916772a0dcc73feca177518ba91e',
      closureSha256: 'ee3378237e29e49f6b9ad5854afac300be42b72ea0024b94f3403106ffbcdd8d',
    }),
    'supabase/migrations/12102026_0645_banking_pay_next_confirm_refresh_membership.sql': Object.freeze({
      contentSha256: '28785f633847c5fecfd937793d8cdc841299f7847975da779b8019a89a345763',
      closureSha256: '430ccb09bed6de441a5f150b2422d20a1c37f2293c2a8a3c9a1bab2e7ed89186',
    }),
  }),
});
export const emptySchemaSql = `select count(*) from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname in ('public','private') and c.relkind in ('r','p','v','S');`;
export const acquireWriterSql = `select pg_catalog.pg_try_advisory_lock(pg_catalog.hashtextextended(${literal(MANAGED_RELEASE_LOCK)},0));`;

// RESET ALL does not release session advisory locks. DISCARD ALL does, and is
// deliberately forbidden. Temp objects/psql variables are per-file, as before.
export const resetWriterSql = `CLOSE ALL; RESET ROLE; RESET SESSION AUTHORIZATION; RESET ALL;
DEALLOCATE ALL; UNLISTEN *; DISCARD TEMP; DISCARD PLANS; DISCARD SEQUENCES;
\\set ON_ERROR_STOP on
\\set VERBOSITY sqlstate
\\set SHOW_CONTEXT never
\\set ECHO none
\\unset invoice_presentation_active_work
\\unset bpay_backfill_batch
\\unset cloudtms_environment
`;
export const assertWriterSql = `do $managed_writer$ begin
  if not exists(select 1 from pg_catalog.pg_locks where locktype='advisory'
    and pid=pg_catalog.pg_backend_pid() and granted and mode='ExclusiveLock' and objsubid=1
    and classid=((pg_catalog.hashtextextended(${literal(MANAGED_RELEASE_LOCK)},0)>>32)&4294967295)::oid
    and objid=(pg_catalog.hashtextextended(${literal(MANAGED_RELEASE_LOCK)},0)&4294967295)::oid)
  then raise exception 'CLOUDTMS_MANAGED_WRITER_GUARD_LOST'; end if;
end $managed_writer$;`;

function validateContext(context) {
  if (!context || !['NEW','UPGRADE'].includes(context.mode) || !['TEST','LIVE'].includes(context.environment)
    || typeof context.releaseId !== 'string' || !context.releaseId || !/^[0-9a-f]{40}$/.test(context.gitCommit)
    || !/^[0-9a-f]{64}$/.test(context.expectedHash) || !/^[0-9a-f]{64}$/.test(context.manifestHash)
    || typeof context.customerKey !== 'string' || !context.expectedDatabase) throw Error('MANAGED_RELEASE_CONTEXT_INVALID');
}

export function managedEvidence(context, extra = {}) {
  validateContext(context);
  return { managed_release: { version: MANAGED_RELEASE_VERSION, manifest_sha256: context.manifestHash,
    owner_rls_envelope_version: OWNER_RLS_ENVELOPE.version,
    immutable_guard_dependency_version: IMMUTABLE_GUARD_DEPENDENCY.version,
    expected_database: context.expectedDatabase, environment: context.environment, customer_key: context.customerKey,
    bootstrap_complete: context.mode === 'NEW' }, ...extra };
}

export function identitySql(context, insert = false) {
  validateContext(context);
  const customer = context.customerKey ? literal(context.customerKey) : 'null';
  return `${insert ? `insert into private.cloudtms_database_identity(singleton,environment,customer_key) values(true,${literal(context.environment)},${customer});` : ''}
    do $managed_identity$ begin
      if pg_catalog.current_database()<>${literal(context.expectedDatabase)} or not exists(
        select 1 from private.cloudtms_database_identity where singleton
          and environment=${literal(context.environment)} and customer_key is not distinct from ${customer})
      then raise exception 'CLOUDTMS_DATABASE_IDENTITY_MISMATCH'; end if;
    end $managed_identity$;`;
}

const releaseMatch = c => `r.git_commit=${literal(c.gitCommit)} and r.repository_contract_sha256=${literal(c.expectedHash)}
  and r.install_mode=${literal(c.mode)} and coalesce(r.evidence_json->'managed_release' @> ${json(managedEvidence(c).managed_release)},false)`;

export function resumeCheckSql(context) {
  validateContext(context);
  return `select exists(select 1 from private.cloudtms_database_releases r where r.release_id=${literal(context.releaseId)}
    and ${releaseMatch(context)} and r.status in ('APPLYING','FAILED','VERIFIED'))::text;`;
}

// The live connection owns the session mutex before entering this transaction.
// Never time-adopt or reconcile another APPLYING attempt, even after a crash.
export function managedAdmissionSql(context, bootstrap = false) {
  validateContext(context);
  return `${assertWriterSql}
    ${identitySql(context)}
    do $managed_admission$ begin
      if exists(select 1 from private.cloudtms_database_releases where status='APPLYING'
        and release_id<>${literal(context.releaseId)}) then raise exception 'CLOUDTMS_DATABASE_RELEASE_ALREADY_APPLYING'; end if;
      if exists(select 1 from private.cloudtms_database_releases r where r.release_id=${literal(context.releaseId)}
        and not (${releaseMatch(context)})) then raise exception 'CLOUDTMS_DATABASE_RELEASE_IDENTITY_MISMATCH'; end if;
      ${bootstrap ? '' : `if ${literal(context.mode)}='NEW' and not exists(select 1 from private.cloudtms_database_releases r
        where r.release_id=${literal(context.releaseId)} and ${releaseMatch(context)})
        then raise exception 'CLOUDTMS_NEW_MANAGED_BOOTSTRAP_RECEIPT_REQUIRED'; end if;`}
    end $managed_admission$;
    insert into private.cloudtms_database_releases(release_id,git_commit,repository_contract_sha256,installed_contract_sha256,
      install_mode,status,completed_at_utc,evidence_json) values(${literal(context.releaseId)},${literal(context.gitCommit)},
      ${literal(context.expectedHash)},${literal(context.expectedHash)},${literal(context.mode)},'APPLYING',null,${json(managedEvidence(context))})
    on conflict(release_id) do update set status='APPLYING',completed_at_utc=null,
      evidence_json=private.cloudtms_database_releases.evidence_json||excluded.evidence_json;`;
}

export function migrationLedgerSql(item, context) {
  return `insert into private.cloudtms_migration_ledger(path,content_sha256,first_release_id)
    values(${literal(item.path)},${literal(item.sha256)},${literal(context.releaseId)});`;
}
export function repeatableLedgerSql(item, context) {
  return `insert into private.cloudtms_repeatable_ledger(path,closure_sha256,last_release_id)
    values(${literal(item.path)},${literal(item.sha256)},${literal(context.releaseId)}) on conflict(path) do update
    set closure_sha256=excluded.closure_sha256,last_release_id=excluded.last_release_id,applied_at_utc=pg_catalog.clock_timestamp();`;
}

export function pendingInventory(current, installed) {
  const actual = new Map(current.migrations.map(row => [row.path, row.sha256]));
  const migrations = new Map(installed.migrations.map(row => [row.path, row.sha256]));
  const repeatables = new Map(installed.repeatables.map(row => [row.path, row.sha256]));
  if (migrations.size !== installed.migrations.length || repeatables.size !== installed.repeatables.length) throw Error('MANAGED_LEDGER_DUPLICATE');
  for (const [file, hash] of migrations) {
    if (actual.get(file) !== hash) throw Error(`MANAGED_MIGRATION_LEDGER_MISMATCH: ${file}`);
  }
  return { migrations: current.migrations.filter(row => !migrations.has(row.path)),
    repeatables: current.repeatables.filter(row => repeatables.get(row.path) !== row.sha256) };
}

export function compileManagedUnit(item, kind, { root, mapSource = value => value, executableFile }) {
  const closure = readManagedClosure(item.path, root, [], mapSource);
  const expected = kind === 'migration' ? digest(closure.ordered[0].source) : closure.closureHash;
  if (expected !== item.sha256) throw Error(`MANAGED_SOURCE_CHANGED: ${item.path}`);
  const ownerRlsEnvelope = ownerRlsEnvelopeFor(closure, kind);
  const immutableGuardDependency = immutableGuardDependencyFor(closure, kind, root);
  // Dollar bodies remain opaque to the packaging lexer. This conservative
  // refusal is additional guard integrity, not a claim that arbitrary DO
  // effects were proved safe by their statement head.
  if (/\bDISCARD\s+ALL\b|\bpg_advisory_unlock(?:_all|_shared)?\s*\(/i.test(closure.expanded))throw Error(`MANAGED_SOURCE_WRITER_GUARD_COMMAND: ${item.path}`);
  // Consult an existing exception pin even if its packaging became atomic.
  const exception = executionPolicy(closure);
  if (exception) {
    assertConnectionPreservingException(closure,exception);
    if (!executableFile) throw Error('MANAGED_EXECUTABLE_TREE_REQUIRED');
    const executable=executableFile(item.path);
    const executableRoot=path.resolve(executable,...item.path.split('/').map(()=> '..'));
    // A canonical mapped include tree is task-owned and immutable. Detect a
    // source edit/cache mismatch rather than executing different copied bytes.
    for(const row of closure.ordered){
      const copied=canonical(fs.readFileSync(path.join(executableRoot,row.path),'utf8'));
      if(copied!==mapSource(row.source,row.path))throw Error(`MANAGED_EXECUTABLE_TREE_CHANGED: ${row.path}`);
    }
    return { item, kind, closure, exception, script: `\\ir ${quotePsqlPath(executable)}\n` };
  }
  try { return { item, kind, closure, body: exactAtomicBody(closure.expanded), ownerRlsEnvelope, immutableGuardDependency }; }
  catch (error) { throw Error(`MANAGED_EXECUTION_ENVELOPE_UNCLASSIFIED: ${item.path} (${error.message})`); }
}

function immutableGuardDependencyFor(closure, kind, root) {
  const scope = IMMUTABLE_GUARD_DEPENDENCY.consumers[closure.path];
  if (!scope) return null;
  if (kind !== 'migration' || closure.files.size !== 1 || closure.ordered.length !== 1
      || closure.files.get(closure.path) !== scope.contentSha256 || closure.closureHash !== scope.closureSha256) {
    throw Error('MANAGED_IMMUTABLE_GUARD_CONSUMER_SOURCE_MISMATCH');
  }
  const source = readManagedClosure(IMMUTABLE_GUARD_DEPENDENCY.source, root);
  if (source.files.size !== 1 || source.ordered.length !== 1
      || source.files.get(source.path) !== IMMUTABLE_GUARD_DEPENDENCY.sourceSha256
      || source.closureHash !== IMMUTABLE_GUARD_DEPENDENCY.sourceClosureSha256) {
    throw Error('MANAGED_IMMUTABLE_GUARD_SOURCE_MISMATCH');
  }
  const text = source.ordered[0].source;
  const statements = scanManagedSql(text).statements.map(row => text.slice(row.start, row.end))
    .filter(statement => routineDefinitions(statement, source.path).includes(IMMUTABLE_GUARD_DEPENDENCY.signature));
  if (statements.length !== 1 || digest(statements[0]) !== IMMUTABLE_GUARD_DEPENDENCY.statementSha256) {
    throw Error('MANAGED_IMMUTABLE_GUARD_STATEMENT_MISMATCH');
  }
  return { scope, statement: statements[0] };
}

function immutableGuardDependencySql(unit) {
  const scope = IMMUTABLE_GUARD_DEPENDENCY.consumers[unit.item.path];
  const dependency = unit.immutableGuardDependency;
  if (!scope || unit.kind !== 'migration' || unit.exception || unit.ownerRlsEnvelope || dependency?.scope !== scope
      || unit.item.sha256 !== scope.contentSha256 || unit.closure.files.size !== 1 || unit.closure.ordered.length !== 1
      || digest(unit.closure.ordered[0].source) !== scope.contentSha256
      || unit.closure.files.get(unit.item.path) !== scope.contentSha256 || unit.closure.closureHash !== scope.closureSha256
      || unit.body !== exactAtomicBody(unit.closure.expanded)
      || unit.body !== exactAtomicBody(mapExecutableSqlSource(unit.closure.ordered[0].source,unit.item.path))
      || digest(dependency.statement) !== IMMUTABLE_GUARD_DEPENDENCY.statementSha256) {
    throw Error('MANAGED_IMMUTABLE_GUARD_SCOPE_INVALID');
  }
  const tag = '$function$',start = dependency.statement.indexOf('as '+tag)+3+tag.length;
  const body = dependency.statement.slice(start,dependency.statement.lastIndexOf(tag));
  const bodyMd5 = createHash('md5').update(body).digest('hex');
  const declaration = mapExecutableSqlSource(dependency.statement+`
alter function private.bpay_next_effect_immutable_v1() owner to postgres;
revoke all on function private.bpay_next_effect_immutable_v1() from public, anon, authenticated, service_role;
`, IMMUTABLE_GUARD_DEPENDENCY.source);
  return `do $managed_immutable_dependency$
  declare v_guard oid := pg_catalog.to_regprocedure('private.bpay_next_effect_immutable_v1()');
  begin
    if v_guard is null then
      ${declaration}
    elsif not exists(select 1 from pg_catalog.pg_proc p
      join pg_catalog.pg_language l on l.oid=p.prolang
      where p.oid=v_guard and p.proowner=current_user::regrole::oid
        and l.lanname='plpgsql' and p.prorettype='pg_catalog.trigger'::regtype
        and p.prokind='f' and p.pronargs=0 and not p.proretset and not p.prosecdef
        and not p.proisstrict and not p.proleakproof and p.provolatile='v' and p.proparallel='u'
        and pg_catalog.md5(p.prosrc)=${literal(bodyMd5)}
        and p.proconfig=array['search_path=pg_catalog, private']::text[]
        and not exists(select 1 from pg_catalog.aclexplode(coalesce(p.proacl,pg_catalog.acldefault('f',p.proowner))) a
          where a.grantee<>p.proowner or a.privilege_type<>'EXECUTE')) then
      raise exception 'MANAGED_IMMUTABLE_GUARD_PREDECESSOR_MISMATCH' using errcode='23514';
    end if;
  end $managed_immutable_dependency$;`;
}

function ownerRlsEnvelopeFor(closure, kind) {
  if (closure.path !== OWNER_RLS_ENVELOPE.path) return null;
  if (kind !== 'migration' || closure.files.size !== 1 || closure.ordered.length !== 1
    || closure.files.get(OWNER_RLS_ENVELOPE.path) !== OWNER_RLS_ENVELOPE.contentSha256
    || closure.closureHash !== OWNER_RLS_ENVELOPE.closureSha256) {
    throw Error('MANAGED_OWNER_RLS_ENVELOPE_SOURCE_MISMATCH');
  }
  return OWNER_RLS_ENVELOPE;
}

function ownerRlsEnvelopeSql(unit) {
  if (unit.ownerRlsEnvelope !== OWNER_RLS_ENVELOPE || unit.exception
    || unit.item.path !== OWNER_RLS_ENVELOPE.path
    || unit.item.sha256 !== OWNER_RLS_ENVELOPE.contentSha256
    || !unit.closure || ownerRlsEnvelopeFor(unit.closure, unit.kind) !== OWNER_RLS_ENVELOPE
    || unit.body !== exactAtomicBody(unit.closure.expanded)) {
    throw Error('MANAGED_OWNER_RLS_ENVELOPE_SCOPE_INVALID');
  }
  const before = `lock table public.candidate_manager_email_template_versions in access exclusive mode;
    create temporary table cloudtms_managed_owner_rls_snapshot(
      relation_oid oid not null, owner_oid oid not null, relation_acl aclitem[],
      relation_kind "char" not null, rls_enabled boolean not null, rls_forced boolean not null
    ) on commit drop;
    do $managed_owner_rls_before$
    declare v_target pg_catalog.pg_class%rowtype;
    begin
      if current_user<>session_user or not exists(select 1 from pg_catalog.pg_roles
        where rolname=current_user and not rolsuper and not rolbypassrls)
        or pg_catalog.current_setting('row_security')<>'on' then
        raise exception 'CLOUDTMS_MANAGED_OWNER_RLS_ROLE_INVALID' using errcode='42501';
      end if;
      select c.* into v_target from pg_catalog.pg_class c
        join pg_catalog.pg_namespace n on n.oid=c.relnamespace
        where n.nspname='public' and c.relname='candidate_manager_email_template_versions';
      if not found or v_target.relkind<>'r' or v_target.relowner<>current_user::regrole::oid
        or not v_target.relrowsecurity or not v_target.relforcerowsecurity
        or exists(select 1 from pg_catalog.pg_policy where polrelid=v_target.oid)
        or exists(select 1 from pg_catalog.aclexplode(coalesce(v_target.relacl,
          pg_catalog.acldefault('r',v_target.relowner))) a where a.grantee<>v_target.relowner) then
        raise exception 'CLOUDTMS_MANAGED_OWNER_RLS_POSTURE_INVALID' using errcode='42501';
      end if;
      insert into pg_temp.cloudtms_managed_owner_rls_snapshot
        values(v_target.oid,v_target.relowner,v_target.relacl,v_target.relkind,
          v_target.relrowsecurity,v_target.relforcerowsecurity);
      execute pg_catalog.format(
        'create policy cloudtms_managed_installer_owner_seed on public.candidate_manager_email_template_versions as permissive for all to %I using (true) with check (true)',
        current_user);
    end $managed_owner_rls_before$;`;
  const after = `do $managed_owner_rls_after$
    declare v_saved record; v_target pg_catalog.pg_class%rowtype;
      v_policy pg_catalog.pg_policy%rowtype;
    begin
      select s.* into strict v_saved from pg_temp.cloudtms_managed_owner_rls_snapshot s;
      select p.* into v_policy from pg_catalog.pg_policy p
        where p.polrelid=v_saved.relation_oid and p.polname='cloudtms_managed_installer_owner_seed';
      if not found or v_policy.polcmd<>'*' or not v_policy.polpermissive
        or v_policy.polroles is distinct from array[v_saved.owner_oid]::oid[]
        or pg_catalog.pg_get_expr(v_policy.polqual,v_policy.polrelid) is distinct from 'true'
        or pg_catalog.pg_get_expr(v_policy.polwithcheck,v_policy.polrelid) is distinct from 'true'
        or (select count(*) from pg_catalog.pg_policy where polrelid=v_saved.relation_oid)<>1 then
        raise exception 'CLOUDTMS_MANAGED_OWNER_RLS_POLICY_CHANGED' using errcode='42501';
      end if;
      drop policy cloudtms_managed_installer_owner_seed on public.candidate_manager_email_template_versions;
      select c.* into v_target from pg_catalog.pg_class c
        join pg_catalog.pg_namespace n on n.oid=c.relnamespace
        where n.nspname='public' and c.relname='candidate_manager_email_template_versions';
      if not found or current_user<>session_user or v_saved.owner_oid<>current_user::regrole::oid
        or not exists(select 1 from pg_catalog.pg_roles
          where rolname=current_user and not rolsuper and not rolbypassrls)
        or pg_catalog.current_setting('row_security')<>'on'
        or (v_target.oid,v_target.relowner,v_target.relacl,v_target.relkind,
          v_target.relrowsecurity,v_target.relforcerowsecurity)
          is distinct from (v_saved.relation_oid,v_saved.owner_oid,v_saved.relation_acl,
            v_saved.relation_kind,v_saved.rls_enabled,v_saved.rls_forced)
        or exists(select 1 from pg_catalog.pg_policy where polrelid=v_saved.relation_oid) then
        raise exception 'CLOUDTMS_MANAGED_OWNER_RLS_FINAL_POSTURE_CHANGED' using errcode='42501';
      end if;
    end $managed_owner_rls_after$;`;
  return `${before}\n${unit.body}\n${after}`;
}

export function assertConnectionPreservingException(closure,exception) {
  // This conservative check supplements, not replaces, the hash-bound body
  // audit. All executed DO/procedure bodies in these exact closures were read.
  if (/\bDISCARD\s+ALL\b|\bpg_advisory_unlock(?:_all|_shared)?\s*\(/i.test(closure.expanded)) throw Error('MANAGED_REPLAY_WRITER_GUARD_COMMAND');
  const conditional = ['INVOICE_GATE','BACKFILL'].includes(exception.reason);
  for(const row of scanManagedSql(closure.expanded).meta){
    if(/^\\set\s+ON_ERROR_STOP\s+on\s*$/i.test(row.text))continue;
    if(conditional && /^\\(?:if\s+:(?:\{\?bpay_backfill_batch\}|invoice_presentation_active_work)|else|endif|gset|gexec|set\s+bpay_backfill_batch\s+5000)\s*$/i.test(row.text))continue;
    throw Error(`MANAGED_REPLAY_CONNECTION_META_UNCLASSIFIED: ${closure.path}`);
  }
}

export function quotePsqlPath(file) {
  const normal = String(file).replaceAll('\\','/');
  if (/[\r\n']/.test(normal)) throw Error('MANAGED_EXECUTABLE_PATH_INVALID');
  return `'${normal}'`;
}

export function unitCheckpointSql(unit, context) {
  const ledger = unit.kind === 'migration' ? migrationLedgerSql(unit.item,context) : repeatableLedgerSql(unit.item,context);
  if (unit.immutableGuardDependency || Object.hasOwn(IMMUTABLE_GUARD_DEPENDENCY.consumers,unit.item.path)) {
    return `begin;\n${assertWriterSql}\n${immutableGuardDependencySql(unit)}\n${unit.body}\n${assertWriterSql}\n${ledger}\ncommit;`;
  }
  if (unit.ownerRlsEnvelope || unit.item.path === OWNER_RLS_ENVELOPE.path) {
    return `begin;\n${assertWriterSql}\n${ownerRlsEnvelopeSql(unit)}\n${assertWriterSql}\n${ledger}\ncommit;`;
  }
  if (!unit.exception) return `begin;\n${assertWriterSql}\n${unit.body}\n${assertWriterSql}\n${ledger}\ncommit;`;
  const gate = unit.exception.reason === 'INVOICE_GATE' ? `
\\if :{?invoice_presentation_active_work}
  select pg_catalog.set_config('cloudtms.managed.invoice_active_work', :'invoice_presentation_active_work', false);
  do $gate$ begin if pg_catalog.current_setting('cloudtms.managed.invoice_active_work')::boolean
    then raise exception 'CLOUDTMS_INVOICE_INSTALL_DEFERRED_ACTIVE_WORK'; end if; end $gate$;
\\else
  do $gate$ begin raise exception 'CLOUDTMS_INVOICE_INSTALL_GATE_MISSING'; end $gate$;
\\endif
` : '';
  // Original COMMIT/CALL/CONCURRENTLY and relative includes stay untouched.
  // A lost reply before this separate receipt reruns ONLY the pinned closure.
  return `${assertWriterSql}\n${unit.script}\n${gate}\nbegin;\n${assertWriterSql}\n${ledger}\ncommit;`;
}

export function assertBootstrapPrefix(current,release,baselinePrefix=BASELINE_MIGRATION_PREFIX) {
  const anchor = current.migrations.findIndex(row => row.path === release.controlPlaneMigration);
  if (anchor < 0) throw Error('MANAGED_CONTROL_PLANE_ANCHOR_MISSING');
  const prefix=current.migrations.slice(0,anchor+1);
  if(prefix.length!==baselinePrefix.count || digest(JSON.stringify(prefix.map(row=>[row.path,row.sha256])))!==baselinePrefix.sha256)throw Error('MANAGED_ORIGINAL_BASELINE_MIGRATION_PREFIX_MISMATCH');
  return anchor;
}

export function compileBootstrap({ root, release, current, context, mapSource = value => value,
  baselinePrefix=BASELINE_MIGRATION_PREFIX }) {
  const anchor=assertBootstrapPrefix(current,release,baselinePrefix);
  const sources = release.baselineFiles.map(file => exactAtomicBody(readManagedClosure(file,root,[],mapSource).expanded));
  const control = exactAtomicBody(readManagedClosure(release.controlPlaneMigration,root,[],mapSource).expanded);
  const original = canonical(fs.readFileSync(path.join(root,release.bootstrapFile),'utf8'));
  if (release.bootstrapFile !== 'supabase/baseline/22082026_1506_cloudtms_new_database_bootstrap.sql' || digest(original) !== BOOTSTRAP_PIN) throw Error('MANAGED_BOOTSTRAP_SOURCE_UNCLASSIFIED');
  const parsed = scanManagedSql(original);
  if (parsed.statements.some(row => ['BEGIN','COMMIT','CALL','ROLLBACK','START'].includes(row.tokens[0]))) throw Error('MANAGED_BOOTSTRAP_TRANSACTION_CHANGED');
  const baseline = readJson(root,release.baselineRepeatableLock).repeatables;
  if (!Array.isArray(baseline) || !baseline.length || new Set(baseline.map(row => row.path)).size !== baseline.length) throw Error('MANAGED_BASELINE_LOCK_INVALID');
  for (const row of baseline) if (!/^supabase\/repeatable\/.+\.sql$/.test(row.path) || !/^[0-9a-f]{64}$/.test(row.sha256)) throw Error('MANAGED_BASELINE_LOCK_INVALID');
  // Only the ORIGINAL baseline inventory is receipted. Never claim that a
  // later migration/current repeatable was installed merely by bootstrapping.
  return `begin;\n${assertWriterSql}\ndo $blank$ begin if (${emptySchemaSql.slice(0,-1)})<>0
    then raise exception 'CLOUDTMS_NEW_REQUIRES_EMPTY_SCHEMA'; end if; end $blank$;
    ${sources.map(source=>resetWriterSql+assertWriterSql+'\n'+source).join('\n')}
    ${resetWriterSql}${assertWriterSql}\n${control}\n${identitySql(context,true)}
    ${resetWriterSql}${assertWriterSql}
    \\set cloudtms_environment ${context.environment}
    ${mapSource(original,release.bootstrapFile)}
    ${managedAdmissionSql(context,true)}
    ${current.migrations.slice(0,anchor+1).map(row=>migrationLedgerSql(row,context)).join('\n')}
    ${baseline.map(row=>repeatableLedgerSql(row,context)).join('\n')}
    ${assertWriterSql}\ncommit;`;
}

const ledgerReadSql = `select jsonb_build_object(
  'migrations',(select coalesce(jsonb_agg(jsonb_build_object('path',path,'sha256',content_sha256)),'[]'::jsonb) from private.cloudtms_migration_ledger),
  'repeatables',(select coalesce(jsonb_agg(jsonb_build_object('path',path,'sha256',closure_sha256)),'[]'::jsonb) from private.cloudtms_repeatable_ledger))::text;`;

export async function prepareManagedSourceReaders(execute) {
  for (let call=0;call<READER_PREPARATION.maximumCallsPerInvocation;call++) {
    const value = (await execute(`${assertWriterSql}\nbegin; set local lock_timeout='5s'; set local statement_timeout='15s'; select ${READER_PREPARATION.pageRoutine}(); commit;`)).trim();
    if (value === 't') return {calls:call+1};
    if (value !== 'f') throw Error('READER_PREPARATION_INVALID_RESULT');
  }
  throw Error('READER_PREPARATION_RESUME_REQUIRED: bounded invocation finished; previous readers retained; rerun the same release to resume');
}

export function managedReleaseContext({ root, current, release, context, baselinePrefix=BASELINE_MIGRATION_PREFIX }) {
  const result = { ...context, manifestHash: managedManifestHash({root,current,release,
    executionPolicy: { bootstrap:BOOTSTRAP_PIN, baselinePrefix, exceptions:MANAGED_EXCEPTIONS,
      ownerRlsEnvelope: OWNER_RLS_ENVELOPE,
      immutableGuardDependency: IMMUTABLE_GUARD_DEPENDENCY,
      logicalOwnerMapping: logicalPostgresExecutablePolicy() }}) };
  validateContext(result); return result;
}

// Only this APPLY path is async. Existing read-only PLAN/export, independent
// pre-apply security rehearsal and fresh-connection verifiers remain separate.
export async function applyManagedRelease({ root, current, release, context: input,
  mapSource = value=>value, executableFile, openSession, deadlockRetries = ()=>0,
  preapply, verify, protectedCapabilityJourneys = [], protectedTestSetupAuthorizations = [], log = ()=>{}, delay = ms=>new Promise(resolve=>setTimeout(resolve,ms)),
  baselinePrefix=BASELINE_MIGRATION_PREFIX }) {
  // Dependency injection is only for the finite control-flow unit model. The
  // CLI never takes a baseline-prefix override from arguments/environment.
  const context = managedReleaseContext({root,current,release,context:input,baselinePrefix});
  let session, admitted = false, failure;
  const execute = sql => session.execute(sql);
  const options = {root,mapSource,executableFile};
  const open = async () => {
    session = openSession();
    if ((await execute(acquireWriterSql)).trim() !== 't') throw Error('CLOUDTMS_DATABASE_RELEASE_WRITER_BUSY');
    await execute(assertWriterSql);
    const actualDatabase = await execute('select pg_catalog.current_database();');
    if (actualDatabase !== context.expectedDatabase) throw Error('CLOUDTMS_DATABASE_IDENTITY_MISMATCH');
  };
  const reset = async () => execute(resetWriterSql+assertWriterSql);
  const applyUnit = async unit => {
    const retries = deadlockRetries(unit.item.path);
    for (let attempt=0;;attempt++) {
      await reset();
      log(`APPLY ${unit.kind.toUpperCase()} ${unit.item.path} (${unit.exception ? 'PINNED_REPLAY' : 'ATOMIC_CHECKPOINT'})`);
      try { await execute(unitCheckpointSql(unit,context)); return; }
      catch (error) {
        // Preserve the original single approved deadlock retry policy. The
        // failed connection is gone; a new writer must reacquire the SAME
        // mutex and prove the exact durable admission before retrying.
        if (error.sqlState !== '40P01' || attempt >= retries) throw error;
        await session.close(); await delay([7000,19000,41000][attempt]);
        await open(); await execute(`begin;${managedAdmissionSql(context)}commit;`);
      }
    }
  };
  try {
    await open();
    const controlPresent = (await execute("select (pg_catalog.to_regclass('private.cloudtms_database_releases') is not null)::text;")).trim() === 'true';
    if (context.mode === 'NEW' && !controlPresent) {
      if (Number(await execute(emptySchemaSql)) !== 0) throw Error('NEW requires an empty application schema or an exact managed bootstrap receipt');
      // Classify the whole selected envelope BEFORE the first DDL.
      const anchor=assertBootstrapPrefix(current,release,baselinePrefix);
      const baseline=new Map(readJson(root,release.baselineRepeatableLock).repeatables.map(row=>[row.path,row.sha256]));
      let envelope={migrations:current.migrations.slice(anchor+1),repeatables:current.repeatables.filter(row=>baseline.get(row.path)!==row.sha256)};
      if(fs.existsSync(path.join(root,'docs/bpay-next/final-definer-closures.json')))envelope=closeFinalDefinerSelection(envelope,current);
      for(const item of envelope.migrations)compileManagedUnit(item,'migration',options);
      for(const item of envelope.repeatables)compileManagedUnit(item,'repeatable',options);
      const bootstrap = compileBootstrap({root,release,current,context,mapSource,baselinePrefix});
      await reset(); await execute(bootstrap); admitted=true;
    } else {
      if (!controlPresent) throw Error('MANAGED_UPGRADE_CONTROL_PLANE_REQUIRED');
      await execute(identitySql(context));
      if (context.mode === 'NEW' && (await execute(resumeCheckSql(context))).trim() !== 'true') throw Error('CLOUDTMS_NEW_MANAGED_BOOTSTRAP_RECEIPT_REQUIRED');
    }
    let pending=pendingInventory(current,JSON.parse(await execute(ledgerReadSql)));
    // F-04: a changed old/root-included definition must not silently win an
    // UPGRADE while its selected final controlling closure is unchanged.
    // This is static source/selection validation; it runs no fixture or RPC.
    if(fs.existsSync(path.join(root,'docs/bpay-next/final-definer-closures.json'))){
      pending=closeFinalDefinerSelection(pending,current);
    }
    const migrationUnits=pending.migrations.map(item=>compileManagedUnit(item,'migration',options));
    const phases=readerReleasePhases(pending.repeatables,current.repeatables);
    const ordinaryUnits=phases.ordinary.map(item=>compileManagedUnit(item,'repeatable',options));
    // Preflight the exact pair too, before either definition is written.
    for(const item of phases.readers)compileManagedUnit(item,'repeatable',options);
    if(!admitted){await execute(`begin;${managedAdmissionSql(context)}commit;`);admitted=true;}
    for(const unit of migrationUnits)await applyUnit(unit);
    // Preserve the original independent, rollback-contained catalogue rehearsal.
    await preapply(pending.repeatables.map(item=>item.path));
    for(const unit of ordinaryUnits)await applyUnit(unit);
    if(phases.readers.length){
      await reset(); await prepareManagedSourceReaders(execute); await reset();
      const activation=readerActivationSql(phases.readers,
        file=>readManagedClosure(file,root,[],mapSource).source,
        item=>`${assertWriterSql}\n${repeatableLedgerSql(item,context)}`);
      await execute(assertWriterSql+'\n'+activation+'\n'+assertWriterSql);
    }
    await reset();
    // Re-read every receipt before setting the all-completed marker.
    const remaining=pendingInventory(current,JSON.parse(await execute(ledgerReadSql)));
    if(remaining.migrations.length||remaining.repeatables.length)throw Error('MANAGED_INSTALLATION_RECEIPTS_INCOMPLETE');
    await execute(`begin;${assertWriterSql}update private.cloudtms_database_releases
      set evidence_json=evidence_json||${json({definitions_complete:true})} where release_id=${literal(context.releaseId)}; commit;
      notify pgrst,'reload schema';`);
    const verified = await verify(context);
    if (verified.sha256 !== context.expectedHash) throw Error('MANAGED_VERIFIED_CONTRACT_MISMATCH');
    if (context.environment==='TEST') verified.evidence={...(verified.evidence??{}),
      weekly_source_protected_qualification_catalogue:installedQualificationCatalogueEvidence(verified.actual,context.expectedHash)};
    // Root-only reviewed setup inputs; not CLI/env/browser flags. Native
    // prerequisite envelopes are byte-preserved and exact commit/contract-bound.
    if (!Array.isArray(protectedCapabilityJourneys)) throw Error('QUALIFICATION_INPUT_INVALID');
    if (protectedCapabilityJourneys.length) {
      if (verified.evidence?.weekly_source_protected_capability_qualifications !== undefined)
        throw Error('QUALIFICATION_EVIDENCE_REPLACEMENT_REFUSED');
      const rows=protectedCapabilityJourneys.map(journey=>{
        if (journey.context.release_id!==context.releaseId || journey.context.git_commit!==context.gitCommit
          || journey.context.contract_sha256!==context.expectedHash || journey.context.environment!==context.environment
          || journey.context.database!==context.expectedDatabase) throw Error('QUALIFICATION_MANAGED_SCOPE_INVALID');
        if (journey.phase==='FULL_OPERATIONAL') {
          if (!journey.qualification || journey.qualification.release_id!==context.releaseId
            || journey.qualification.git_commit!==context.gitCommit || journey.qualification.contract_sha256!==context.expectedHash)
            throw Error('OPERATIONAL_MANAGED_SCOPE_INVALID');
          return journey.qualification;
        }
        if (journey.phase==='ROLLBACK_PREREQUISITE') {
          if (!journey.receipts.length) throw Error('PREREQUISITE_GATE_CENSUS_INVALID');
          const first=JSON.parse(journey.receipts[0].receipt_bytes.toString('utf8'));
          const sourceProof=proveCommittedSources(root,first.execution.source_commit,context.gitCommit);
          return buildPrerequisiteQualification(journey.context,journey.receipts,sourceProof);
        }
        if (journey.phase!==undefined) throw Error('QUALIFICATION_INPUT_INVALID');
        return buildQualification(journey.context,journey.receipts);
      });
      verified.evidence={...(verified.evidence??{}),weekly_source_protected_capability_qualifications:rows};
    }
    const qualifications=verified.evidence?.weekly_source_protected_capability_qualifications??[];
    assertManagedPrerequisiteEvidence(context,{weekly_source_protected_prerequisite_qualifications:qualifications.filter(
      q=>q.schema_version==='WEEKLY_SOURCE_PROTECTED_CAPABILITY_PREREQUISITE_QUALIFICATION_V2')},root);
    assertManagedOperationalEvidence(context,{weekly_source_protected_capability_qualifications:qualifications.filter(
      q=>q.schema_version!=='WEEKLY_SOURCE_PROTECTED_CAPABILITY_PREREQUISITE_QUALIFICATION_V2')},root);
    if (!Array.isArray(protectedTestSetupAuthorizations)) throw Error('TEST_SETUP_INPUT_INVALID');
    if (protectedTestSetupAuthorizations.length) {
      if (verified.evidence?.weekly_source_protected_test_setup_authorizations!==undefined) throw Error('TEST_SETUP_EVIDENCE_REPLACEMENT_REFUSED');
      const authorizations=protectedTestSetupAuthorizations.map(input=>buildTestSetupAuthorization({...context,mode},input,qualifications));
      for (const auth of authorizations) {
        const actual=JSON.parse(await execute('select private.weekly_source_protected_qualification_census_v1('
          +literal(auth.source_cycle_id)+'::uuid)::text'));
        if (domainHash('TEST_SETUP_ACTUAL_CENSUS',actual)!==domainHash('TEST_SETUP_ACTUAL_CENSUS',auth.original_census))
          throw Error('TEST_SETUP_NATIVE_CENSUS_CHANGED');
      }
      verified.evidence={...(verified.evidence??{}),weekly_source_protected_test_setup_authorizations:authorizations};
    }
    for (const q of qualifications.filter(q=>q.schema_version==='WEEKLY_SOURCE_PROTECTED_CAPABILITY_OPERATIONAL_QUALIFICATION_V1')) {
      const native=q.native_execution_qualification,closure=q.test_setup_closure;
      const recorded=JSON.parse(await execute('select jsonb_build_object('+literal('release_id')+',r.release_id,'+
        literal('source_commit')+',r.git_commit,'+literal('contract_sha256')+',r.installed_contract_sha256,'+
        literal('catalogue_sha256')+',r.evidence_json#>>'+literal('{weekly_source_protected_qualification_catalogue,jsonb_sha256}')+
        ')::text from private.cloudtms_database_releases r where r.release_id='+literal(native.release_id)+
        ' and r.status='+literal('VERIFIED')+' and r.completed_at_utc is not null and r.installed_contract_sha256=r.repository_contract_sha256'));
      if (!recorded || recorded.source_commit!==native.git_commit || recorded.contract_sha256!==context.expectedHash
        || recorded.catalogue_sha256!==verified.evidence.weekly_source_protected_qualification_catalogue.jsonb_sha256)
        throw Error('OPERATIONAL_RECORDED_EXECUTION_CHANGED');
      const actual=JSON.parse(await execute('select jsonb_build_object('+literal('id')+',a.id,'+literal('after')+',a.after_json,'+
        literal('sha256')+',encode(private.weekly_source_sha256_jsonb_v1('+literal('WEEKLY_SOURCE_TEST_JOURNEY_CLOSED_V1')+
        ',a.after_json),'+literal('hex')+'))::text from public.audit_events a where a.id='+literal(closure.closure_audit_id)+
        '::uuid and a.object_type='+literal('weekly_source_protected_test_journey')+' and a.object_id_text='+literal(closure.scope_id)+
        ' and a.action='+literal('WEEKLY_SOURCE_TEST_JOURNEY_CLOSED')+' and a.event_sequence_is_authoritative'));
      const {closure_audit_id,closure_audit_sha256,...after}=closure;
      if (!actual || actual.id!==closure_audit_id || actual.sha256!==closure_audit_sha256
        || domainHash('ACTUAL_TEST_SETUP_CLOSURE',actual.after)!==domainHash('ACTUAL_TEST_SETUP_CLOSURE',after))
        throw Error('OPERATIONAL_ACTUAL_SETUP_CLOSURE_REQUIRED');
    }
    if (protectedCapabilityJourneys.length) {
      const priorRows=JSON.parse(await execute('select coalesce(evidence_json->' + literal('weekly_source_protected_capability_qualifications')
        + ',' + literal('null') + '::jsonb)::text from private.cloudtms_database_releases where release_id=' + literal(context.releaseId)));
      if (priorRows!==null && domainHash('WEEKLY_SOURCE_REPLAY_SAFE_PREREQUISITE_EVIDENCE_V1',priorRows)!==
        domainHash('WEEKLY_SOURCE_REPLAY_SAFE_PREREQUISITE_EVIDENCE_V1',verified.evidence.weekly_source_protected_capability_qualifications))
        throw Error('QUALIFICATION_EVIDENCE_REPLACEMENT_REFUSED');
      for (const q of verified.evidence.weekly_source_protected_capability_qualifications) {
        if (q.schema_version!=='WEEKLY_SOURCE_PROTECTED_CAPABILITY_PREREQUISITE_QUALIFICATION_V2') continue;
        const recorded=JSON.parse(await execute('select jsonb_build_object('+
          literal('release_id')+',r.release_id,'+literal('source_commit')+',r.git_commit,'+
          literal('contract_sha256')+',r.installed_contract_sha256,'+literal('installation_id')+',i.installation_id,'+
          literal('agency_id')+',g.agency_id,'+literal('environment')+',i.environment,'+literal('database')+',current_database(),'+
          literal('managed_manifest_sha256')+',r.evidence_json#>>'+literal('{managed_release,manifest_sha256}')+','+
          literal('installed_catalogue_jsonb_sha256')+',r.evidence_json#>>'+literal('{weekly_source_protected_qualification_catalogue,jsonb_sha256}')+
          ')::text from private.cloudtms_database_releases r cross join private.cloudtms_database_identity i '+
          'join public.weekly_source_groups g on g.id='+literal(q.source_group_id)+'::uuid and g.active and g.environment='+literal('TEST')+
          ' and g.source_family='+literal('ROSTER')+' where i.singleton and i.environment='+literal('TEST')+
          ' and r.release_id='+literal(q.execution_identity.release_id)+' and r.status='+literal('VERIFIED')+
          ' and r.completed_at_utc is not null and r.installed_contract_sha256=r.repository_contract_sha256'));
        if (domainHash('EXECUTION_IDENTITY',recorded)!==domainHash('EXECUTION_IDENTITY',q.execution_identity))
          throw Error('PREREQUISITE_RECORDED_EXECUTION_CHANGED');
      }
    }
    if (protectedTestSetupAuthorizations.length) {
      const old=JSON.parse(await execute('select coalesce(evidence_json->'+literal('weekly_source_protected_test_setup_authorizations')+','+
        literal('null')+'::jsonb)::text from private.cloudtms_database_releases where release_id='+literal(context.releaseId)));
      if (old!==null && domainHash('REPLAY_TEST_SETUP_AUTHORIZATIONS',old)!==domainHash('REPLAY_TEST_SETUP_AUTHORIZATIONS',
        verified.evidence.weekly_source_protected_test_setup_authorizations)) throw Error('TEST_SETUP_EVIDENCE_REPLACEMENT_REFUSED');
    }
    await execute(`begin;${assertWriterSql}${identitySql(context)}
      update private.cloudtms_database_releases r set status='VERIFIED',installed_contract_sha256=${literal(verified.sha256)},
        completed_at_utc=pg_catalog.clock_timestamp(),evidence_json=evidence_json||${json(verified.evidence??{})}
      where r.release_id=${literal(context.releaseId)} and ${releaseMatch(context)} and r.status='APPLYING';
      do $verified$ begin if not exists(select 1 from private.cloudtms_database_releases r
        where r.release_id=${literal(context.releaseId)} and ${releaseMatch(context)} and r.status='VERIFIED')
        then raise exception 'MANAGED_RELEASE_VERIFIED_RECEIPT_MISSING'; end if; end $verified$; commit;`);
    return {releaseId:context.releaseId,manifestHash:context.manifestHash};
  } catch(error) {
    failure=error;
    // Never open an unguarded second writer to mark failure. A disconnected
    // attempt stays APPLYING with its exact bootstrap/checkpoints intact.
    if(admitted && session && !session.failure && !session.finished){
      try { await execute(`begin;${assertWriterSql}update private.cloudtms_database_releases r
        set status='FAILED',completed_at_utc=pg_catalog.clock_timestamp(),
          evidence_json=evidence_json||${json({failure:'verification_or_apply_failed'})}
        where r.release_id=${literal(context.releaseId)} and ${releaseMatch(context)} and r.status='APPLYING';commit;`); }
      catch { /* Original failure is authoritative; no separate repair. */ }
    }
    throw error;
  } finally { if(session)try {await session.close();}catch(error){if(!failure)throw error;} }
}
