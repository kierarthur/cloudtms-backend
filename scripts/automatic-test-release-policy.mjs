// Pure, fail-closed route selection. No network, credentials, SQL or mutations.
export function validateConnectionProof(proof, sha, targets, now = Date.now()) {
  const checked = Date.parse(proof?.checkedAt);
  if (proof?.environment !== 'TEST' || proof.backendCommit !== sha || !Number.isFinite(checked)
      || now - checked > 15 * 60000 || checked > now + 60000) {
    throw new Error('Fresh commit-bound TEST build-connection inspection required');
  }
  for (const [, worker, branch] of targets) {
    const entry = proof.workers?.find(x => x.worker === worker);
    if (!entry || entry.branch !== branch || entry.repository !== 'kierarthur/cloudtms-backend'
        || entry.verified !== true || !/^[a-f0-9]{40}$/.test(entry.activeCommit || '')) {
      throw new Error(`Missing verified connection and active deployment commit: ${worker}`);
    }
  }
}

export function selectDispatchedRun(runs, sha, since, pinnedId) {
  const candidates = runs.filter(r => r.head_sha === sha && r.event === 'workflow_dispatch'
    && Date.parse(r.created_at) >= Math.floor(Date.parse(since) / 1000) * 1000);
  if (pinnedId) return candidates.find(r => r.id === pinnedId);
  if (candidates.length > 1) throw new Error('Concurrent release dispatches found; reconcile exact run before continuing');
  return candidates[0];
}

export function chooseTestDatabaseRoute(state) {
  if (state.environment !== 'TEST' || state.database !== 'cloudtms_test_clone') {
    throw new Error('Automatic release is restricted to the managed agency TEST database');
  }
  if (!state.identityVerified || !state.ledgerVerified) throw new Error('Target or source ledger is unproved');
  const pending = [...state.pendingMigrations, ...state.pendingRepeatables];
  const interrupted = state.latestStatus !== 'VERIFIED';
  if (!pending.length && !interrupted && state.contractMatches && state.verificationAuthorityUnchanged) {
    return { route: 'NO_DATABASE_CHANGE', reason: 'Exact installed contract and verified authority still match; no SQL installation or full verifier replay is needed.' };
  }
  // A failed full release is never hidden behind a smaller component release.
  if (!interrupted && !state.pendingMigrations.length) {
    const eligible = (state.components || []).filter(c => c.sourceVerified && c.baseVerified
      && (state.verificationAuthorityUnchanged || c.scopeVerified)
      && pending.length > 0 && pending.every(p => c.paths.includes(p)));
    if (eligible.length === 1) return { route: 'APPROVED_COMPONENT', component: eligible[0].id,
      reason: 'Every pending definition belongs to one exact, reviewed component; its own engine retains all scope and verification guards.' };
  }
  return { route: 'FULL_UPGRADE', reason: interrupted
    ? 'Latest database release is not VERIFIED. Reconcile installed changes and rerun every required verifier; application publication remains blocked.'
    : 'Schema, verification authority, contract or unclassified definition changes require the complete protected UPGRADE route.' };
}

export function classifyApplicationPaths(paths) {
  const result = { backend: false, candidatePrivate: false, candidateSynthetic: false,
    candidateBroker: false, office: false, mobileNative: false, unknown: [] };
  for (const p of paths) {
    if (/^(docs\/|tests\/|AGENTS\.md$)/.test(p)) continue;
    if (/^(broker\/|shared\/|package(?:-lock)?\.json$|wrangler\.)/.test(p)) result.backend = true;
    else if (p.startsWith('candidate-private-api/')) result.candidatePrivate = true;
    else if (p.startsWith('candidate-synthetic-private-api/')) result.candidateSynthetic = true;
    else if (p.startsWith('candidate-broker/')) result.candidateBroker = true;
    else if (p.startsWith('office:')) result.office = true;
    else if (/^(mobile:|apps\/candidate-app\/)/.test(p)) result.mobileNative = true;
    else if (!/^(supabase\/|scripts\/|\.github\/|\.node-version$|\.npmrc$)/.test(p)) result.unknown.push(p);
  }
  // Shared backend authority can affect each private adapter. Preserve rollout order.
  if (result.backend) result.candidatePrivate = result.candidateSynthetic = result.candidateBroker = true;
  return result;
}

export function publicationStages(application) {
  if (application.unknown.length) throw new Error('Unclassified deployment paths require a reviewed registry entry');
  return ['database', ...(application.backend ? ['backend'] : []),
    ...(application.candidatePrivate ? ['candidate-private'] : []),
    ...(application.candidateSynthetic ? ['candidate-synthetic'] : []),
    ...(application.candidateBroker ? ['candidate-broker'] : []),
    ...(application.office ? ['office'] : []), ...(application.mobileNative ? ['mobile-build'] : [])];
}
