import path from 'node:path';
import {execFileSync,spawnSync} from 'node:child_process';
import crypto from 'node:crypto';

// Fresh read-only Git evidence for the distinct second install. It neither
// probes a database nor claims an installed/runtime/contract qualification.
export function collectInstall2SourceRelationship({repositoryRoot,installedSourceCommit,
  completedBankCommit,expectedCandidateCommit,expectedRemoteTestCommit,featureBranch}) {
  const pin=/^[a-f0-9]{40}$/;
  if(!path.isAbsolute(repositoryRoot)||[installedSourceCommit,completedBankCommit,
    expectedCandidateCommit,expectedRemoteTestCommit].some(value=>!pin.test(value??''))
    ||!/^codex\/healthroster-install2-\d{8}$/.test(featureBranch??''))
    throw Error('INSTALL2_SOURCE_RELATIONSHIP_ARGUMENTS_REFUSED');
  const prefix=['-c','gc.auto=0','-c','maintenance.auto=false','-C',repositoryRoot];
  const git=args=>execFileSync('git',[...prefix,...args],{encoding:'utf8',windowsHide:true,maxBuffer:4000000});
  const hash=value=>crypto.createHash('sha256').update(value).digest('hex');
  const remote=branch=>{
    const rows=git(['ls-remote','origin','refs/heads/'+branch]).trim().split(/\r?\n/);
    if(rows.length!==1)throw Error('INSTALL2_REMOTE_REF_ENVELOPE_REFUSED');
    const [sha,ref]=rows[0].split(/\s+/);
    if(!pin.test(sha??'')||ref!=='refs/heads/'+branch)throw Error('INSTALL2_REMOTE_REF_ENVELOPE_REFUSED');
    return sha;
  };
  const origin=git(['remote','get-url','origin']).trim();
  if(!['git@github.com:kierarthur/cloudtms-backend.git',
    'https://github.com/kierarthur/cloudtms-backend.git',
    'ssh://git@github.com/kierarthur/cloudtms-backend.git'].includes(origin))
    throw Error('INSTALL2_CANONICAL_REPOSITORY_REFUSED');
  if(git(['rev-parse','HEAD']).trim()!==expectedCandidateCommit
    ||git(['branch','--show-current']).trim()!==featureBranch)
    throw Error('INSTALL2_LOCAL_SOURCE_CHANGED');
  if(remote('test')!==expectedRemoteTestCommit||remote(featureBranch)!==expectedCandidateCommit)
    throw Error('INSTALL2_REMOTE_SOURCE_CHANGED');
  for(const commit of [installedSourceCommit,completedBankCommit,expectedCandidateCommit,expectedRemoteTestCommit]){
    if(git(['rev-parse','--verify',commit+'^{commit}']).trim()!==commit)
      throw Error('INSTALL2_COMMIT_OBJECT_UNAVAILABLE');
  }
  const links=[[completedBankCommit,installedSourceCommit],
    [installedSourceCommit,expectedRemoteTestCommit],[expectedRemoteTestCommit,expectedCandidateCommit]];
  for(const [ancestor,descendant]of links){const result=spawnSync('git',[...prefix,'merge-base','--is-ancestor',ancestor,descendant],{windowsHide:true,stdio:'pipe'});
    if(result.status!==0)throw Error('INSTALL2_NONFORCED_ANCESTRY_REFUSED');}
  const difference=git(['diff','--name-only','-z',installedSourceCommit,expectedRemoteTestCommit]);
  const paths=difference.split('\0').filter(Boolean);
  const status=git(['status','--porcelain=v1','-z']);
  // Keep the complete status envelope and its digest. Never pretend unrelated
  // retained files are absent or discard them to obtain a clean-tree claim.
  const entries=status.split('\0').filter(Boolean);
  const evidence={status:'FRESH_GIT_RELATIONSHIP_ONLY_NATIVE_PREDECESSOR_UNVERIFIED',
    checkedAt:new Date().toISOString(),repository:'kierarthur/cloudtms-backend',featureBranch,
    installedSourceCommitSupplied:installedSourceCommit,completedBankCommitSupplied:completedBankCommit,
    observedRemoteTestCommit:expectedRemoteTestCommit,pushedCandidateCommit:expectedCandidateCommit,
    ancestry:links.map(([ancestor,descendant])=>({ancestor,descendant,proved:true})),
    installedSourceToRemoteChangedPaths:paths,changedPathEnvelopeSha256:hash(difference),
    fullGitClean:entries.length===0,gitStatusEnvelope:entries,gitStatusEnvelopeSha256:hash(status),
    databaseProbed:false,installedReleaseStatusVerified:false,runtimeBuildsVerified:false,
    currentContractVerified:false,permanentWritesPerformed:false,
    boundary:'Fresh ancestry and source difference only. Actual native release/ledger/contract and deployed versions must be separately proved under the shared turn; every changed path and retained untracked file needs its exact reviewed disposition. This evidence cannot authorise installation.'};
  if(remote('test')!==expectedRemoteTestCommit||remote(featureBranch)!==expectedCandidateCommit
    ||git(['rev-parse','HEAD']).trim()!==expectedCandidateCommit
    ||git(['status','--porcelain=v1','-z'])!==status)
    throw Error('INSTALL2_SOURCE_CHANGED_DURING_RELATIONSHIP_PROOF');
  return evidence;
}
