import { candidateBrokerInternals, handleCandidateBrokerRequest } from './candidate-broker.js';
import { handleWeeklySourcePushAuthority } from './weekly-source-push-authority.js';

export default {
  async fetch(request, env, ctx) {
    const weeklyPushResponse=await handleWeeklySourcePushAuthority(request,env,{
      openVersionedEnvelope:candidateBrokerInternals.openVersionedEnvelope,
      deviceEncryptionAuthority:candidateBrokerInternals.credentialAuthorities.deviceEncryption
    });
    if (weeklyPushResponse) return weeklyPushResponse;
    return handleCandidateBrokerRequest(request, env, ctx);
  }
};
