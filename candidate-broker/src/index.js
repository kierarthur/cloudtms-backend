import { handleCandidateBrokerRequest } from './candidate-broker.js';

export default {
  async fetch(request, env, ctx) {
    return handleCandidateBrokerRequest(request, env, ctx);
  }
};
