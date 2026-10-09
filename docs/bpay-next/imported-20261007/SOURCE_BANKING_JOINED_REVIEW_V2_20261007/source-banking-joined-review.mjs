import { calculateDestinationRepriceContext } from './destination-reprice-preview.mjs';

const UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const HASH=/^[0-9a-f]{64}$/;
const SCHEMA='SOURCE_DESTINATION_PAY_FINANCIAL_REVIEW_V2';
function fail(code){throw Object.assign(new Error(code),{code});}
function actorId(actor){
  if(!UUID.test(String(actor?.id??'')))fail('WEEKLY_SOURCE_OFFICE_ACTOR_REQUIRED');
  return actor.id;
}
function freezeJson(value){
  const result=JSON.parse(JSON.stringify(value));
  const freeze=item=>{
    if(item&&typeof item==='object'){Object.values(item).forEach(freeze);Object.freeze(item);}
    return item;
  };
  return freeze(result);
}
function sameJson(left,right){
  const ordered=value=>Array.isArray(value)?value.map(ordered):value&&typeof value==='object'
    ?Object.fromEntries(Object.keys(value).sort().map(key=>[key,ordered(value[key])])):value;
  return JSON.stringify(ordered(left))===JSON.stringify(ordered(right));
}

// Proposed placement beside the frozen Source V2 adapter in broker/src/weekly-source.
// No calculator or financial choices are invented here. SQL qualifies the exact
// Source request, current book/policy, explicit choices and full joined digest.
export function createWeeklySourceBankingJoinedReviewOwner({rpc}){
  if(typeof rpc!=='function')fail('WEEKLY_SOURCE_JOINED_REVIEW_RPC_REQUIRED');
  return Object.freeze({
    async preview({actor,baseRequest,financialSelections}){
      const actorUserId=actorId(actor);
      if(!Array.isArray(financialSelections))fail('WEEKLY_SOURCE_JOINED_SELECTION_REQUIRED');
      const base=freezeJson(baseRequest);const selections=freezeJson(financialSelections);
      const loaded=await rpc('weekly_source_destination_financial_preview_v2',{
        p_actor:actorUserId,p_base_request:base,p_calculations:null,p_financial_selections:selections,
      });
      if(loaded?.ok!==true)fail('WEEKLY_SOURCE_DESTINATION_REPRICE_CONTEXT_UNAVAILABLE');
      const calculations=freezeJson(calculateDestinationRepriceContext(loaded.context));
      const qualified=await rpc('weekly_source_destination_financial_preview_v2',{
        p_actor:actorUserId,p_base_request:base,p_calculations:calculations,p_financial_selections:selections,
      });
      const quote=qualified?.quote;
      if(qualified?.ok!==true||quote?.review?.schema_version!==SCHEMA
          ||!HASH.test(String(quote.review_sha256??''))
          ||!sameJson(quote.review.financial_selections,selections)){
        fail('WEEKLY_SOURCE_JOINED_QUOTE_UNAVAILABLE');
      }
      return freezeJson({schema_version:SCHEMA,actor_user_id:actorUserId,base_request:base,
        calculations,financial_selections:selections,review_sha256:quote.review_sha256,
        joined_review:quote.review,financial_review_accepted:false,financial_comparison_resolved:false});
    },
    async accept({actor,reviewed,idempotencyKey,reason}){
      const actorUserId=actorId(actor);
      if(reviewed?.schema_version!==SCHEMA||reviewed.actor_user_id!==actorUserId
          ||!HASH.test(String(reviewed.review_sha256??''))||!Array.isArray(reviewed.financial_selections)
          ||typeof reason!=='string'||!reason.trim()
          ||typeof idempotencyKey!=='string'||idempotencyKey.length<16){
        fail('WEEKLY_SOURCE_JOINED_ACCEPT_INVALID');
      }
      return rpc('weekly_source_destination_financial_accept_v2',{
        p_actor:actorUserId,p_base_request:reviewed.base_request,p_calculations:reviewed.calculations,
        p_financial_selections:reviewed.financial_selections,p_review_sha256:reviewed.review_sha256,
        p_idempotency_key:idempotencyKey,p_reason:reason,
      });
    },
  });
}
