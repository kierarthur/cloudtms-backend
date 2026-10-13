// Root-review proposal only. No route, database, network or mutable store.
// Tax arithmetic delegates the already-frozen single invoice VAT authority.
import { invoiceVatRoundingPence } from '../../../../broker/src/weekly-source/source-invoice-money.js';
import { createHash } from 'node:crypto';
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const SHA = /^[0-9a-f]{64}$/;
const fail = code => { throw new Error(code); };
const id = (value, code) => UUID.test(value ?? '') ? value : fail(code);
const hash = (value, code) => SHA.test(value ?? '') ? value : fail(code);
const pence = value => {
  if (!/^-?(?:0|[1-9][0-9]*)$/.test(String(value))) fail('INVOICE_PENCE_INVALID');
  const amount = BigInt(value); if (amount < -999999999999n || amount > 999999999999n) fail('INVOICE_PENCE_BOUND');
  return amount;
};
const sha = (domain, value) => createHash('sha256').update(JSON.stringify([domain,value])).digest('hex');

// Mirrors the proposed server-private predicate's closed facts, never arbitrary
// caller-assigned pence or an invoice Move. Genuine movements retain source IDs.
export function partitionInvoiceWindows({sourceCycle,revisionId,movements,existingWindows}) {
  id(sourceCycle.id,'SOURCE_CYCLE_REQUIRED'); id(revisionId,'SOURCE_REVISION_REQUIRED');
  const byId = new Map(existingWindows.map(w => [id(w.id,'WINDOW_ID_REQUIRED'),w]));
  if (!byId.has(sourceCycle.id)) fail('ACTUAL_SOURCE_WINDOW_REQUIRED');
  const seen = new Set(), groups = new Map();
  for (const movement of movements) {
    id(movement.id,'MOVEMENT_REQUIRED'); hash(movement.hash,'MOVEMENT_HASH_REQUIRED');
    if (seen.has(movement.id)) fail('MOVEMENT_DUPLICATE'); seen.add(movement.id);
    if (movement.finalRevisionId !== revisionId || movement.finalisationCycleId !== sourceCycle.id) fail('ORIGINAL_SOURCE_IDENTITY_CHANGED');
    id(movement.clientId,'ACTUAL_CLIENT_REQUIRED');
    const target = movement.lineKind === 'SOURCE_INVOICE_CORRECTION' && movement.role !== 'CORRECTION_UNWIND'
      ? id(movement.invoiceWindowCycleId,'CORRECTION_WINDOW_REQUIRED') : sourceCycle.id;
    const window = byId.get(target);
    if (!window || window.groupId !== sourceCycle.groupId || (window.clientId && window.clientId !== movement.clientId)) fail('FOREIGN_INVOICE_WINDOW');
    if (target !== sourceCycle.id) {
      if (!['OPEN','FINALISABLE','FINALISING'].includes(window.state)) fail('INVOICE_WINDOW_NOT_OPEN');
      const timestamp = Date.parse(movement.finalisedAtUtc), cutoff = Date.parse(window.cutoffAtUtc), floor = Date.parse(window.previousCutoffAtUtc);
      if (![timestamp,cutoff,floor].every(Number.isFinite) || timestamp <= floor || timestamp > cutoff || movement.workDate > window.weekEnding) fail('INVOICE_WINDOW_TIMESTAMP_MISMATCH');
      if (String(movement.totalPayPence) !== '0') fail('FOREIGN_WINDOW_PAY_FORBIDDEN');
    }
    const key = `${movement.clientId}|${target}`;
    const group = groups.get(key) ?? {clientId:movement.clientId,invoiceWindowCycleId:target,
      sourceCycleId:sourceCycle.id,finalRevisionId:revisionId,invoiceWeekEnding:window.weekEnding,
      payOwningManifest:target === sourceCycle.id,movementWitnesses:[]};
    group.movementWitnesses.push({id:movement.id,hash:movement.hash}); groups.set(key,group);
  }
  return [...groups.values()].sort((a,b) => a.invoiceWeekEnding.localeCompare(b.invoiceWeekEnding) || a.clientId.localeCompare(b.clientId) || a.invoiceWindowCycleId.localeCompare(b.invoiceWindowCycleId))
    .map(group => ({...group,movementWitnesses:group.movementWitnesses.sort((a,b)=>a.id.localeCompare(b.id))}));
}

// Explicit complete receipt set: no latest receipt/current setting fallback.
// SQL production must derive every member from manifest and revalidate each
// receipt with the existing immutable movement-money owner before calling this.
export function deriveRoundingReceiptAuthority({manifest,rules,roundingMovement,members}) {
  if (rules.invoice_sender !== 'CLIENT_SELF_BILL' || !['SUM_OF_LINES','ONCE_ON_TOTAL_HALF_UP','ONCE_ON_TOTAL_TRUNCATE'].includes(rules.invoice_vat_method)) fail('FROZEN_SELF_BILL_RULES_REQUIRED');
  const expected = new Map(manifest.memberWitnesses.map(m => [id(m.id,'MANIFEST_MEMBER_ID_REQUIRED'),hash(m.hash,'MANIFEST_MEMBER_HASH_REQUIRED')]));
  if (expected.size !== manifest.memberWitnesses.length || expected.size !== members.length) fail('RECEIPT_SET_CENSUS_INVALID');
  const seen = new Set(), witnesses = [], original = [], admitted = [];
  for (const member of members) {
    if (seen.has(member.id) || expected.get(member.id) !== member.hash) fail('RECEIPT_SET_MEMBER_MISMATCH'); seen.add(member.id);
    const net=pence(member.netPence), vat=pence(member.vatPence), gross=pence(member.grossPence);
    if (net+vat !== gross || String(member.vatRatePct) !== String(manifest.vatRatePct)) fail('ORIGINAL_MONEY_INVALID');
    original.push({netPence:net.toString(),vatPence:vat.toString()});
    let chosenVat=vat, chosenGross=gross, releaseAuthority=null;
    if (member.release) {
      const release = member.release;
      if (release.holdKind !== 'VAT_FIT' || release.uploadRowId !== member.uploadRowId || release.sourceRowHash !== member.sourceRowHash
        || pence(release.netPence) !== net || !['BILL_AT_NET_PLUS_20','BILL_AS_FILE'].includes(release.action)) fail('RELEASE_SCOPE_INVALID');
      id(release.id,'RELEASE_ID_REQUIRED'); hash(release.hash,'RELEASE_HASH_REQUIRED');
      chosenVat=pence(release.vatPence); chosenGross=pence(release.grossPence);
      if (net+chosenVat !== chosenGross) fail('RELEASE_MONEY_INVALID');
      const expectedVat = release.action === 'BILL_AS_FILE' ? vat
        : pence(invoiceVatRoundingPence([{netPence:net.toString(),vatPence:'0'}],'ONCE_ON_TOTAL_HALF_UP','20'));
      if (chosenVat !== expectedVat) fail('RELEASE_CHOICE_MONEY_INVALID');
      releaseAuthority={id:release.id,hash:release.hash,uploadRowId:release.uploadRowId,sourceRowHash:release.sourceRowHash,
        action:release.action,netPence:net.toString(),vatPence:chosenVat.toString(),grossPence:chosenGross.toString()};
    } else if (member.unreleasedVatFit) fail('UNRELEASED_VAT_FIT');
    admitted.push({netPence:net.toString(),vatPence:chosenVat.toString()});
    witnesses.push({movementId:member.id,movementHash:member.hash,netPence:net.toString(),originalVatPence:vat.toString(),
      admittedVatPence:chosenVat.toString(),admittedGrossPence:chosenGross.toString(),releaseAuthority});
  }
  const oldDelta=invoiceVatRoundingPence(original,rules.invoice_vat_method,manifest.vatRatePct);
  const newDelta=invoiceVatRoundingPence(admitted,rules.invoice_vat_method,manifest.vatRatePct);
  const totalNet=admitted.reduce((sum,line)=>sum+pence(line.netPence),0n);
  const admittedVat=admitted.reduce((sum,line)=>sum+pence(line.vatPence),0n);
  if (roundingMovement) {
    id(roundingMovement.id,'ROUNDING_ID_REQUIRED'); hash(roundingMovement.hash,'ROUNDING_HASH_REQUIRED');
    if (pence(roundingMovement.originalVatPence) !== pence(oldDelta)) fail('FROZEN_ROUNDING_AMOUNT_MISMATCH');
  } else if (newDelta !== '0' || oldDelta !== '0') fail('FROZEN_ROUNDING_AUTHORITY_REQUIRED');
  const authority={contract:'WEEKLY_SOURCE_INVOICE_ROUNDING_AUTHORITY_V1',
    clientManifestId:id(manifest.id,'MANIFEST_ID_REQUIRED'),manifestHash:hash(manifest.hash,'MANIFEST_HASH_REQUIRED'),
    finalRevisionId:id(manifest.finalRevisionId,'REVISION_REQUIRED'),clientId:id(manifest.clientId,'CLIENT_REQUIRED'),
    invoiceWindowCycleId:id(manifest.invoiceWindowCycleId,'WINDOW_REQUIRED'),
    frozenRuleVersionId:id(manifest.ruleVersionId,'FROZEN_VERSION_REQUIRED'),frozenRuleValuesSha256:hash(manifest.ruleValuesSha256,'FROZEN_RULE_HASH_REQUIRED'),
    invoiceVatMethod:rules.invoice_vat_method,vatRatePct:String(manifest.vatRatePct),
    roundingMovementId:roundingMovement?.id ?? null,roundingMovementHash:roundingMovement?.hash ?? null,
    originalRoundingVatPence:oldDelta,admittedRoundingVatPence:newDelta,totalNetPence:totalNet.toString(),
    admittedLineVatPence:admittedVat.toString(),targetVatPence:(admittedVat+pence(newDelta)).toString(),
    targetGrossPence:(totalNet+admittedVat+pence(newDelta)).toString(),
    suppressVisibleRoundingLine: Boolean(roundingMovement) && newDelta==='0',
    memberWitnesses:witnesses.sort((a,b)=>a.movementId.localeCompare(b.movementId))};
  // Fixture hash only; SQL production uses the native jsonb-domain hash owner.
  return {...authority,fixtureSha256:sha('WEEKLY_SOURCE_INVOICE_ROUNDING_AUTHORITY_V1',authority)};
}
