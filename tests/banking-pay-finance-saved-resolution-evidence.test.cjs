const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const historicalPath = 'supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql';
const replacementPath = 'supabase/repeatable/04092026_0910_banking_pay_finance_saved_resolution_evidence_v1.sql';
const consumerPath = 'supabase/repeatable/17082026_2052_pay_finance_resolution_cancel_authority.sql';
const read = relativePath => fs.readFileSync(path.join(root, relativePath), 'utf8').replace(/\r\n/g, '\n');
const sha256 = value => crypto.createHash('sha256').update(value).digest('hex');

function extractFunction(source) {
  const start = source.indexOf('CREATE OR REPLACE FUNCTION public.pay_finance_component_resolutions_apply(');
  assert.ok(start >= 0, 'writer function start is present');
  const marker = '$function$;';
  const end = source.indexOf(marker, start);
  assert.ok(end > start, 'writer function end is present');
  return source.slice(start, end + marker.length);
}

test('saved-resolution replacement is one exact additive owner and leaves the historical monolith byte-identical', () => {
  const historicalBytes = fs.readFileSync(path.join(root, historicalPath));
  assert.equal(sha256(historicalBytes), '8b3cb3e112ae227a80bf2e661272264c3d6145d0e2ff6ad8d88e8eee2db1553f');

  const replacement = read(replacementPath);
  assert.equal((replacement.match(/CREATE OR REPLACE FUNCTION/gi) || []).length, 1);
  assert.equal((replacement.match(/pay_finance_component_resolutions_apply\s*\(/gi) || []).length, 4);
  assert.ok(replacement.includes('Historical 26052026_2100HRS_NEW_FUNCTIONS.sql remains byte-identical.'));
  assert.ok(replacement.includes('SECURITY DEFINER'));
  assert.ok(replacement.includes("SET search_path TO 'public'"));
  assert.ok(replacement.includes('ALTER FUNCTION public.pay_finance_component_resolutions_apply(uuid,jsonb,uuid,uuid,text) OWNER TO postgres;'));
  assert.ok(replacement.includes('REVOKE ALL ON FUNCTION public.pay_finance_component_resolutions_apply(uuid,jsonb,uuid,uuid,text) FROM PUBLIC, anon, authenticated;'));
  assert.ok(replacement.includes('GRANT EXECUTE ON FUNCTION public.pay_finance_component_resolutions_apply(uuid,jsonb,uuid,uuid,text) TO service_role;'));
  assert.doesNotMatch(replacement, /\b(?:BEGIN|COMMIT|ROLLBACK)\s*;/i);
  assert.doesNotMatch(replacement, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});

test('replacement differs from the established writer in only the saved evidence envelope', () => {
  const historicalFunction = extractFunction(read(historicalPath));
  let replacementFunction = extractFunction(read(replacementPath));

  assert.equal((replacementFunction.match(/'resolution_family', 'TAXABLE_CHANNEL_RESTRUCTURE'/g) || []).length, 2);
  assert.equal((replacementFunction.match(/p_target_basis_json\s*=>\s*v_resolution_payload_json/g) || []).length, 1);
  assert.doesNotMatch(replacementFunction, /p_target_basis_json\s*=>\s*v_target_basis_json/);

  replacementFunction = replacementFunction
    .replace("        'resolution_family', 'TAXABLE_CHANNEL_RESTRUCTURE',\n", '')
    .replace("        'resolution_family', 'TAXABLE_CHANNEL_RESTRUCTURE',\n", '')
    .replace('p_target_basis_json         => v_resolution_payload_json', 'p_target_basis_json         => v_target_basis_json');

  assert.equal(replacementFunction, historicalFunction,
    'no validation, calculation, amount, VAT, PAYE/Umbrella, event or result behavior may drift');
});

test('replacement emits exactly the strict evidence identity consumed by current preview and clear owners', () => {
  const replacement = read(replacementPath);
  const consumer = read(consumerPath);
  for (const marker of [
    "'resolution_family', 'TAXABLE_CHANNEL_RESTRUCTURE'",
    'p_target_basis_json         => v_resolution_payload_json',
    'saved_resolution_payload_json = v_resolution_payload_json',
    'saved_resolution_result_json = v_resolution_result_json'
  ]) assert.ok(replacement.includes(marker), marker);
  for (const marker of [
    "coalesce(pfc.saved_resolution_payload_json, pfc.saved_resolution_result_json, '{}'::jsonb)",
    "component_row.saved_resolution_payload_json->>'resolution_family'",
    "component_row.saved_resolution_result_json->>'resolution_family'",
    "= 'TAXABLE_CHANNEL_RESTRUCTURE'",
    'WORKBENCH_FINANCE_RESOLUTION_OWNER_AMBIGUOUS'
  ]) assert.ok(consumer.includes(marker), marker);
});

test('mutation guards kill either missing family identity or the old transient fingerprint basis', () => {
  const source = read(replacementPath);
  const missingOneFamily = source.replace("        'resolution_family', 'TAXABLE_CHANNEL_RESTRUCTURE',\n", '');
  const restoredOldBasis = source.replace('p_target_basis_json         => v_resolution_payload_json', 'p_target_basis_json         => v_target_basis_json');
  assert.equal((missingOneFamily.match(/'resolution_family', 'TAXABLE_CHANNEL_RESTRUCTURE'/g) || []).length, 1);
  assert.match(restoredOldBasis, /p_target_basis_json\s*=>\s*v_target_basis_json/);
  assert.throws(() => {
    assert.equal((missingOneFamily.match(/'resolution_family', 'TAXABLE_CHANNEL_RESTRUCTURE'/g) || []).length, 2);
  });
  assert.throws(() => {
    assert.equal((restoredOldBasis.match(/p_target_basis_json\s*=>\s*v_resolution_payload_json/g) || []).length, 1);
  });
});
