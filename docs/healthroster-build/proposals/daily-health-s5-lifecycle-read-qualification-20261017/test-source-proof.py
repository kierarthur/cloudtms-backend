"""Synthetic source counterexamples only; no observed native results are generated."""
import copy
import importlib.util
import json
import pathlib
import unittest

HERE=pathlib.Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('s5_source_proof',HERE/'source-proof.py')
proof=importlib.util.module_from_spec(spec)
spec.loader.exec_module(proof)

class SourceBoundary(unittest.TestCase):
    def setUp(self):
        self.sources=proof.load()
        self.relations=json.loads((HERE/'reference/source_relation_metadata.json').read_text())
        self.policies=json.loads((HERE/'reference/source_policy_metadata.json').read_text())

    def inspect(self):
        return proof.inspect(self.sources,self.relations,self.policies)

    def change(self,name,before,after):
        self.assertEqual(self.sources[name].count(before),1)
        self.sources[name]=self.sources[name].replace(before,after)

    def test_actual_complete_source_is_conditional_never_native_qualification(self):
        result=self.inspect()
        self.assertEqual(result['sourceStatus'],'CONDITIONAL_EXACT_OFFICE_MANUAL_FIXTURE_ONLY')
        self.assertFalse(result['nativeQualification'])
        self.assertEqual(result['nativeInvocation'],'NOT_RUN')
        self.assertFalse(result['normalLifecycleRouteUnconditionallyReadOnly'])

    def test_payload_flag_before_hash_is_rejected_even_when_posthash_use_still_exists(self):
        self.change('timesheet_lifecycle_guard_signature_v1',
          '  v_signature := MD5(v_signature_payload::text);',
          '  IF COALESCE(p_include_payload, false) THEN v_signature_payload := \'{}\'::jsonb; END IF;\n  v_signature := MD5(v_signature_payload::text);')
        with self.assertRaisesRegex(ValueError,'pre-hash'):
            self.inspect()

    def test_diagnostic_moved_outside_false_gate_is_rejected(self):
        self.change('timesheet_lifecycle_guard_signature_v1',
          '  IF COALESCE(p_include_payload, false) THEN','  IF true THEN')
        with self.assertRaisesRegex(ValueError,'false-arm gate'):
            self.inspect()

    def test_hidden_insert_before_hash_is_rejected(self):
        self.change('timesheet_lifecycle_guard_signature_v1',
          '  v_signature := MD5(v_signature_payload::text);',
          '  INSERT INTO public.audit_events(action) VALUES (\'SYNTHETIC_TEST\');\n  v_signature := MD5(v_signature_payload::text);')
        with self.assertRaisesRegex(ValueError,'non-SELECT'):
            self.inspect()

    def test_one_source_absence_guard_cannot_cover_both_associations(self):
        self.change('timesheet_lifecycle_guard_signature_v1',
          'f.root_family_booking_id=v_current_ts.booking_id',
          'f.root_family_booking_id IS NOT NULL')
        with self.assertRaisesRegex(ValueError,'Source association'):
            self.inspect()

    def test_zero_candidate_identity_must_not_reach_daily_projection(self):
        self.change('_candidate_record_capabilities_v1','if v_daily_identity_count=1 then','if v_daily_identity_count=0 then')
        with self.assertRaisesRegex(ValueError,'projection guard'):
            self.inspect()

    def test_contractless_fixture_requires_original_early_refusal(self):
        self.change('_candidate_record_capabilities_v1',
          'or v_timesheet.contract_id is null then',
          'or v_timesheet.contract_id is not null then')
        with self.assertRaisesRegex(ValueError,'early refusal'):
            self.inspect()

    def test_candidate_identity_must_bind_actual_booking_origin(self):
        self.change('_candidate_record_capabilities_v1',
          "origin.idempotency_key like 'candidate-daily-first:%'",
          "origin.idempotency_key like '%'")
        with self.assertRaisesRegex(ValueError,'identity association'):
            self.inspect()

    def test_component_new_effect_is_rejected(self):
        self.change('_candidate_signature_component_v1',
          '  return jsonb_build_object(',
          '  PERFORM public._temp_diag_log(\'X\',\'Y\',NULL,\'{}\'::jsonb);\n  return jsonb_build_object(')
        with self.assertRaisesRegex(ValueError,'new component routine'):
            self.inspect()

    def test_relation_view_or_custom_rls_expression_cannot_be_assumed_readonly(self):
        self.relations[0]['kind']='v'
        with self.assertRaisesRegex(ValueError,'heap/owner'):
            self.inspect()
        self.setUp()
        self.policies[0]['using']='private.hidden_effect()'
        with self.assertRaisesRegex(ValueError,'policy expression'):
            self.inspect()

    def test_actual_inventory_and_all_complete_owner_pins(self):
        result=proof.run()
        self.assertEqual(result['actualOwnerFinals'],9)
        self.assertEqual(len(result['grammar']),9)
        self.assertTrue(all(x['nativeBodyCompilation']=='NOT_PROVED' for x in result['grammar']))

    def test_root_native_observation_does_not_invoke_or_qualify_false_arm(self):
        binding=json.loads((HERE/'NATIVE_DEFINITION_BINDING.json').read_text())
        self.assertTrue(binding['completeInnerBodyRawEqual'])
        self.assertFalse(binding['completeFunctionHeadersIdentical'])
        self.assertFalse(binding['newlineOrSqlNormalisationApplied'])
        self.assertEqual(binding['nativeFalseArmInvocation'],'NOT_RUN')
        self.assertEqual(binding['installedTransitiveDependencies'],'NOT_PROVED')

if __name__=='__main__':
    unittest.main(verbosity=2)
