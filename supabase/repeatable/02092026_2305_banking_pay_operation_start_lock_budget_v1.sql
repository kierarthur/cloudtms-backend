-- CloudTMS Banking Pay operation-start lock-budget closure.
-- The established function owner is replayed after historical authorities so
-- the declared 1000ms lock budget cannot be replaced by the former in-body
-- 3s override. No operation branch or payment policy is duplicated here.

\ir 04082026_1154_banking_pay_operation_start.sql

NOTIFY pgrst, 'reload schema';
