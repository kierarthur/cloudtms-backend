Source proposal for root review before any QT CTAS native attempt: retain the
original six reserved rollback-only user fixtures, programme policy/command IDs,
scope, shared-turn and full restoration requirements. The sole additional
control objects are fresh session-temporary `hr_contact_qt_audit_before` and
`hr_contact_qt_audit_after`, each ON COMMIT DROP and containing exactly one text
digest row from the exact whole audit relation. Check both names absent before
the first CTAS. No audit/business row is copied into either object; no digest is
returned. Both objects exist only inside the original server-owned B transaction,
which unconditionally rolls back and closes. This is not durable application
setup, an additional business fixture, a separate transaction, or a relaxation
of the original 12 complete canonical restoration scopes or any cap. Root must
record actual source admission and any native result separately; this text is
not an execution or acceptance receipt.
