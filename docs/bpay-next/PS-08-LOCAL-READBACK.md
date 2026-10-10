# PS-08 sole local PG17 catalog readback

Observed10October2026, databasebanking_modal_v2_test via127.0.0.1:55439, cloudtms_bpay_local_owner; SQL selected pg_catalog metadata only. No business function or data executed. Exactlyfourmatching attachments:

| Relation | Name | tgtype | enabled | callback |
| --- | --- | --- | --- | --- |
| private.bpay_next_run_work | bpay_next_run_work_frozen_immutable_v1 | 27 | O | private.bpay_next_effect_immutable_v1() |
| private.bpay_next_run_work | bpay_next_run_work_frozen_no_truncate_v1 | 34 | O | same |
| private.bpay_next_run_line | bpay_next_run_line_frozen_immutable_v1 | 27 | O | same |
| private.bpay_next_run_line | bpay_next_run_line_frozen_no_truncate_v1 | 34 | O | same |

Allfour: tgnargs0, tgattrempty, tgqualNULL, tgconstraint0, notdeferrable/notinitdeferred/notinternal. Type27=BEFORE ROW UPDATE+DELETE;34=BEFORE STATEMENT TRUNCATE. NoINSERTattachment. Solecarried0718DDL and1342callbacksource unchanged. Local loader0migrations/0closuresCOMPILED; db314/816/seal303276902e7265d7dd135614f980bd44d104b40c86d345e4538760c7226e3e71/couplingPASS. ProtectedverificationPENDING, hostedinstallation/app/joinedbusinessNOT_RUN. F24mustrepeatreviewedrelease exactcatalog verification afterownergoahead.
