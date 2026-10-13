# Joined actual expiry owner — E-only candidate

This combines the independently authored WORK-hold and Case-hold changes to the same actual `04102026_0510_banking_pay_next_preparation_expiry_v1.sql`. Both use the exact saved W preimage, not each other's replacement file. Their disjoint spans are applied once, commute, and fully invert to the same complete saved source. No existing change is overwritten or silently omitted.

The WORK path records its real released hold before known-work carry; the existing scalar/currentness path remains for non-comparison work. The Case path uses its real released Case hold UUID and the current comparison-capacity owner; the ordinary expiry guard remains unchanged. Required core and collection helper definitions must be installed before either path can execute.

The four focused tests prove source composition only. PostgreSQL compilation, actual lease/deadline/freeze/release semantics, simultaneous WORK/Case holds, principal reconciliation and exact replay still require the joined native proof and independent closing review. No W adoption, database action, deployment or completion award is made by this packet.
