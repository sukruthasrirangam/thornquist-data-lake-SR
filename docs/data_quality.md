# Data quality gate (`glue/dq_gate.py`)

The quality gate sits between **staging** (conformed, unchecked) and **trusted**. Every sale line is tested
against the rules below. A line that passes goes to `trusted/`. A line that fails goes to `quarantine/`
with a `dq_reason` that names **every** rule it broke. Nothing is deleted, so rejected records can be
investigated, fixed at the source and re-run.

## Rules

| # | Dimension | Rule | Action |
|---|---|---|---|
| 1 | Completeness | `transaction_id`, `quantity`, `unit_price` present; `cust_num` present unless the sale is a walk-in | Quarantine |
| 2 | Validity | `sale_date` could be parsed (original text kept in `sale_date_original`) | Quarantine |
| 3 | Uniqueness | `source_system + transaction_id + line_number` appears once; later copies are re-sends | Quarantine copies, keep the first |
| 4 | Referential integrity | `cust_num` exists in the customer extract (walk-ins are exempt by design) | Quarantine |
| 5 | Range | 0 < quantity ≤ 100 and 0 < unit_price ≤ $5,000 | Quarantine |
| 6 | Domain | category, region and channel are known values | Quarantine |
| 7 | Uniqueness (customers) | the same email on more than one `cust_num` | **Flag only** (`dq_warning`); the customer is kept |

Rule 7 flags instead of rejecting because two accounts with one email may be the same person *or* a shared
family email; a person must decide, so the gate must not silently drop a real customer.

Walk-ins (`cust_num = 0000000` in POS) are **valid sales, not orphans**. The ETL marks them `is_walk_in = true`
and the gate promotes them.

## Results on the sample data (Aug 24–30, 2026)

| Source | Lines checked | Passed | Quarantined | Revenue quarantined |
|---|---|---|---|---|
| POS (in-store) | 1,051 | 1,048 | 3 | $720.27 (0.3%) |
| E-commerce (online) | 654 | 553 | 101 | $20,319.17 (16.1%) |
| **Total** | **1,705** | **1,601** | **104** | **$21,039.44** |

| Reason | Source | Lines | Revenue at risk |
|---|---|---|---|
| orphan_customer | e-commerce | 90 (35 orders) | $18,361.87 |
| duplicate_record | e-commerce | 15 (5 orders re-sent by webhook) | $2,943.95 |
| duplicate_record | POS | 3 (terminal re-send) | $720.27 |

4 lines were both duplicates *and* orphans, so per-reason counts add to 108 while 104 lines were quarantined.
4 customer records (2 pairs) share an email and were flagged.

**Business impact:** 1 in 6 online dollars could not be attributed to a known customer, and 16.6% of
in-store revenue comes from anonymous walk-ins, so loyalty analysis cannot see those customers.

## Rule self-test

The sample data never triggers some rules (range, domain, missing values), so a passing run would not prove
those rules work. On every run the job therefore injects **one deliberately broken row per rule** (plus a
clean control row and a duplicate pair), checks each got exactly the expected reason, and writes the results
to `quality/rule_self_test/`. If any test fails, the job **fails on purpose**, which triggers the
EventBridge → SNS failure email. Test rows never reach trusted or quarantine.

## Outputs

| Path | Glue table | What it holds |
|---|---|---|
| `trusted/sales_lines/` | `thornquist_trusted.sales_lines` | lines that passed, partitioned by source_system |
| `trusted/customers/` | `thornquist_trusted.customers` | all customers + `dq_warning` |
| `quarantine/quarantined_sales/run_date=…/` | `thornquist_quality.quarantined_sales` | failed lines + `dq_reason`, one folder per run (history kept) |
| `quality/dq_metrics/run_date=…/` | `thornquist_quality.dq_metrics` | rows and revenue per rule per run (data health trend) |
| `quality/rule_self_test/run_date=…/` | `thornquist_quality.rule_self_test` | PASS/FAIL per test case |

## Live demo batch

`data/demo/pos_sales_2026-08-30_terminal7_late_batch.csv` is a late file from a misconfigured till: 8 rows,
each breaking exactly one rule. Running it through the pipeline quarantines all 8 and leaves trusted
unchanged (1,601 lines). One row has a unit price of $99,999.00: without the gate, a single typo would have
added about $100,000 of revenue that never happened.
