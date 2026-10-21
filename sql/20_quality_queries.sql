-- =====================================================================
-- Data quality queries  |  database: thornquist_quality   (owner: Sukrutha)
-- =====================================================================

-- QA1. Why records were quarantined in the latest run (a row can break several rules)
SELECT reason,
       source_system,
       COUNT(*)                   AS rows_quarantined,
       ROUND(SUM(line_amount), 2) AS revenue_at_risk
FROM thornquist_quality.quarantined_sales
CROSS JOIN UNNEST(split(dq_reason, ';')) AS t(reason)
WHERE run_date = (SELECT MAX(run_date) FROM thornquist_quality.quarantined_sales)
GROUP BY reason, source_system
ORDER BY revenue_at_risk DESC;

-- QA2. The cost of bad data: revenue that could not be trusted, per source
SELECT source_system,
       SUM(CASE WHEN metric = 'rows_checked'     THEN revenue END) AS revenue_checked,
       SUM(CASE WHEN metric = 'rows_quarantined' THEN revenue END) AS revenue_quarantined,
       ROUND(100 * SUM(CASE WHEN metric = 'rows_quarantined' THEN revenue END)
                 / SUM(CASE WHEN metric = 'rows_checked' THEN revenue END), 1) AS pct_quarantined,
       SUM(CASE WHEN metric = 'rows_checked' THEN row_count END) AS rows_checked,
       SUM(CASE WHEN metric = 'rows_passed'  THEN row_count END) AS rows_passed
FROM thornquist_quality.dq_metrics
WHERE run_date = (SELECT MAX(run_date) FROM thornquist_quality.dq_metrics)
GROUP BY source_system;

-- QA3. Rule self-test: every rule must PASS
SELECT test_case, expected_reason, dq_reason AS actual_reason, test_result
FROM thornquist_quality.rule_self_test
WHERE run_date = (SELECT MAX(run_date) FROM thornquist_quality.rule_self_test)
ORDER BY test_result, test_case;

-- QA4. Show the rejected records themselves (for the live demo)
SELECT source_system, transaction_id, line_number, cust_num, product_name,
       quantity, unit_price, line_amount, dq_reason
FROM thornquist_quality.quarantined_sales
ORDER BY dq_reason, transaction_id, line_number
LIMIT 50;
