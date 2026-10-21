-- =====================================================================
-- Bonus: regional managers see only their region's rows   (owner: Sukrutha)
-- Preferred: Lake Formation data filter (see governance/lakeformation_permissions.md).
-- Fallback if Learner Lab blocks Lake Formation: a view per region, and the
-- regional manager is granted SELECT on the view only, never on sales_fact.
-- =====================================================================
CREATE OR REPLACE VIEW thornquist_curated.v_sales_ontario AS
SELECT *
FROM thornquist_curated.sales_fact
WHERE region = 'Ontario';

-- Demo: the analyst sees 5 regions, the Ontario manager sees 1
SELECT region, COUNT(*) AS line_count FROM thornquist_curated.sales_fact     GROUP BY region ORDER BY region;
SELECT region, COUNT(*) AS line_count FROM thornquist_curated.v_sales_ontario GROUP BY region;
