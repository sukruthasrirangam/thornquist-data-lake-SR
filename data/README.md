# Sample Data -- Project 1: RetailLake (Thornquist Outfitters)

These files simulate the three source systems described in the project brief.
They are synthetic but deliberately messy in the same ways the brief asks you
to detect and handle -- don't clean them before you've built your data-quality
gate; the mess is the point.

## Files

| File | Represents | Notes |
|---|---|---|
| `pos_sales_2026-08-24_to_2026-08-30.csv` | Daily POS exports from all 10 stores, one week | ~1,050 rows |
| `ecommerce_orders.json` | E-commerce order export from the storefront platform | ~265 orders, nested `items` array |
| `customer_extract.csv` | Weekly flat-file export from the legacy on-prem customer database | 400 customers |

## Deliberately seeded issues (map to Requirements section 3-5 of the brief)

- **Inconsistent date formats**: most `pos_sales` rows use `YYYY-MM-DD`, but a
  small fraction use `DD/MM/YYYY` -- the kind of thing a single POS terminal
  with a different locale setting would produce.
- **Placeholder customer IDs**: walk-in POS sales use `cust_num = "0000000"`
  instead of a real customer reference.
- **Duplicate transactions**: a handful of POS rows are exact duplicates
  (simulating a terminal re-sending a batch).
- **Different customer key names**: the e-commerce export calls its customer
  reference `cust_ref`, not `cust_num` -- you'll need to reconcile this against
  `customer_extract.csv`'s `cust_num` column.
- **Orphaned customer references**: a few `cust_ref` / `cust_num` values in the
  sales data do not exist in `customer_extract.csv` (simulating a customer who
  was deleted or never fully synced).
- **Duplicate e-commerce orders**: a few orders appear twice, simulating a
  webhook retry.
- **Stray whitespace/garbage characters**: a couple of `product_name` values in
  the POS file contain a non-breaking space or leading/trailing whitespace.

## Suggested use

Land these three files as-is into your `landing` zone exactly as provided.
Your ingestion mechanisms (Lambda for the JSON, a scheduled load for the CSVs)
should move them to `raw`, and your Glue ETL/reconciliation step is where the
issues above should be resolved on the way to `trusted` and `curated`.

Feel free to regenerate a larger volume in the same shape if you want more
data to work with -- the generation logic (documented in the project appendix)
is simple enough to extend for extra weeks/stores.
