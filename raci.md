# Access Matrix and RACI

## Who can see what (zone access)
R = read, W = write, — = no access

| Role | landing | raw | staging | trusted | quarantine | curated |
|---|---|---|---|---|---|---|
| Ingestion (script, Lambda, customer job) | R/W | W | — | — | — | — |
| ETL (raw-to-staging, dq-gate, trusted-to-curated) | — | R | R/W | R/W | W | W |
| Data engineer (investigates failures) | R | R | R | R | R | R |
| Analyst | — | — | — | — | — | R |
| Regional manager | — | — | — | — | — | R (own region rows only) |

## RACI for activities
R = Responsible (does it), A = Accountable (owns the outcome), C = Consulted, I = Informed

| Activity | Data engineer | Analytics lead | Analyst | Regional manager |
|---|---|---|---|---|
| Ingest source files | R, A | I | — | — |
| Fix schema conflicts | R, A | C | — | — |
| Review quarantined records | R | A | I | — |
| Fix bad data at source system | C | A | — | R (store data) |
| Maintain curated tables | R | A | C | — |
| Grant data access | R | A | — | I |
| Answer business questions | C | A | R | I |
