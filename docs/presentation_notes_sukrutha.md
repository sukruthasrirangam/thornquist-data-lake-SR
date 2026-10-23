# Sukrutha: presentation notes (4 minutes)

**Opening line (Ruchi, first 20 seconds, before architecture):**
> "Thornquist's online customers spend $469 per order — 2.3 times the $205 they spend in a store.
> Until this week nobody could see that, because sales lived in three systems that disagreed.
> Here's how we built the lake that made it visible."

## 1. Quality gate, live (1.5 min)
- "Ruchi's ETL fixes *formats*. My gate decides what is *trustworthy*: 7 rules, and failures are quarantined with a reason, never deleted."
- Show **QA1**: 104 of 1,705 lines quarantined. **$18,362 of online sales belong to 35 customers who don't exist in the customer system**: 1 in 6 online dollars.
- **Live batch** (started during Ruchi's section): show **QA4**: 8 new rejects from a late till file, each with its reason.
  "One row says a rain shell cost $99,999. Without the gate, one typo adds $100,000 of revenue that never happened. And trusted still has exactly 1,601 lines."
- Show **QA3**: "Our sample data never breaks some rules, so the job tests itself every run: one broken row per rule. If a rule stops working, the job fails and we get an email."

## 2. Curated layer + cost (45 s)
- `sales_fact`: Parquet, partitioned by year/month and channel, loyalty segment already attached.
- Benchmark: "Same query, CSV vs our curated Parquet table: 224 KB vs 3 KB scanned, 98.6% less data." Whole pipeline ≈ **$0.10–0.15 per weekly run**, nothing always-on.

## 3. Insights (1 min): show the notebook charts
- Online orders 2.3× larger (2.75 items vs 1.19).
- Silver members spend most online ($539/order); Gold members spend *less* online ($347) than Standard ($459): a loyalty opportunity.
- Atlantic is the only region where online beats in-store.
- 21.6% of BC store revenue is from walk-ins with no loyalty ID: a sign-up campaign target.

## 4. Governance (45 s)
- Learner Lab blocks role creation, so: 3 least-privilege IAM policies as design artifacts; **enforced** controls = bucket policy, Athena workgroup with 1 GB/query limit.
- Analyst sees curated only; never raw, staging or quarantine.

## Likely questions
- *Why quarantine instead of delete?* Evidence and recovery: fix at source, rerun, nothing lost.
- *Why are walk-ins not orphans?* They're valid sales with no customer by design; orphans claim a customer who doesn't exist.
- *Why flag duplicate emails instead of rejecting?* Could be one person or a shared family email; a human decides.
- *Why only year/month + channel partitions?* Small data: more partitions = tiny files = slower and costlier.
- *Why did flat Parquet scan less than the partitioned table?* With one tiny file, Parquet's footer metadata lets Athena skip almost everything; partitioning pays off when there are many files and months of data.
- *How do you know the rules work?* The self-test, every run, and the job fails if any rule doesn't fire.
