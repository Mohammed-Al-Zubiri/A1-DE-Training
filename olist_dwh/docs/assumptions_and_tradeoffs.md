# Assumptions & Trade‑offs

This document captures the key assumptions made during the design and implementation
of the Olist data warehouse, as well as the deliberate trade‑offs between competing
goals (simplicity vs. historical accuracy, performance vs. storage, normalisation vs.
query convenience).

---

## Assumptions

### Source Data

1. **Static address history**
   The source `customers` and `sellers` tables each hold a single current
   ZIP code per entity. We assume that this ZIP code accurately represents the
   shipping origin/destination for **all historical orders** placed by that
   customer or fulfilled by that seller. There is no address change log in the
   OLTP schema, so we cannot reconstruct actual per‑order addresses.

2. **Geolocation table precision**
   Analysis showed that the `geolocation` table contains 1 million rows with an
   average coordinate spread of 15.7 km per ZIP code. These coordinates are
   therefore **not reliable for distance calculations**. We drop this table
   entirely and perform location analysis at the city/state level using cleaned
   data from `customers` and `sellers`.

3. **Duplicate `review_id` values**
   The `order_reviews` table contains 789 duplicate `review_id` records, which originate because a customer can post a single review across multiple distinct order items. We treat these differently now by avoiding artificial deduplication; instead mapping each distinct order constraint against a shared `dim_review_comment` payload entity. Note that the composite key `(review_id, order_id)` acts as the natural unique constraint.

4. **Orphaned sellers in `leads_closed`**
   462 of 842 closed leads reference a `seller_id` that does not exist in the
   `sellers` table. We assume these are either sellers that were later deleted
   or a data synchronisation gap. To avoid losing 55% of the funnel data, we map
   them to a dedicated “Unknown” seller (surrogate key `-1`).

5. **Missing product categories**
   Two product categories (`pc_gamer` and `portateis_cozinha_e_preparadores_de_alimentos`)
   lack English translations. We assume they can be manually added before ETL
   and that no other categories are missing.

6. **Zero‑value payments**
   Nine orders have a payment value of 0 (mostly vouchers). We assume these are
   genuine promotional transactions and retain them with a boolean flag
   `is_zero_value`, rather than excluding them.

7. **Order status lifecycle**
   Orders with status `canceled` or `created` are excluded from the fulfillment
   fact because they do not represent shipped products. We assume these statuses
   are final and will not later transition to `delivered`.

8. **Data consistency between customers and geolocation**
   City/state mismatches between the `customers` and `geolocation` tables are
   overwhelmingly cosmetic (e.g., `"sao paulo"` vs `"São Paulo"`). We assume
   the customer‑supplied city is correct and clean only the geolocation‑side
   names during dimension loading.

---

## Trade‑offs

### 1. Separate Fact Tables vs. Consolidated Fact

**Decision:** Five distinct fact tables (`fact_sales`, `fact_order_fulfillment`,
`fact_payments`, `fact_reviews`, `fact_seller_leads`).

**Why:** Each business process has a different natural grain. Merging them into a
single order‑ or order‑item‑level fact would either:
- Aggregate away vital detail (e.g., losing individual payment instalments), or
- Duplicate rows for every combination of payments and reviews, causing
  double‑counting in aggregations.

**Trade‑off:** More tables to manage, but each is simpler, safer for business users,
and query performance is better because tables are narrower and more targeted.

---

### 2. Location Dimension: Zip‑Code Grain (No Coordinates)

**Decision:** Build `dim_location` from cleaned `(zip_code_prefix, city, state)`.
Do not include latitude/longitude.

**Why:** The `geolocation` coordinates are too noisy for meaningful haversine
calculations. Maintaining them would introduce data quality warnings and complex
averaging logic for no analytical gain.

**Trade‑off:** We lose the ability to compute precise distance or map‑based
visualisations. If needed in the future, a `latitude`/`longitude` column can be
added to `dim_location` by integrating an external geocoding service (e.g.,
Google Maps API) – the surrogate key design makes this a non‑breaking change.

---

### 3. SCD Type 1 for Customer and Seller Dimensions

**Decision:** `dim_customer` and `dim_seller` use SCD Type 1 (overwrite on change).
Only the latest demographic data is kept.

**Why:** Analytical queries about customer or seller performance primarily care
about current attributes (e.g., “most valuable customers by current city”). The
historical transaction‑time locations are already captured in the fact tables
(`origin_location_key`, `destination_location_key`), so history is not lost.

**Trade‑off:** We cannot answer “What was the customer’s state at the time of
their first purchase?” without joining to the fact table and looking at
`destination_location_key`. This is acceptable because that analysis concerns
the *transaction*, not the *customer entity*.

**Alternative considered:** SCD Type 2 for customer location would provide
perfect history but add complexity (surrogate key versioning, extra join logic
in ETL). The marginal benefit did not justify the cost for this dataset.

---

### 4. SCD Type 2 for Product Dimension

**Decision:** `dim_product` tracks history using SCD Type 2. Changes to category,
name length, or physical dimensions generate a new row with `effective_from_date`/
`effective_to_date`.

**Why:** Revenue by product category is a core business question. If a product
changes category, we must not retroactively reassign its past sales – that would
corrupt time‑series comparisons.

**Trade‑off:** Slightly more complex ETL (detecting changes, expiring old rows)
and a larger dimension table. Benefits are critical for accurate category‑level
reporting.

---

### 5. Customer Key Denormalised into `fact_payments` and `fact_reviews`

**Decision:** `fact_payments` and `fact_reviews` both carry a `customer_key`
foreign key, even though the customer could be inferred via `order_id`.

**Why:**
- Simplifies queries – no need to join through an order table for customer‑level
  payment/review analysis.
- A single payment or review is unambiguously tied to one customer (the order owner).
- The redundancy is minimal: a 4‑byte integer per row.

**Trade‑off:** Slight storage increase, but columnar databases compress repeated
values efficiently. Risk of inconsistency if payment‑customer links were ever
changed (not applicable in this source).

---

### 6. No Seller Key in `fact_order_fulfillment`

**Decision:** The order‑level fulfillment fact does not reference a seller.

**Why:** An order can contain items from multiple sellers. Forcing a single
`seller_key` would be arbitrary and misleading. Seller‑level delivery performance
can be analysed by joining `fact_order_fulfillment` to `fact_sales` on `order_id`.

**Trade‑off:** Queries for “average delivery time per seller” require a join
between two fact tables, which is slightly more complex. However, this keeps the
order grain pure and prevents silent data distortion.

---

### 7. Pre‑computed Delivery Metrics

**Decision:** `fact_order_fulfillment` stores `days_to_carrier`, `days_to_customer`,
`days_late`, `is_on_time`, and `is_delivered` as physical columns.

**Why:** These metrics are central to the most frequent analytical queries.
Pre‑computing them once during ETL saves every query from recalculating date
differences and improves indexing possibilities.

**Trade‑off:** Extra storage (~40 bytes per order) and slight ETL processing
overhead. Negligible compared to the query performance gain.

---

### 8. Idempotency Strategy: `ON CONFLICT DO NOTHING` vs. Full UPSERT

**Decision:** Fact tables use `INSERT ... ON CONFLICT DO NOTHING` for idempotent
inserts. `fact_order_fulfillment` uses `ON CONFLICT ... DO UPDATE` for late‑arriving
status updates.

**Why:** For a static snapshot, we expect facts to be immutable once loaded.
`DO NOTHING` is the simplest safe pattern. For order fulfillment, status changes
(e.g., “shipped” → “delivered”) may arrive in consecutive runs, so an upsert
captures the latest state.

**Trade‑off:** We cannot automatically correct erroneous fact values that were
loaded previously. A full data correction would require a manual rewrite.

---

### 9. Watermark‑Based Incremental Extraction

**Decision:** Use a `last_extracted` timestamp per fact table stored in
`etl_control`.

**Why:** Enables incremental loads for both static snapshots (first run loads
everything) and hypothetical change‑data‑capture sources. Simple, transparent,
and easy to troubleshoot.

**Trade‑off:** Timestamps must be trusted and consistently formatted. If source
timestamps are ever back‑dated, the watermark may skip rows. In production, we’d
add checksum‑based or log‑based CDC.

---

### 10. Indexing vs. Partitioning

**Decision:** Use indexes only; no table partitioning.

**Why:** The current data volume (~100k orders, ~500k fact rows) does not require
partitioning. Adding it would increase DDL complexity without measurable
performance benefit.

**Trade‑off:** If the dataset grows to tens of millions of rows, the architecture
supports adding range partitioning on `purchase_date_key` with no schema changes
to the fact table columns.

---

### 12. Continuous Metrics Moving to Facts
**Decision:** Continuous numerical attributes (`declared_product_catalog_size`, `declared_monthly_revenue`) were shifted out of `dim_lead` and pushed down into `fact_seller_leads`. The original dimension locations were replaced with banded categories.

**Why:** Proper Kimball modelling insists dimensions be kept for slicing, dicing, and grouping qualitative bands, whereas completely continuous infinite numerical domains risk bloating the dimension or leading to poor surrogate mapping. We store them natively in the fact table where they can be quickly summed or averaged.

**Trade‑off:** A marginally wider fact table layout vs the ability to do unbounded continuous aggregations directly against the leads pipeline.

---
