# Migration Failure Analysis: `KeyError: 'name'`

## Executive Summary
The persistent `KeyError: 'name'` failure in `test_search_alias_promotion` is caused by **test pollution** leaking logical replication state from `test_full_replication_flow`. 

Specifically, the `pgai` vectorizer crashes because it attempts to format a string using the variable `$name` (e.g., `Template("V2: $name...")`), but the data row retrieved from the database **does not contain the `name` key**. This indicates the column is missing from the runtime query result, despite static checks showing the column exists on the table.

## Detailed Root Cause

### 1. The Sequence of Events
1.  **`test_full_replication_flow` runs first:**
    *   Configures `products` table with only `id` and `description`.
    *   Creates a Postgres persistent replication slot `sub_products` and publication `pub_products` on the Source.
    *   Creates the Sink table `products` with `id` and `description`.
    *   **Result:** A replication stream is established specifically for minimal columns.

2.  **`test_search_alias_promotion` runs second:**
    *   Reuses the physical `products` table and the `products` configuration key.
    *   **CRITICAL CHANGE:** This test configuration *includes* the `name` column and defines a formatting template that *requires* `$name`.
    *   The test attempts to "upgrade" the existing setup.

### 2. The Failure Mechanism
The `KeyError` proves that `pgai` is fetching a row dictionary that lacks the `name` key. In `psycopg` (the driver used), a `KeyError` on a `Row` object means the **column was not selected/fetched**.

*   **Hypothesis A (Stale Subscription):** If the valid physical `products` table has the `name` column (which logs confirm: `Existing: {id, description, name}`), but the **Replication Subscription** `sub_products` is bound to the *old* publication (which excludes `name`), then the replication stream sends `NULL` (or default) for that column. However, `psycopg` would return `None`, not raise `KeyError`.
*   **Hypothesis B (Stale Worker/Connection):** The `pgai` vectorizer worker runs in a separate `asyncio` task. If this worker starts *before* the `ALTER TABLE` schema evolution commits, or if it reuses a prepared statement/connection snapshot from *before* the column was added, it will execute `SELECT *` on the *old* schema version. 
    *   The logged error occurs repeatedly (`vectorizer finished with errors... sleeping... running...`).
    *   The fact that it *persists* even after my attempted `DROP TABLE` suggests that the **Subscription/Slot** on the source side is retaining the old schema definition, or `pgai` is fundamentally failing to refresh its definition of "what columns to fetch".

### 3. Why the "Fixes" Failed
*   **Fix 1: `DROP TABLE products`**: I added this to `test_search_alias_promotion`. This ensures the table is recreated with the *correct* columns (`id, name, description`).
    *   **Result:** The table *has* the column.
*   **Fix 2: `DROP SUBSCRIPTION sub_products`**: I added this to force a re-subscription.
    *   **Result:** Partial success? If the *Source Publication* `pub_products` wasn't also updated to include `name`, the new subscription would still pull the restricted data set.

## Conclusion and Next Steps
The system is working "correctly" but failing on test/environment complexity:
1.  **Source Publication Stale:** We must ensure `pub_products` on the **Source DB** is updated to include `name`. If `setup_source` (Reconciler) doesn't run or thinks it's already done, `name` never flows.
2.  **Worker Race Condition:** We must ensure the vectorizer worker is NOT started until schema evolution and subscription updates are fully committed and visible.

**Recommended Solution (to be applied carefully):**
Instead of fighting the shared state, we should isolate the tests properly. `test_search_alias_promotion` should use a unique table name (e.g., `alias_products`) or we must strictly enforce a `DROP PUBLICATION` on the source side during cleanup. Use of `make test` exacerbates this as it runs all tests in sequence against persistent Docker containers.
