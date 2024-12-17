 **Slowly Changing Dimensions (SCD)**

Slowly Changing Dimensions (SCD) are used to manage changes to dimensional data over time. There are different types of SCDs:

- **Type 0 (Fixed)**: No changes are allowed to historical data.
- **Type 1 (Overwrite)**: Updates replace the existing values.
- **Type 2 (Versioning)**: A new record is inserted with a version number or timestamp.
- **Type 3 (History in Columns)**: Changes are tracked using additional columns.

In this example, we will focus on **Type 3**, where historical changes are tracked in specific columns while retaining both old and new values in the same row.

---

1. **`samples.tpch.orders`** (as the target table)
2. **`samples.tpch.orders_update`** (as the source table for updates and inserts)

 **Type 3 Slowly Changing Dimensions**, rules:
- A new column is added to track the previous value.
- Existing records are updated to reflect the new value in the primary column while retaining the old value in a separate column.

We will introduce two new columns:
- `previous_o_totalprice`: Tracks the previous total price.
- `updated_at`: Captures the timestamp when the update occurred.

---

**Create Target Table (orders_target)**

We will use the `orders` table as the base data and create a target table named `orders_target` with additional columns for Type 3 tracking.

```sql
CREATE TABLE orders_target AS
SELECT 
    o_orderkey, 
    o_custkey, 
    o_orderstatus, 
    o_totalprice, 
    NULL AS previous_o_totalprice, 
    o_orderdate, 
    current_timestamp() AS updated_at
FROM samples.tpch.orders
LIMIT 5; -- Only a subset for simplicity
```

**Initial Data in `orders_target`**:

| o_orderkey | o_custkey | o_orderstatus | o_totalprice | previous_o_totalprice | o_orderdate | updated_at          |
|------------|-----------|--------------|-------------|-----------------------|-------------|---------------------|
| 1          | 370       | O            | 173665.47   | NULL                  | 1996-01-02  | 2024-06-17 12:00:00 |
| 2          | 781       | O            | 46929.18    | NULL                  | 1996-12-01  | 2024-06-17 12:00:00 |
| 3          | 1234      | F            | 193846.25   | NULL                  | 1993-10-14  | 2024-06-17 12:00:00 |
| 4          | 1369      | O            | 32151.78    | NULL                  | 1995-10-11  | 2024-06-17 12:00:00 |
| 5          | 445       | F            | 144659.20   | NULL                  | 1994-07-30  | 2024-06-17 12:00:00 |

---

**Create Source Table (orders_update)**

The `orders_update` table will contain new and updated orders. We will insert data into this table.

```sql
CREATE TABLE orders_update (
    o_orderkey INT,
    o_custkey INT,
    o_orderstatus STRING,
    o_totalprice DOUBLE,
    o_orderdate DATE
);

INSERT INTO orders_update VALUES
(2, 781, 'F', 50000.00, '1996-12-01'), -- Updated Total Price and Status
(6, 500, 'O', 25000.00, '1997-02-01'); -- New Order
```

**Data in `orders_update`**:

| o_orderkey | o_custkey | o_orderstatus | o_totalprice | o_orderdate |
|------------|-----------|--------------|-------------|-------------|
| 2          | 781       | F            | 50000.00    | 1996-12-01  | -- Updated Total Price and Status
| 6          | 500       | O            | 25000.00    | 1997-02-01  | -- New Record

---

**Perform MERGE INTO for SCD Type 3**

We will now merge the source table (`orders_update`) into the target table (`orders_target`). Since this is **Type 3**, we will:
1. Update matching records to retain the previous total price in the `previous_o_totalprice` column.
2. Update the `o_totalprice` to the new value.
3. Insert new records directly.

```sql
MERGE INTO orders_target AS target
USING orders_update AS source
ON target.o_orderkey = source.o_orderkey
WHEN MATCHED THEN
  UPDATE SET 
    target.previous_o_totalprice = target.o_totalprice,
    target.o_totalprice = source.o_totalprice,
    target.o_orderstatus = source.o_orderstatus,
    target.updated_at = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT (o_orderkey, o_custkey, o_orderstatus, o_totalprice, previous_o_totalprice, o_orderdate, updated_at)
  VALUES (source.o_orderkey, source.o_custkey, source.o_orderstatus, source.o_totalprice, NULL, source.o_orderdate, current_timestamp());
```

---

**Explanation**

1. **Matching Records (Type 3):**
   - For `o_orderkey = 2`, the existing record is updated:
     - The `previous_o_totalprice` is set to the old value (`46929.18`).
     - The `o_totalprice` is updated to the new value (`50000.00`).
     - The `updated_at` timestamp is refreshed.

2. **Non-Matching Records:**
   - For `o_orderkey = 6`, the record does not exist in the target table, so it is inserted directly.

---

**Resulting Data in `orders_target`**

After the `MERGE INTO` operation, the `orders_target` table looks like this:

| o_orderkey | o_custkey | o_orderstatus | o_totalprice | previous_o_totalprice | o_orderdate | updated_at          |
|------------|-----------|--------------|-------------|-----------------------|-------------|---------------------|
| 1          | 370       | O            | 173665.47   | NULL                  | 1996-01-02  | 2024-06-17 12:00:00 |
| 2          | 781       | F            | 50000.00    | 46929.18              | 1996-12-01  | 2024-06-17 12:05:00 | -- Updated Record
| 3          | 1234      | F            | 193846.25   | NULL                  | 1993-10-14  | 2024-06-17 12:00:00 |
| 4          | 1369      | O            | 32151.78    | NULL                  | 1995-10-11  | 2024-06-17 12:00:00 |
| 5          | 445       | F            | 144659.20   | NULL                  | 1994-07-30  | 2024-06-17 12:00:00 |
| 6          | 500       | O            | 25000.00    | NULL                  | 1997-02-01  | 2024-06-17 12:05:00 | -- New Record

---

**Achieving Type 3 SCD**

- **Type 3** tracks changes by retaining previous values in a separate column.
- Matching records update the `previous_*` column while reflecting new values in the primary column.
- New records are inserted without historical data.
- The `updated_at` column helps track when changes occurred.

This approach ensures that the most recent and prior values coexist in the same row for reporting purposes.
