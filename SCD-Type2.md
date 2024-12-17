# SCD-Type 2
 

---

 **Slowly Changing Dimensions (SCD)**

Slowly Changing Dimensions (SCD) are used to manage changes to dimensional data over time. There are different types of SCDs:

- **Type 0 (Fixed)**: No changes are allowed to historical data.
- **Type 1 (Overwrite)**: Updates replace the existing values.
- **Type 2 (Versioning)**: A new record is inserted with a version number or timestamp.
- **Type 3 (History in Columns)**: Changes are tracked using additional columns.

 **Type 2**, where historical data is preserved by adding a new record and marking the old record as inactive.

---
 

We will use the following tables from the Databricks TPC-H dataset:

1. **`samples.tpch.orders`** (as the target table)
2. **`samples.tpch.orders_update`** (as the source table for updates and inserts)

 implement **Type 2 Slowly Changing Dimensions**, rules:

- A new version of a record is inserted for changes.
- Existing records are marked as inactive.
- New records that don’t match existing records are inserted as active.

We will achieve this by introducing two new columns:
- `is_active`: Marks if a record is the latest version (1 = Active, 0 = Inactive).
- `updated_at`: Captures the timestamp when the record was inserted or updated.

---

 **  Create Target Table (orders_target)**

We will use the `orders` table as the base data and create a target table named `orders_target` with `is_active` and `updated_at` columns.

```sql
CREATE TABLE orders_target AS
SELECT 
    o_orderkey, 
    o_custkey, 
    o_orderstatus, 
    o_totalprice, 
    o_orderdate, 
    1 AS is_active, 
    current_timestamp() AS updated_at
FROM samples.tpch.orders
LIMIT 5; -- Only a subset for simplicity
```

**Initial Data in `orders_target`**:

| o_orderkey | o_custkey | o_orderstatus | o_totalprice | o_orderdate | is_active | updated_at          |
|------------|-----------|--------------|-------------|-------------|-----------|---------------------|
| 1          | 370       | O            | 173665.47   | 1996-01-02  | 1         | 2024-06-17 12:00:00 |
| 2          | 781       | O            | 46929.18    | 1996-12-01  | 1         | 2024-06-17 12:00:00 |
| 3          | 1234      | F            | 193846.25   | 1993-10-14  | 1         | 2024-06-17 12:00:00 |
| 4          | 1369      | O            | 32151.78    | 1995-10-11  | 1         | 2024-06-17 12:00:00 |
| 5          | 445       | F            | 144659.20   | 1994-07-30  | 1         | 2024-06-17 12:00:00 |

---

**  Create Source Table (orders_update)**

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

** Perform MERGE INTO for SCD Type 2**

We will now merge the source table (`orders_update`) into the target table (`orders_target`). Since this is **Type 2**, we will:
1. Mark existing matching records as inactive (`is_active = 0`).
2. Insert the updated version of the records as new rows (`is_active = 1`).
3. Insert new records directly as active.

```sql
MERGE INTO orders_target AS target
USING orders_update AS source
ON target.o_orderkey = source.o_orderkey AND target.is_active = 1
WHEN MATCHED THEN
  UPDATE SET target.is_active = 0
WHEN NOT MATCHED THEN
  INSERT (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, is_active, updated_at)
  VALUES (source.o_orderkey, source.o_custkey, source.o_orderstatus, source.o_totalprice, source.o_orderdate, 1, current_timestamp());
```

---
 **Explanation**

1. **Matching Records (Type 2):**
   - Records with `o_orderkey = 2` are found in the target table. The existing record is marked as inactive (`is_active = 0`).
   - A new version of the record is inserted with updated values and `is_active = 1`.

2. **Non-Matching Records:**
   - Records with `o_orderkey = 6` do not exist in the target table. They are inserted as new active records (`is_active = 1`).

---

 **Resulting Data in `orders_target`**

After the `MERGE INTO` operation, the `orders_target` table looks like this:

| o_orderkey | o_custkey | o_orderstatus | o_totalprice | o_orderdate | is_active | updated_at          |
|------------|-----------|--------------|-------------|-------------|-----------|---------------------|
| 1          | 370       | O            | 173665.47   | 1996-01-02  | 1         | 2024-06-17 12:00:00 |
| 2          | 781       | O            | 46929.18    | 1996-12-01  | 0         | 2024-06-17 12:00:00 | -- Marked as Inactive
| 3          | 1234      | F            | 193846.25   | 1993-10-14  | 1         | 2024-06-17 12:00:00 |
| 4          | 1369      | O            | 32151.78    | 1995-10-11  | 1         | 2024-06-17 12:00:00 |
| 5          | 445       | F            | 144659.20   | 1994-07-30  | 1         | 2024-06-17 12:00:00 |
| 2          | 781       | F            | 50000.00    | 1996-12-01  | 1         | 2024-06-17 12:05:00 | -- New Active Version
| 6          | 500       | O            | 25000.00    | 1997-02-01  | 1         | 2024-06-17 12:05:00 | -- New Record

---

**Achieving Type 2 SCD**

- **Type 2** preserves historical data by adding a new record for changes.
- Existing records are marked as inactive (`is_active = 0`).
- New versions of matching records and completely new records are inserted as active.
- The `updated_at` column helps track when changes occurred.

This approach ensures historical data integrity and tracks changes over time.
