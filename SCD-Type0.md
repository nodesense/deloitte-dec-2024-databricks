## SCD - Type 0


The `MERGE INTO` statement can be used for implementing Slowly Changing Dimensions (SCD) in data warehousing. 

---

**Slowly Changing Dimensions (SCD)**

Slowly Changing Dimensions (SCD) are used to manage changes to dimensional data over time. There are different types of SCDs:

- **Type 0 (Fixed)**: No changes are allowed to historical data.
- **Type 1 (Overwrite)**: Updates replace the existing values.
- **Type 2 (Versioning)**: A new record is inserted with a version number or timestamp.
- **Type 3 (History in Columns)**: Changes are tracked using additional columns.

In this example, we will focus on **Type 0**, where historical data is fixed and cannot be updated. New records are inserted, but existing records remain untouched.

---


We will use the following tables from the Databricks TPC-H dataset:

1. **`samples.tpch.orders`** (as the target table)
2. **`samples.tpch.orders_update`** (as the source table for updates and inserts)


Type 0 SCD

- Matching records will not be updated (historical data is preserved).
- Non-matching records (new orders) will be inserted into the target table.

---

**Create Target Table (orders_target)**

We will use the `orders` table as the base data and create a target table named `orders_target`.

```sql
CREATE TABLE orders_target AS
SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate
FROM samples.tpch.orders
LIMIT 5; -- Only a subset for simplicity
```

**Initial Data in `orders_target`**:

| o_orderkey | o_custkey | o_orderstatus | o_totalprice | o_orderdate |
|------------|-----------|--------------|-------------|-------------|
| 1          | 370       | O            | 173665.47   | 1996-01-02  |
| 2          | 781       | O            | 46929.18    | 1996-12-01  |
| 3          | 1234      | F            | 193846.25   | 1993-10-14  |
| 4          | 1369      | O            | 32151.78    | 1995-10-11  |
| 5          | 445       | F            | 144659.20   | 1994-07-30  |

---

**Create Source Table (orders_update)**

The `orders_update` table will contain new and potentially updated orders. We will insert data into this table.

```sql
CREATE TABLE orders_update (
    o_orderkey INT,
    o_custkey INT,
    o_orderstatus STRING,
    o_totalprice DOUBLE,
    o_orderdate DATE
);

INSERT INTO orders_update VALUES
(2, 781, 'F', 50000.00, '1996-12-01'), -- Attempted Update to Order 2
(6, 500, 'O', 25000.00, '1997-02-01'); -- New Order
```

**Data in `orders_update`**:

| o_orderkey | o_custkey | o_orderstatus | o_totalprice | o_orderdate |
|------------|-----------|--------------|-------------|-------------|
| 2          | 781       | F            | 50000.00    | 1996-12-01  | -- Updated Total Price and Status
| 6          | 500       | O            | 25000.00    | 1997-02-01  | -- New Record

---

** Perform MERGE INTO for SCD Type 0**

 merge the source table (`orders_update`) into the target table (`orders_target`). Since this is **Type 0**,  **not allow updates** to the existing data. Only new records will be inserted.

```sql
MERGE INTO orders_target AS target
USING orders_update AS source
ON target.o_orderkey = source.o_orderkey
WHEN NOT MATCHED THEN
  INSERT (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate)
  VALUES (source.o_orderkey, source.o_custkey, source.o_orderstatus, source.o_totalprice, source.o_orderdate);
```

---

**Explanation**

1. **Matching Records (Type 0):**
   - Records with `o_orderkey = 2` already exist in `orders_target`. Since we are implementing **Type 0**, these records are **not updated**.

2. **Non-Matching Records:**
   - The record with `o_orderkey = 6` does not exist in `orders_target`. It is inserted as a new record.

---

**Resulting Data in `orders_target`**

After the `MERGE INTO` operation, the `orders_target` table looks like this:

| o_orderkey | o_custkey | o_orderstatus | o_totalprice | o_orderdate |
|------------|-----------|--------------|-------------|-------------|
| 1          | 370       | O            | 173665.47   | 1996-01-02  |
| 2          | 781       | O            | 46929.18    | 1996-12-01  | -- Unchanged (Type 0)
| 3          | 1234      | F            | 193846.25   | 1993-10-14  |
| 4          | 1369      | O            | 32151.78    | 1995-10-11  |
| 5          | 445       | F            | 144659.20   | 1994-07-30  |
| 6          | 500       | O            | 25000.00    | 1997-02-01  | -- Inserted

---

**Summary: Achieving Type 0 SCD**

- **Type 0** ensures historical data is fixed.
- Matching records are ignored during the `MERGE` (no updates occur).
- Non-matching records (new data) are inserted into the table.
- The `MERGE INTO` operation only uses `WHEN NOT MATCHED` to insert new records.

This approach preserves historical data integrity, which is ideal for scenarios where past records should remain untouched while allowing new data to be added.
