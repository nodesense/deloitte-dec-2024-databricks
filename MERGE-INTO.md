 
 MERGE INTO also known as upsert

The syntax is as follows:

```sql
MERGE INTO target_table AS target
USING source_table AS source
ON target.condition = source.condition
WHEN MATCHED THEN
  UPDATE SET target.column = source.column
WHEN NOT MATCHED THEN
  INSERT (column1, column2) VALUES (value1, value2);
```

This is particularly useful for managing slowly changing dimensions, deduplication, and incremental updates.

---
 
1. **Target Table (`customers`)** - Contains existing customer data.  
2. **Source Table (`updates`)** - Contains new and updated customer records.  

We will use `MERGE INTO` to **update matching records** and **insert new records** into the `customers` table.
 

```sql
CREATE TABLE customers (
    customer_id INT,
    name STRING,
    email STRING,
    address STRING
);
```

 

```sql
INSERT INTO customers VALUES
(1, 'Alice', 'alice@example.com', '123 Main St'),
(2, 'Bob', 'bob@example.com', '456 Oak St'),
(3, 'Charlie', 'charlie@example.com', '789 Pine St');
```

---

 Create Source Table (updates) 

```sql
CREATE TABLE updates (
    customer_id INT,
    name STRING,
    email STRING,
    address STRING
);
```

**Insert New Data and Updates:**

```sql
INSERT INTO updates VALUES
(2, 'Bob', 'bob_new@example.com', '456 Oak St'), -- Updated Email
(3, 'Charlie', 'charlie@example.com', '789 Pine St'), -- No Change
(4, 'Daisy', 'daisy@example.com', '101 Birch St'); -- New Record
```

---
 
  MERGE INTO  

```sql
MERGE INTO customers AS target
USING updates AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
  UPDATE SET
    target.email = source.email,
    target.address = source.address
WHEN NOT MATCHED THEN
  INSERT (customer_id, name, email, address)
  VALUES (source.customer_id, source.name, source.email, source.address);
```

---

Scenarios

```
 Matching Records:   When `customer_id` matches between the `customers` table (target) and the `updates` table (source), the `email` and `address` fields are updated.  
   - For example, Bob's email is updated to `bob_new@example.com`.  

Non-Matching Records:  
   - When `customer_id` does not exist in the `customers` table, the entire record is **inserted**.  
   - For example, Daisy's record (`customer_id = 4`) is added to the `customers` table.  

 No Action for Unchanged Records   
   - Records that match but have no changes remain unaffected. For example, Charlie's data remains unchanged.
```
---


```
SELECT * FROM customers;
```

---

- `WHEN MATCHED` updates existing records based on the condition.
- `WHEN NOT MATCHED` inserts new records when no match is found.
- This operation combines both update and insert logic in a single statement, improving efficiency and reducing code complexity.


 
