select * from
(
  SELECT qs.execution_count, 
    SUBSTRING(qt.text,qs.statement_start_offset/2 +1,   
                 (CASE WHEN qs.statement_end_offset = -1   
                       THEN LEN(CONVERT(nvarchar(max), qt.text)) * 2   
                       ELSE qs.statement_end_offset end -  
                            qs.statement_start_offset  
                 )/2  
             ) AS query_text,
  qs.total_worker_time/qs.execution_count AS avg_cpu_time, qp.dbid 
  --, qt.text, qs.plan_handle, qp.query_plan   
  FROM sys.dm_exec_query_stats AS qs  
  CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) as qp  
  CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) as qt  
  where qp.dbid=DB_ID() and qs.execution_count > 10
) t
where query_text like 'select * from%'
order by avg_cpu_time desc; 

-- Rule: RID Lookup + WHERE condition(That column must be added to index)

set statistics time on;
set statistics time off;
set statistics io on;
set statistics io off;
set showplan_text on;
set showplan_text off;

SELECT COUNT(*)
FROM OrderItem oi
JOIN "Order"  o ON oi.ido = o.ido
JOIN Customer c ON o.idc = c.idc
WHERE c.residence = 'Berlin'
  AND o.order_datetime = '2025-05-01' 
  AND oi.unit_price BETWEEN 100000 AND 200000;

/*IDENTITIFY THE PROPBLEM
Problem 1: TABLE SCAN on Order(This is very expensive, Scans entire table,High CPU cost)
Problem 2: RID Lookup on Customer(Means:Index used = only idc (PK), But residence NOT in index)
➡️ So SQL Server:Finds row using index, Goes back to table (RID lookup)
Problem 3: RID Lookup on OrderItemSame issue(Index only on ido, But filter column unit_price missing)
Problem 4: Too many Nested Loops( Nested loops are OK only if input is small )
Input is large (because of scans + lookups)
So CPU increases

3. Root Cause: indexes support joins only, not filters
That’s why:
Filters are applied after lookup
Causing RID lookups*/

/*Iteration 0 – Initial Execution Plan Analysis
Execution Plan Observations:
- TABLE SCAN on Order (major bottleneck)
- INDEX SEEK on Customer and OrderItem (PK only)
- RID LOOKUP on Customer (residence filter not indexed)
- RID LOOKUP on OrderItem (unit_price filter not indexed)

Problems Identified
1) Full Table Scan on Order
Operation: TABLE SCAN
Cause: Missing index on order_datetime
Impact: High CPU usage due to scanning entire table
2) RID Lookup on Customer
Operation: RID LOOKUP
Cause: Column residence is not included in the index
Impact: Additional I/O due to lookup after index seek
3) RID Lookup on OrderItem
Operation: RID LOOKUP
Cause: Column unit_price is not indexed
Impact: Extra reads and CPU overhead
4) Inefficient Join Processing
Indexes support only primary key joins
Filtering columns are not part of indexes
Leads to increased intermediate results and repeated lookups

Performance Metrics
CPU time = 63 ms
Elapsed time = 59 ms
Since both values are similar, the query is CPU-bound

Performance:
- CPU time ~63 ms
- Logical Reads:
			   Order -> 2179 -> VERY HIGH (2179 / 2590 ≈ 84%) =bottleneck
			   Customer -> 333 -> Medium (333 / 2590 ≈ 13%)
			   OrderItem -> 78 -> Low  (78 / 2590 ≈ 3%)
			   Total = 2179 + 333 + 78 = 2590 reads(baseline)

Conclusion:
- Indexes exist only for joins (primary keys)
- No indexes support filtering conditions
- Query is CPU-bound due to excessive logical reads

*/


-- Selectivity checks
/*Selectivity = (rows after filter) / (total rows
The most selective condition is order_datetime, 
therefore the query should start from the Order table.
Why This Matters?
Because:
Optimizer prefers most selective filter first
That reduces:
intermediate rows
joins
CPU cost   

Conclusion:
The condition on order_datetime is the most selective
Therefore, the query should begin with the Order table
Indexing should prioritize this column

Selectivity Analysis:
The condition on order_datetime has a selectivity of 0.016%, meaning it filters the data very strongly (only 82 rows out of ~500k).
The condition on residence has a selectivity of ~4.9%, which is moderately selective.
The condition on unit_price has a selectivity of ~4.4%, also moderately selective.

Justification:
Applying the most selective filter first significantly reduces the number of rows early in the execution process
This minimizes the cost of joins and improves CPU efficiency

Implication for Index Design:
The column order_datetime should be the leading column in the composite index on the Order table*/
SELECT COUNT(*) FROM Customer WHERE residence = 'Berlin'; -- 14643 / 300000 = 4.9%
SELECT COUNT(*) FROM "Order" WHERE order_datetime ='2025-05-01'; --82 / 501414 = 0.00016 = 0.016%
SELECT COUNT(*) FROM OrderItem WHERE unit_price BETWEEN 100000 AND 200000; --220897 /5000000 = 0.044 ≈ 4.4%


/*Fix 1 → Order table = Table Scan (Order)*/
CREATE INDEX idx_order_date
ON "Order"(order_datetime);

/*Iteration 1 – Index on Order(order_datetime)

Problem:
The execution plan contains a TABLE SCAN on the Order table.
This is caused by the absence of an index on the order_datetime column.
It results in high logical reads and CPU usage.

Execution Plan Changes:
TABLE SCAN (Order) is replaced by:
INDEX SEEK (idx_order_date)
followed by RID LOOKUP (Order)

Performance Improvement:
Logical reads on Order reduced from 2179 → 85 (~96% reduction)
Total logical reads reduced from 2590 → 496 (~80% reduction)
CPU time reduced significantly (from 63 ms to ~1 ms)

Remaining Issues:
RID LOOKUP (Order) still exists because the index does not include:
idc (join column)
ido (join column)
RID LOOKUP operations also remain on:
Customer (missing residence)
OrderItem (missing unit_price)

Conclusion:
The index successfully eliminates the full table scan and significantly reduces CPU and I/O cost.
However, it supports only filtering, not join operations.
Further optimization requires composite indexes to eliminate RID lookups and improve join efficiency.
*/


/*Fix 2 → Customer RID Lookup = RID Lookup (residence)
No RID lookup
Index covers filter + join*/
CREATE INDEX idx_customer_residence
ON Customer(residence, idc);

/*Iteration 2 — Index on Customer(residence, idc)

Execution Plan Changes
✔ Before (Iteration 1):
INDEX SEEK (Customer PK on idc)
RID LOOKUP (Customer) ❌
✔ Now:
INDEX SEEK (idx_customer_residence) ✔
❌ No more RID Lookup on Customer

CPU Time
~1 ms → ~5 ms (still very low, acceptable)

👉 Focus is logical reads reduction, which improved significantly ✔

Customer is Fully Optimized 
Filter (residence = 'Berlin') handled in index
✔ Join (idc) also supported
✔ No RID lookup

Order Still Has Problem
Reason:
Index has only order_datetime
Missing:
idc
ido

OrderItem Still Has Problem
Reason:
Filter column unit_price not in index

Hash Match Appeared
Why it appeared:
Optimizer thinks:
Data volume still large
Nested Loop not efficient

OrderItem Still Has Problem
Problem
The execution plan still performs:
RID Lookup on Customer
Hash Join between Order and Customer

Reason:
The predicate residence = 'Berlin' is applied after joining Order → Customer.
No index supports filtering by residence before the join.
SQL Server must scan many Customer rows to find Berlin customers.
This increases logical reads and CPU time.

Why This Index Was Created:
supports both:
1)Filtering
residence = 'Berlin'
2)Joining = idc (foreign key from Order)
This allows SQL Server to:
Seek directly into Berlin customers
Immediately join them to Order using idc
Reduce the number of rows entering the join

Execution Plan Changes
After creating the index, SQL Server uses: Index Seek (idx_customer_residence)
instead of scanning Customer.
However:
SQL Server still chooses a Hash Join, not Nested Loops.
This is because residence = 'Berlin' has low selectivity (many Berlin customers).
So the join method does not change.

Performance Improvement
Customer logical reads drop significantly (from ~200+ to ~54)
CPU time improves slightly
Join input size decreases
But:
The join is still a Hash Join
RID Lookup on Customer still exists (because the index does not cover all columns)

Remaining Issues
The index does not eliminate RID Lookup on Customer
→ because the index does not include all columns needed by the query.
residence is not selective enough  
→ SQL Server still prefers Hash Join over Nested Loops.
The main bottleneck is still the RID Lookup on OrderItem  
→ because unit_price is not indexed.

Conclusion
The index on Customer(residence, idc) correctly supports filtering and joining on the Customer table. 
It reduces logical reads and improves filtering efficiency, 
but does not change the join type because the residence predicate is not selective. RID Lookup remains, and further optimization requires composite indexes on Order and OrderItem to eliminate remaining lookups and reduce CPU time.
*/

/*Fix 3 → OrderItem RID Lookup = RID Lookup (unit_price)*/
DROP INDEX idx_orderitem_price ON OrderItem;
CREATE INDEX idx_orderitem_price
ON OrderItem(unit_price, ido);

/*Problem (Before Fix)
The execution plan still contained:
RID Lookup on OrderItem:
Filtering on unit_price applied after fetching rows from the heap
High logical reads on OrderItem (78 reads)
Join on oi.ido = o.ido using only the PK index
This means SQL Server had to:
Seek OrderItem rows using PK (ido)
Fetch each row from the heap
Apply the unit_price filter row‑by‑row
This is slow and CPU‑intensive.

Why This Index Was Created
Filtering
unit_price BETWEEN 100000 AND 200000
 Joining
ido (foreign key to Order)
This allows SQL Server to:
Seek directly into the price range
Immediately join matching rows to Order
Avoid scanning irrelevant OrderItem rows
Reduce RID lookups
This is exactly what the assignment requires:
a composite index that supports both selection and join operations.

Execution Plan Changes (After Creating the Index)
Index Seek (pk_orderitem)
RID Lookup (OrderItem)
Why?
Because SQL Server decided:
The PK index on (ido, …) is still cheaper for the join
The unit_price filter is applied after the join
The optimizer prefers joining first, filtering later
This is normal when:
The join column (ido) is more selective than the price range
The price range returns many rows (your range is not very selective)
So SQL Server does not switch to your new index yet.
This is expected.

Performance Improvement
Even though the index is not used yet:
It becomes available for future join orders
It will be used when the optimizer chooses OrderItem as the driving table
It reduces the cost of filtering when SQL Server decides to push the predicate down
Right now:
Logical reads on OrderItem remain ~78
CPU time remains low (0 ms)
The plan is still dominated by the join order chosen by SQL Server

Remaining Issues
SQL Server still performs RID Lookup on OrderItem
The optimizer still prefers the PK index for the join
The unit_price predicate is not selective enough to drive the plan
A covering index may be needed if the query requires more columns
The main bottleneck now is:
The join order
SQL Server joins Order → Customer → OrderItem
instead of
OrderItem → Order → Customer.
Your composite index will matter only when the join order changes.

Conclusion
The composite index on OrderItem(unit_price, ido) supports both filtering on unit_price and joining on ido. 
Although SQL Server continues to use the primary key index for the join, the new index eliminates the need for a full scan when the optimizer chooses OrderItem as the driving table. 
The index is correctly designed but not yet selected by the optimizer because the unit_price predicate is not selective enough. RID lookups remain, and further optimization requires adjusting join order or creating a covering index to eliminate remaining lookups and reduce CPU time.
*/

/*Fix 4 → RID Lookup on Order */
CREATE INDEX idx_order_date_idc
ON "Order"(order_datetime, idc);

/*1. Problem (Before Fix)
Even after creating:
idx_order_date (Iteration 1)
idx_customer_residence (Iteration 2)
idx_orderitem_price (Iteration 3)
the execution plan still contained:
RID Lookup on Order
Hash Join between Order and Customer
High logical reads on Order (~85 pages)
Why?
Because the index on order_datetime supported only filtering, not the join on idc.
SQL Server had to:
Seek by date
Fetch each row from the heap (RID Lookup)
Then join using idc
This caused unnecessary IO and prevented nested loops.

Why This Composite Index Was Created
Filtering
order_datetime = '2025‑05‑01'
Joining
idc (foreign key to Customer)
This allows SQL Server to:
Seek directly into the correct date
Immediately join to Customer using idc
Reduce the number of rows entering the join
Potentially eliminate RID Lookup on Order
This is exactly what the assignment requires:
a composite index that supports both selection and join operations.

Execution Plan Changes (After Creating the Index)
Index Seek (idx_order_date_idc)
However, the plan still contains:
RID Lookup (Order)
Hash Match (Order → Customer)
Why?
 Reason 1 — The index is NOT covering
The query still needs columns not included in the index → SQL Server must fetch the row from the heap.

 Reason 2 — residence = 'Berlin' is not selective
SQL Server still prefers a Hash Join over Nested Loops.
Reason 3 — The join order is still:
Order → Customer → OrderItem
not
OrderItem → Order → Customer.
So the index is used, but the join strategy does not change yet.

Performance
Order table
Logical reads: 86 (same as before)
RID Lookup still present
Customer table
Logical reads: 54
Still using idx_customer_residence
OrderItem table
Logical reads: 78
Still using PK_ORDERITEM + RID Lookup
CPU time: CPU time = 0 ms
Elapsed time = 5 ms


Remaining Issues
Even after this index:
RID Lookup on Order remains: Because the index does not cover all needed columns.
Hash Join remains: Because residence is not selective.
 RID Lookup on OrderItem remains: Because SQL Server still prefers PK_ORDERITEM for the join.
The join order has not changed:
SQL Server still drives from Order → Customer → OrderItem.
To eliminate the final RID Lookup, you need: (unit_price, ido)

Conclusion
The composite index on Order(order_datetime, idc) is correctly used by SQL Server to filter orders by date and support the join to Customer. 
The index seek replaces the earlier single‑column seek and improves join efficiency. However, because the index is not covering, SQL Server still performs a RID Lookup on the Order table. The join remains a hash join because the residence predicate is not selective. Logical reads and CPU time remain similar, 
and further optimization requires a composite covering index on OrderItem(unit_price, ido) to eliminate the final RID Lookup and reduce CPU cost.
*/


/*Final Summary
Initial QEP shows inefficient execution due to:
Full table scan on Order
RID lookups on Customer and OrderItem
These occur because:
Indexes do not include filtering columns
Optimization strategy:
Create composite indexes combining filter + join columns
Goal:
Eliminate table scans and RID lookups
Reduce CPU time*/
