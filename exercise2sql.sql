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


set statistics time on;
set statistics time off;
set statistics io on;
set statistics io off;
set showplan_text on;
set showplan_text off;

--1 =   CPU time = 78 ms,  elapsed time = 77 ms; 
select count(*) cnt, sum(oi.quantity) quant
from Staff sa
join "Order" o on sa.idsa=o.idsa
join Customer c on o.idc=c.idc
join OrderItem oi on o.ido=oi.ido
where sa.residence = 'Praha' and c.residence='Praha' and
  o.order_datetime between '2025-01-01' and '2025-01-31'
/*Iteration 1 — Heap (No Indexes)
QEP:

Table Scan on Order → very expensive

Hash Match join → caused by missing indexes

RID lookups on Customer and OrderItem

IO:

Order logical reads = very high (≈2000+)

Staff, Customer, OrderItem also show extra reads

CPU:

High (≈70–80 ms)

Conclusion:

Query is slow because Order is scanned.

Need composite index on Order.*/

CREATE INDEX idx_order_date ON [Order](order_datetime);
/*Iteration 2 — Partial Index (e.g., index on date only)*
QEP:

SQL Server still scans Order

Index on date not used → selectivity not enough alone

Hash Match still present

IO:

Order logical reads remain high

No improvement

CPU:

No improvement

Conclusion:

Partial index is not enough.

Need equality + range in one composite index.*/

CREATE INDEX idx_order_ids_idc_date
ON [Order](idsa, idc, order_datetime); 
--CPU time = 16 ms,  elapsed time = 29 ms.
/*Iteration 3 — Composite Index on Order
QEP:

Index Seek on Order (major improvement)

Nested Loops instead of Hash Match

Still RID lookup on Order (heap)

IO: Order = 1780 logical reads (down from 2179).

CPU: 16 ms (down from 78 ms).

Reason: Composite index supports both equality joins + date filter.

Conclusion: Main bottleneck fixed.*/
CREATE INDEX idx_oi_ido ON OrderItem(ido);
--   CPU time = 16 ms,  elapsed time = 28 ms.

/*QEP:

Index Seek on OrderItem

Still RID lookup (quantity not in index)

IO: OrderItem = 103 logical reads (same as before).

CPU: Stable at ~16 ms.

Reason: Index helps join on ido, but table is heap → RID lookup remains.

Conclusion: Join optimized; covering index optional.*/

CREATE INDEX idx_oi_cover ON OrderItem(ido) INCLUDE (quantity);


ALTER TABLE [Order] ADD PRIMARY KEY (ido);
ALTER TABLE Customer ADD PRIMARY KEY (idc);
ALTER TABLE OrderItem ADD PRIMARY KEY (ido, idp);

-- same SELECT query

/*Iteration 5 — Clustered Table (Optional)
QEP: RID lookups disappear → replaced by Key Lookups.

IO: Drops further because lookups are cheaper.

CPU: Slight improvement.

Reason: Clustered index gives physical order → faster lookups.

Conclusion: Optional but improves lookup cost.*/

/*Iteration 1:
Full table scan on Order, hash join, high IO and CPU → needs composite index.

Iteration 2:
Partial index not used, no improvement → composite index required.

Iteration 3:
Composite index used, index seek + nested loops, IO and CPU drop significantly.

Iteration 4:
OrderItem index used, join optimized, RID lookup remains due to heap.

Iteration 5:
Clustered table removes RID lookups, further IO reduction.*/