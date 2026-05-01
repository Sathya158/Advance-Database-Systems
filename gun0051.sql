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


select distinct c.residence as cust
from Customer c
join "Order" o on c.idc = o.idc
join OrderItem oi on oi.ido = o.ido
join  Product p on p.idp = oi.idp
where 
o.order_datetime between '2024-12-01' and '2024-12-31' and
oi.unit_price between 10000 and 11000 and
p.producer = 'Shimano' option (maxdop 1);

/*iteration 0
   |--Stream Aggregate(GROUP BY:([c].[residence]))
       |--Sort(ORDER BY:([c].[residence] ASC))
            |--Nested Loops(Inner Join, OUTER REFERENCES:([Bmk1000]))
                 |--Nested Loops(Inner Join, OUTER REFERENCES:([o].[idc]))
                 |    |--Hash Match(Inner Join, HASH:([oi].[idp])=([p].[idp])DEFINE:([Opt_Bitmap1010]))
                 |    |    |--Hash Match(Inner Join, HASH:([o].[ido])=([oi].[ido])DEFINE:([Opt_Bitmap1008]))
                 |    |    |    |--Table Scan(OBJECT:([GUN0051].[dbo].[Order] AS [o]),  WHERE:([GUN0051].[dbo].[Order].[order_datetime] as [o].[order_datetime]>='2024-12-01' AND [GUN0051].[dbo].[Order].[order_datetime] as [o].[order_datetime]<='2024-12-31'))
                 |    |    |    |--Table Scan(OBJECT:([GUN0051].[dbo].[OrderItem] AS [oi]),  WHERE:([GUN0051].[dbo].[OrderItem].[unit_price] as [oi].[unit_price]>=(10000) AND [GUN0051].[dbo].[OrderItem].[unit_price] as [oi].[unit_price]<=(11000) AND PROBE([Opt_Bitmap1008],[GUN0051].[dbo].[OrderItem].[ido] as [oi].[ido])))
                 |    |    |--Table Scan(OBJECT:([GUN0051].[dbo].[Product] AS [p]),  WHERE:([GUN0051].[dbo].[Product].[producer] as [p].[producer]='Shimano' AND PROBE([Opt_Bitmap1010],[GUN0051].[dbo].[Product].[idp] as [p].[idp])))
                 |    |--Index Seek(OBJECT:([GUN0051].[dbo].[Customer].[PK__Customer__DC501A0C3F4C8112] AS [c]), SEEK:([c].[idc]=[GUN0051].[dbo].[Order].[idc] as [o].[idc]) ORDERED FORWARD)
                 |--RID Lookup(OBJECT:([GUN0051].[dbo].[Customer] AS [c]), SEEK:([Bmk1000]=[Bmk1000]) LOOKUP ORDERED FORWARD)
                 
Comments:
-- Baseline QEP includes, Table scan (product, order,orderitem ), Index seek & RID lookup on Customer table.
This is inefficient as it includes table scan and RID lookup operations. 
This can be improved by creating appropiate indexes.

CPU time = 453 ms, IO: 507+17922+2179+52 = 20660*/


--Selectivity
SELECT COUNT(*) FROM Product WHERE producer = 'Shimano'; -- 3362/100000 =3.36% (moderate)
SELECT COUNT(*) FROM OrderItem WHERE unit_price BETWEEN 10000 AND 11000; --70609/5000000 =0.0141218 = 1.4% (moderate)
SELECT COUNT(*) FROM "Order" WHERE order_datetime between '2024-12-01' and '2024-12-31'; --2152/501414 = 0.0042918626125318 = 0.4% (highly selective < 1%)

/*The most selective condition is order_datetime, 
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
*/


create index idx_order_date_ids on "Order"(order_datetime, idc, ido);

/*ITERATION 1 
  |--Stream Aggregate(GROUP BY:([c].[residence]))
       |--Sort(ORDER BY:([c].[residence] ASC))
            |--Nested Loops(Inner Join, OUTER REFERENCES:([Bmk1000]))
                 |--Nested Loops(Inner Join, OUTER REFERENCES:([o].[idc]))
                 |    |--Hash Match(Inner Join, HASH:([oi].[idp])=([p].[idp])DEFINE:([Opt_Bitmap1010]))
                 |    |    |--Hash Match(Inner Join, HASH:([o].[ido])=([oi].[ido])DEFINE:([Opt_Bitmap1008]))
                 |    |    |    |--Index Seek(OBJECT:([GUN0051].[dbo].[Order].[idx_order_date_ids] AS [o]), SEEK:([o].[order_datetime] >= '2024-12-01' AND [o].[order_datetime] <= '2024-12-31') ORDERED FORWARD)
                 |    |    |    |--Table Scan(OBJECT:([GUN0051].[dbo].[OrderItem] AS [oi]),  WHERE:([GUN0051].[dbo].[OrderItem].[unit_price] as [oi].[unit_price]>=(10000) AND [GUN0051].[dbo].[OrderItem].[unit_price] as [oi].[unit_price]<=(11000) AND PROBE([Opt_Bitmap1008],[GUN0051].[dbo].[OrderItem].[ido] as [oi].[ido])))
                 |    |    |--Table Scan(OBJECT:([GUN0051].[dbo].[Product] AS [p]),  WHERE:([GUN0051].[dbo].[Product].[producer] as [p].[producer]='Shimano' AND PROBE([Opt_Bitmap1010],[GUN0051].[dbo].[Product].[idp] as [p].[idp])))
                 |    |--Index Seek(OBJECT:([GUN0051].[dbo].[Customer].[PK__Customer__DC501A0C3F4C8112] AS [c]), SEEK:([c].[idc]=[GUN0051].[dbo].[Order].[idc] as [o].[idc]) ORDERED FORWARD)
                 |--RID Lookup(OBJECT:([GUN0051].[dbo].[Customer] AS [c]), SEEK:([Bmk1000]=[Bmk1000]) LOOKUP ORDERED FORWARD)


Comments:
-- Currently the query  on Order table order_datetime = '2025-05-01' is highly selective <1%, table scan on Order table is very inefficient, creating an index 
-- "Order"(order_datetime, idc, ido) eliminates table scan on Order table 
-- the second and third parameters, idc and ido help in efficient join operations (c.idc = o.idc), (oi.ido = o.ido)

CPU time = 422 ms, IO = 507+17922+11+52 = 18,492 , 
*/


create index idx_orderItem_up_ido on OrderItem(unit_price,ido,idp);

/*teration2
  |--Hash Match(Aggregate, HASH:([c].[residence]), RESIDUAL:([GUN0051].[dbo].[Customer].[residence] as [c].[residence] = [GUN0051].[dbo].[Customer].[residence] as [c].[residence]))
       |--Nested Loops(Inner Join, OUTER REFERENCES:([Bmk1000]))
            |--Nested Loops(Inner Join, OUTER REFERENCES:([o].[idc]))
            |    |--Hash Match(Inner Join, HASH:([oi].[idp])=([p].[idp])DEFINE:([Opt_Bitmap1010]))
            |    |    |--Hash Match(Inner Join, HASH:([o].[ido])=([oi].[ido])DEFINE:([Opt_Bitmap1008]))
            |    |    |    |--Index Seek(OBJECT:([GUN0051].[dbo].[Order].[idx_order_date_ids] AS [o]), SEEK:([o].[order_datetime] >= '2024-12-01' AND [o].[order_datetime] <= '2024-12-31') ORDERED FORWARD)
            |    |    |    |--Index Seek(OBJECT:([GUN0051].[dbo].[OrderItem].[idx_orderItem_up_ido] AS [oi]), SEEK:([oi].[unit_price] >= (10000) AND [oi].[unit_price] <= (11000))  WHERE:(PROBE([Opt_Bitmap1008],[GUN0051].[dbo].[OrderItem].[ido] as [oi].[ido])) ORDERED FORWARD)
            |    |    |--Table Scan(OBJECT:([GUN0051].[dbo].[Product] AS [p]),  WHERE:([GUN0051].[dbo].[Product].[producer] as [p].[producer]='Shimano' AND PROBE([Opt_Bitmap1010],[GUN0051].[dbo].[Product].[idp] as [p].[idp])))
            |    |--Index Seek(OBJECT:([GUN0051].[dbo].[Customer].[PK__Customer__DC501A0C3F4C8112] AS [c]), SEEK:([c].[idc]=[GUN0051].[dbo].[Order].[idc] as [o].[idc]) ORDERED FORWARD)
            |--RID Lookup(OBJECT:([GUN0051].[dbo].[Customer] AS [c]), SEEK:([Bmk1000]=[Bmk1000]) LOOKUP ORDERED FORWARD)
            
 Comments:
-- Currently dbms is performing index seek on OrderItem table for unit_price, creating a composite index OrderItem(unit_price, ido,idp) 
-- eliminates Table Scan
-- ido helps in effecient join opertaion with Order table (oi.ido = o.ido), p.idp = oi.idp
-- unit_price helps in matching unit price in index without table access

-- CPU time = 31 ms, IO: 507 + 267+ 11+ 52 =837
*/


create index idx_producer_idp on Product(producer, idp);
/*iteration 3
  |--Hash Match(Aggregate, HASH:([c].[residence]), RESIDUAL:([GUN0051].[dbo].[Customer].[residence] as [c].[residence] = [GUN0051].[dbo].[Customer].[residence] as [c].[residence]))
       |--Nested Loops(Inner Join, OUTER REFERENCES:([Bmk1000]))
            |--Nested Loops(Inner Join, OUTER REFERENCES:([o].[idc]))
            |    |--Hash Match(Inner Join, HASH:([oi].[idp])=([p].[idp])DEFINE:([Opt_Bitmap1010]))
            |    |    |--Hash Match(Inner Join, HASH:([o].[ido])=([oi].[ido])DEFINE:([Opt_Bitmap1008]))
            |    |    |    |--Index Seek(OBJECT:([GUN0051].[dbo].[Order].[idx_order_date_ids] AS [o]), SEEK:([o].[order_datetime] >= '2024-12-01' AND [o].[order_datetime] <= '2024-12-31') ORDERED FORWARD)
            |    |    |    |--Index Seek(OBJECT:([GUN0051].[dbo].[OrderItem].[idx_orderItem_up_ido] AS [oi]), SEEK:([oi].[unit_price] >= (10000) AND [oi].[unit_price] <= (11000))  WHERE:(PROBE([Opt_Bitmap1008],[GUN0051].[dbo].[OrderItem].[ido] as [oi].[ido])) ORDERED FORWARD)
            |    |    |--Index Seek(OBJECT:([GUN0051].[dbo].[Product].[idx_producer_idp] AS [p]), SEEK:([p].[producer]='Shimano')  WHERE:(PROBE([Opt_Bitmap1010],[GUN0051].[dbo].[Product].[idp] as [p].[idp])) ORDERED FORWARD)
            |    |--Index Seek(OBJECT:([GUN0051].[dbo].[Customer].[PK__Customer__DC501A0C3F4C8112] AS [c]), SEEK:([c].[idc]=[GUN0051].[dbo].[Order].[idc] as [o].[idc]) ORDERED FORWARD)
            |--RID Lookup(OBJECT:([GUN0051].[dbo].[Customer] AS [c]), SEEK:([Bmk1000]=[Bmk1000]) LOOKUP ORDERED FORWARD)

Comments:
-- Currently dbms is performing index seek on Product table for producer, creating a composite index OrderItem(producer, idp) 
-- eliminates Table Scan
-- idp helps in effecient join opertaion with Orderitem table (p.idp = oi.idp)
-- unit_price helps in matching unit price in index without table access


    CPU time = 16 ms, IO = 16+267+11+52 = 346


*/


drop index idx_producer_idp on Product;

drop index idx_order_date_ids on "Order";

drop index idx_orderItem_up_ido on OrderItem;