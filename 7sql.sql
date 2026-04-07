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


select * from  "OrderItem" o join 
 Product p on o.idp = p.idp 
 where p.unit_price between 20000000 and 20002000
 option (maxdop 1);

 -- Iteration 1:
 --17922, 626
  --Hash Match(Inner join...)
 --Table Scan (Product)
 --Table Scan (OrterItem);

 -- Iteration 2:
 create index idx_product_up on Product(unit_price); 
 --cpu = 78

 drop index idx_product_up on Product;

 SELECT 
    COUNT(*) AS total_order_items,
    SUM(oi.quantity) AS total_sold_units
FROM "OrderItem" oi
JOIN Product P ON oi.idp = P.idp
WHERE P.unit_price BETWEEN 20000000 AND 20002000;


create index idx_orderitem on OrderItem(idp) include (quantity);



-- 
SELECT 
    oi.*, p.*, o.*
FROM OrderItem oi
JOIN Product p ON oi.idp = p.idp
JOIN [Order] o ON oi.ido = o.ido
WHERE p.name LIKE 'Car%'
  AND o.order_datetime BETWEEN '2022-01-01' AND '2022-12-31'
  AND oi.unit_price BETWEEN 1000000 AND 1010000;


 SELECT 
    COUNT(*) AS orderitem_count,
    SUM(oi.quantity) AS total_units
FROM OrderItem oi
JOIN Product p ON oi.idp = p.idp
JOIN [Order] o ON oi.ido = o.ido
WHERE p.name LIKE 'Car%'
  AND o.order_datetime BETWEEN '2022-01-01' AND '2022-12-31'
  AND oi.unit_price BETWEEN 1000000 AND 1010000;




--ruery to tune
SELECT COUNT(*) 
FROM (
  -- paste Query 3 here
) t;

--CREATE INDEX idx_product_name ON Product(name);
--CREATE INDEX idx_order_datetime ON [Order](order_datetime);
--CREATE INDEX idx_orderitem ON OrderItem(idp, ido, unit_price);

--SELECT * INTO Order_ct FROM [Order];
--GO
--CREATE CLUSTERED INDEX cidx_Order_ct ON Order_ct(order_datetime);
--GO


--SELECT * INTO OrderItem_ctFROM OrderItem;
--GO
--CREATE CLUSTERED INDEX cidx_OrderItem_ct ON OrderItem_ct(unit_price);
--GO


--after this (Product -> Product_ct)repeat 3 and 4

DROP INDEX idx_product_name ON Product;
DROP INDEX idx_order_datetime ON [Order];
DROP INDEX idx_orderitem ON OrderItem;

DROP TABLE Product_ct;
DROP TABLE Order_ct;
DROP TABLE OrderItem_ct;