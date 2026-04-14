col sql_text format a30;

-- Task for a sys user
create or replace view vsql_user AS
  select 
    sql_id, plan_hash_value, 
    sum(executions) as executions, 
    round(sum(buffer_gets)/sum(executions),0) as buffer_gets_per_exec,
    round(sum(cpu_time)/sum(executions),0) as cpu_time_per_exec, 
    round(sum(elapsed_time)/sum(executions),0) as elapsed_time_per_exec,
    round(sum(rows_processed)/sum(executions),0) as rows_processed_per_exec,
    round(sum(elapsed_time)/1000,0) as total_elapsed_time_ms,
    substr(max(sql_text),1,1000) sql_text
  from v$sql
  where parsing_schema_name = sys_context('USERENV','SESSION_USER')
  group by sql_id, plan_hash_value
  having sum(executions) <> 0;

grant select on vsql_user to public;

create public synonym vsql_user for SYS.vsql_user;

-----------------------

select * from vsql_user; --sqlid 0ugvq9u7m3wja


create or replace procedure PrintQueryStat(p_sql_id varchar2, p_plan_hash_value int)
as
begin
  -- report the statistics of the query processing
  for rec in (
    select * from vsql_user  
    where sql_id=p_sql_id and plan_hash_value=p_plan_hash_value
  )
  loop
    dbms_output.put_line('---- Query Processing Statistics ----');
    dbms_output.put_line('executions:               ' || rec.executions);
    dbms_output.put_line('rows_processed_per_exec:  ' || rec.rows_processed_per_exec);
    dbms_output.put_line('buffer_gets_per_exec:     ' || rec.buffer_gets_per_exec);
    dbms_output.put_line('cpu_time_per_exec:        ' || rec.cpu_time_per_exec);
    dbms_output.put_line('cpu_time_per_exec_ms:     ' || round(rec.cpu_time_per_exec/1000, 0));
    dbms_output.put_line('elapsed_time_per_exec:    ' || rec.elapsed_time_per_exec);
    dbms_output.put_line('elapsed_time_per_exec_ms: ' || round(rec.elapsed_time_per_exec/1000, 0));
    dbms_output.put_line('total_elapsed_time_ms:    ' || rec.total_elapsed_time_ms);
    dbms_output.put_line('sql_text: ' || rec.sql_text);
  end loop;
end;

EXPLAIN PLAN FOR select count(*) cnt, count(distinct o.idc) cust
from "Order" o
join OrderItem oi on o.ido=oi.ido
join Product p on oi.idp=p.idp
where oi.unit_price between 10000 and 10050 and 
  p.unit_price between 10000 and 10050;
  
SELECT 
    o.idc,
    COUNT(*) AS cnt,
    COUNT(DISTINCT o.ido) AS orders_per_customer
FROM "Order" o
JOIN OrderItem oi ON o.ido = oi.ido
JOIN Product p ON oi.idp = p.idp
WHERE oi.unit_price BETWEEN 10000 AND 10050
  AND p.unit_price BETWEEN 10000 AND 10050
GROUP BY o.idc
ORDER BY cnt DESC;


SELECT 
    o.ido,
    COUNT(*) AS items_in_order,
    SUM(oi.unit_price * oi.quantity) AS order_total
FROM "Order" o
JOIN OrderItem oi ON o.ido = oi.ido
JOIN Product p ON oi.idp = p.idp
WHERE oi.unit_price BETWEEN 10000 AND 10050
  AND p.unit_price BETWEEN 10000 AND 10050
GROUP BY o.ido
ORDER BY order_total DESC;

SELECT 
    p.idp,
    COUNT(*) AS times_sold,
    SUM(oi.quantity) AS total_quantity
FROM "Order" o
JOIN OrderItem oi ON o.ido = oi.ido
JOIN Product p ON oi.idp = p.idp
WHERE oi.unit_price BETWEEN 10000 AND 10050
  AND p.unit_price BETWEEN 10000 AND 10050
GROUP BY p.idp
ORDER BY total_quantity DESC;

SELECT 
    EXTRACT(YEAR FROM o.order_datetime) AS order_year,
    p.idp,
    p.name,
    COUNT(*) AS times_sold,
    SUM(oi.quantity) AS total_quantity
FROM "Order" o
JOIN OrderItem oi ON o.ido = oi.ido
JOIN Product p ON oi.idp = p.idp
WHERE oi.unit_price BETWEEN 10000 AND 10050
  AND p.unit_price BETWEEN 10000 AND 10050
GROUP BY 
    EXTRACT(YEAR FROM o.order_datetime),
    p.idp,
    p.name
ORDER BY 
    order_year DESC,
    total_quantity DESC;

  
--clustering
CREATE TABLE OrderItem_ct CLUSTERING BY LINEAR ORDER (unit_price) AS
SELECT * FROM OrderItem;
DROP TABLE OrderItem_ct;


  
SELECT * FROM TABLE(dbms_xplan.display);  --3776728256, 1883249722, 3519894320
--Plan hash value: 3519894320

set feedback on SQL_ID;
select count(*) cnt, count(distinct o.idc) cust
from "Order" o
join OrderItem oi on o.ido=oi.ido
join Product p on oi.idp=p.idp
where oi.unit_price between 10000 and 10050 and 
  p.unit_price between 10000 and 10050;
set feedback off SQL_ID;   --SQL_ID: gbqj4kn8wjjc3

exec PrintQueryStat('gbqj4kn8wjjc3', 3519894320);

--before index
--
--Query Processing Statistics ----
--executions:               1
--rows_processed_per_exec:  1
--buffer_gets_per_exec:     16716
--cpu_time_per_exec:        230932
--cpu_time_per_exec_ms:     231
--elapsed_time_per_exec:    245121
--elapsed_time_per_exec_ms: 245
--total_elapsed_time_ms:    245
  
-- index
CREATE INDEX idx_oi_price ON OrderItem(unit_price);
CREATE CLUSTERED INDEX cidx_orderitem_unitprice
ON OrderItem(unit_price);

DROP INDEX idx_oi_price ON OrderItem;

--composite index
CREATE INDEX idx_oi_price_ido
ON OrderItem(unit_price, ido);


--after index creation 

----- Query Processing Statistics ----
--executions:               2
--rows_processed_per_exec:  1
--buffer_gets_per_exec:     4499
--cpu_time_per_exec:        30691
--cpu_time_per_exec_ms:     31
--elapsed_time_per_exec:    25028
--elapsed_time_per_exec_ms: 25
--total_elapsed_time_ms:    50


DROP INDEX IDX_P_PRICE;

select * from OrderItem;

select index_name from user_indexes
where table_name = 'ORDERITEM';

select index_name from user_indexes
where table_name = 'PRODUCT';


CREATE INDEX idx_p_price ON Product(unit_price);

DROP INDEX idx_oi_price;


SELECT index_name, table_name
FROM user_indexes
WHERE table_name = 'PRODUCT';



--Find the total number of orders and the average order value
SELECT COUNT(*) AS total_orders,
       AVG(total_amount) AS avg_order_value
FROM "Order";

SELECT 
    COUNT(*) AS total_orders,
    AVG(order_total) AS avg_order_value
FROM (
    SELECT o.ido,
           SUM(oi.unit_price * oi.quantity) AS order_total
    FROM "Order" o
    JOIN ORDERITEM oi ON o.ido = oi.ido
    GROUP BY o.ido
);


 --1 Count how many products exist in the PRODUCT table. = total number of products
SELECT COUNT(*) AS total_products
FROM PRODUCT;

--2 Count how many unique customers have placed orders. = number of distinct Order.idc (DISTINCT Aggregate)
SELECT COUNT(DISTINCT idc) AS unique_customers
FROM "Order";

--3 Find how many order items belong to each order. = order ID, number of items in that order (JOIN + Aggregate)
SELECT  oi.ido,COUNT(*) AS items_in_order
FROM ORDERITEM oi
GROUP BY oi.ido;


--4 Count how many order items have quantity greater than 2. = total count (JOIN + WHERE + Aggregate)
SELECT COUNT(*) AS total_items
FROM ORDERITEM
WHERE quantity > 2;


--5 Count how many products have a unit_price between 5000 and 7000. = total count (BETWEEN + Aggregate)
SELECT COUNT(*) AS products_in_range
FROM PRODUCT
WHERE unit_price BETWEEN 5000 AND 7000;


--6 Count how many order items have:1)OrderItem.unit_price between 10000 and 10030
--2)Product.unit_price between 10000 and 10030 = total records, number of unique customers(JOIN + BETWEEN + Aggregate)
SELECT 
    COUNT(*) AS total_records,
    COUNT(DISTINCT o.idc) AS unique_customers
FROM ORDERITEM oi
JOIN PRODUCT p ON oi.idp = p.idp
JOIN "Order" o ON oi.ido = o.ido
WHERE oi.unit_price BETWEEN 10000 AND 10030
  AND p.unit_price BETWEEN 10000 AND 10030;


--7 Find customers who ordered more than 5 items total. = customer ID, total items ordered(GROUP BY + HAVING)
SELECT o.idc, COUNT(*) AS total_items
FROM ORDERITEM oi
JOIN "Order" o ON oi.ido = o.ido
GROUP BY o.idc
HAVING COUNT(*) > 5;


--8 Find the total spending of each customer.(Assume OrderItem.unit_price × quantity = spending)= customer ID, total spending(JOIN + SUM + GROUP BY)
SELECT o.idc,
       ROUND(SUM(oi.unit_price * oi.quantity) / 1000000, 2) AS spending_millions
FROM ORDERITEM oi
JOIN "Order" o ON oi.ido = o.ido
GROUP BY o.idc;

SELECT o.idc,
       TO_CHAR(SUM(oi.unit_price * oi.quantity), '999,999,999,999,999') AS total_spending
FROM GUN0051.ORDERITEM oi
JOIN GUN0051."Order" o ON oi.ido = o.ido
GROUP BY o.idc;

--9 Find customers whose total spending is above the average spending of all customers. = customer ID, total spending(Subquery + Aggregate)
SELECT idc, total_spending
FROM (
    SELECT o.idc,
           SUM(oi.unit_price * oi.quantity) AS total_spending
    FROM ORDERITEM oi
    JOIN "Order" o ON oi.ido = o.ido
    GROUP BY o.idc
)
WHERE total_spending > (
    SELECT AVG(SUM(oi.unit_price * oi.quantity))
    FROM ORDERITEM oi
    JOIN "Order" o ON oi.ido = o.ido
    GROUP BY o.idc
);


/*10 List each product and show:

total number of times it was ordered

total quantity ordered

number of unique customers who ordered it = product ID,total order count, total quantity, unique customers */

SELECT 
    p.idp,
    COUNT(*) AS total_orders,
    SUM(oi.quantity) AS total_quantity,
    COUNT(DISTINCT o.idc) AS unique_customers
FROM ORDERITEM oi
JOIN PRODUCT p ON oi.idp = p.idp
JOIN "Order" o ON oi.ido = o.ido
GROUP BY p.idp;


SELECT
    -- ORDER attributes
    o.ido,
    o.order_datetime,
    o.idc,
    o.order_status,
    o.idso,
    o.idsa,

    -- ORDERITEM attributes
    oi.idp,
    oi.unit_price AS item_unit_price,
    oi.quantity,

    -- PRODUCT attributes
    p.name AS product_name,
    p.unit_price AS product_unit_price,
    p.producer,
    p.description

FROM "Order" o
JOIN OrderItem oi ON o.ido = oi.ido
JOIN Product p   ON oi.idp = p.idp

WHERE
    -- Example filters (you can change these)
    o.order_datetime BETWEEN DATE '2023-01-01' AND DATE '2023-12-31'
    AND oi.unit_price BETWEEN 10000 AND 20000
    
    
    
SELECT 
    EXTRACT(YEAR FROM o.order_datetime) AS order_year,
    EXTRACT(MONTH FROM o.order_datetime) AS order_month,
    p.idp,
    p.name AS product_name,
    s.idso,
    s.name AS store_name,
    st.idsa,
    st.fname AS staff_fname,
    st.lname AS staff_lname,
    c.idc,
    c.fname AS cust_fname,
    c.lname AS cust_lname,
    COUNT(*) AS times_sold,
    SUM(oi.quantity) AS total_quantity
FROM "Order" o
JOIN OrderItem oi ON o.ido = oi.ido
JOIN Product p ON oi.idp = p.idp
JOIN Store s ON o.idso = s.idso
JOIN Staff st ON o.idsa = st.idsa
JOIN Customer c ON o.idc = c.idc
WHERE oi.unit_price BETWEEN 10000 AND 10050
  AND p.unit_price BETWEEN 10000 AND 10050
GROUP BY 
    EXTRACT(YEAR FROM o.order_datetime),
    EXTRACT(MONTH FROM o.order_datetime),
    p.idp,
    p.name,
    s.idso,
    s.name,
    st.idsa,
    st.fname,
    st.lname,
    c.idc,
    c.fname,
    c.lname
ORDER BY 
    order_year DESC,
    order_month DESC,
    total_quantity DESC;

    
    
-- top 10
-- detail: Top 10 most recent orders
SELECT *
FROM (
    SELECT o.*
    FROM "Order" o
    ORDER BY o.order_datetime DESC
)
WHERE ROWNUM <= 10;
--OR
SELECT o.*
    FROM "Order" o
    ORDER BY o.order_datetime DESC
FETCH FIRST 10 ROWS ONLY;

--Top 10 customers by total spending
SELECT *
FROM (
    SELECT 
        o.idc,
        SUM(oi.unit_price * oi.quantity) AS total_spending
    FROM "Order" o
    JOIN OrderItem oi ON o.ido = oi.ido
    GROUP BY o.idc
    ORDER BY total_spending DESC
)
WHERE ROWNUM <= 10;

--
SELECT 
    o.idc,
    SUM(oi.unit_price * oi.quantity) AS total_spending
FROM "Order" o
JOIN OrderItem oi ON o.ido = oi.ido
GROUP BY o.idc
ORDER BY total_spending DESC
FETCH FIRST 10 ROWS ONLY;







