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


--Task 7: Join Operations 3
  
EXPLAIN PLAN FOR
SELECT *
FROM ORDERITEM oi
JOIN PRODUCT p ON oi.IDP = p.IDP
JOIN "Order" o ON oi.IDO = o.IDO
WHERE p.NAME LIKE 'Car%'
  AND o.ORDER_DATETIME BETWEEN DATE '2022-01-01' AND DATE '2022-12-31'
  AND oi.UNIT_PRICE BETWEEN 1000000 AND 1010000;

SELECT * FROM TABLE(dbms_xplan.display);  -- 1742163594

set feedback on SQL_ID;
SELECT *
FROM ORDERITEM oi
JOIN PRODUCT p ON oi.IDP = p.IDP
JOIN "Order" o ON oi.IDO = o.IDO
WHERE p.NAME LIKE 'Car%'
  AND o.ORDER_DATETIME BETWEEN DATE '2022-01-01' AND DATE '2022-12-31'
  AND oi.UNIT_PRICE BETWEEN 1000000 AND 1010000;
set feedback off SQL_ID;  --SQL_ID: 1j2p0xwc87f86


SELECT sql_id, plan_hash_value, executions,
       cpu_time_per_exec, elapsed_time_per_exec,
       buffer_gets_per_exec
FROM vsql_user
WHERE sql_text LIKE '%ORDERITEM%';

exec PrintQueryStat('1j2p0xwc87f86', 2548601169);

SET TIMING ON;

SELECT *
FROM ORDERITEM oi
JOIN PRODUCT p ON oi.IDP = p.IDP
JOIN "Order" o ON oi.IDO = o.IDO
WHERE p.NAME LIKE 'Car%'
  AND o.ORDER_DATETIME BETWEEN DATE '2022-01-01' AND DATE '2022-12-31'
  AND oi.UNIT_PRICE BETWEEN 1000000 AND 1010000;

SET TIMING OFF;

--4 Aggregate
SELECT
  COUNT(*)         AS orderitem_count,
  SUM(oi.QUANTITY) AS total_units
FROM ORDERITEM oi
JOIN PRODUCT p ON oi.IDP = p.IDP
JOIN "Order" o ON oi.IDO = o.IDO
WHERE p.NAME LIKE 'Car%'
  AND o.ORDER_DATETIME BETWEEN DATE '2022-01-01' AND DATE '2022-12-31'
  AND oi.UNIT_PRICE BETWEEN 1000000 AND 1010000;

--
CREATE INDEX idx_product_name ON PRODUCT(NAME);

CREATE INDEX idx_order_datetime ON "Order"(ORDER_DATETIME);

CREATE INDEX idx_orderitem_idp_ido_price
ON ORDERITEM(IDP, IDO, UNIT_PRICE);


--SELECT table_name 
--FROM user_tables
--ORDER BY table_name;

--SELECT column_name FROM user_tab_columns WHERE table_name = 'PRODUCT';

--CREATE TABLE PRODUCT_CT AS SELECT * FROM PRODUCT ORDER BY NAME;
--CREATE TABLE ORDER_CT AS SELECT * FROM "Order" ORDER BY ORDER_DATETIME;
--CREATE TABLE ORDERITEM_CT ASSELECT * FROM ORDERITEMORDER BY UNIT_PRICE;
----------------------------------------------------------------------------------
-- CLEANUP: DROP INDEXES AND CLUSTERED TABLES
--------------------------------------------------------------------------------

--DROP INDEX idx_product_name;
---DROP INDEX idx_order_datetime;
--DROP INDEX idx_orderitem_idp_ido_price;

--DROP TABLE PRODUCT_CT;
--DROP TABLE ORDER_CT;
--DROP TABLE ORDERITEM_CT;



SELECT sql_id, plan_hash_value, executions,
       cpu_time_per_exec, elapsed_time_per_exec,
       buffer_gets_per_exec
FROM vsql_user
WHERE sql_text LIKE '%ORDERITEM%';

SELECT * FROM vsql_user WHERE sql_text LIKE '%ORDERITEM%';


