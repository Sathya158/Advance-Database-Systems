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


EXPLAIN PLAN FOR
SELECT COUNT(*)
FROM OrderItem oi
JOIN "Order"  o ON oi.ido = o.ido
JOIN Customer c ON o.idc = c.idc
WHERE c.residence = 'Berlin'
  AND o.order_datetime = TO_DATE('01.05.2025', 'DD.MM.YYYY')
  AND oi.unit_price BETWEEN 100000 AND 200000;
  
SELECT * FROM TABLE(dbms_xplan.display); 

set feedback on SQL_ID;
SELECT COUNT(*)
FROM OrderItem oi
JOIN "Order"  o ON oi.ido = o.ido
JOIN Customer c ON o.idc = c.idc
WHERE c.residence = 'Berlin'
  AND o.order_datetime = TO_DATE('01.05.2025', 'DD.MM.YYYY')
  AND oi.unit_price BETWEEN 100000 AND 200000;
set feedback off SQL_ID;

exec PrintQueryStat('4kqqv9amgcwtn', 2876827149);

SELECT COUNT(*) FROM Customer WHERE residence='Berlin';   
SELECT COUNT(*) FROM "Order" WHERE order_datetime = DATE '2025-05-01';
SELECT COUNT(*) FROM OrderItem WHERE unit_price BETWEEN 100000 AND 200000;
/*selectivity =  (matching rows) / (total rows)
I computed selectivity for each filter separately.

residence='Berlin': 15,731 / 300,000 = 5.24%

order_datetime=1 day: 88 / 499,894 = 0.0176%

unit_price range: 230,802 / 5,000,005 = 4.61%

Index design based on selectivity:

The most selective predicate is order_datetime, so the correct index is "Order"(order_datetime, idc).

The next selective predicate is unit_price, so the index is OrderItem(unit_price, ido).

The least selective predicate is residence, so Customer(residence, idc) has the smallest impact.

Based on the row counts, the most selective predicate is the order date filter (88 out of 499,894 rows → 0.0176%), 
so the primary index should be created on Order(idc, order_datetime). 

The next most selective predicate is the unit_price range on OrderItem (230,802 out of 5,000,005 rows → 4.61%), 
so an index on OrderItem(unit_price, ido) is appropriate. The least selective predicate is residence='Berlin' (15,731 out of 300,000 rows → 5.24%), 

so the Customer index (residence, idc) is helpful but has the smallest impact. 
These selectivity values explain the recommended index order and why the optimizer may or may not use them.
*/


/*Iteration 1: Plan hash value: 2876827149
|   0 | SELECT STATEMENT                |              |     1 |    86 |  1076   (1)| 00:00:01 |
|   1 |  SORT AGGREGATE                 |              |     1 |    86 |            |          |
|   2 |   NESTED LOOPS                  |              |  4082 |   342K|  1076   (1)| 00:00:01 |
|   3 |    NESTED LOOPS                 |              | 90858 |   342K|  1076   (1)| 00:00:01 |
|   4 |     NESTED LOOPS                |              |    38 |  2280 |   696   (2)| 00:00:01 |
|*  5 |      TABLE ACCESS FULL          | Order        |    38 |  1330 |   658   (2)| 00:00:01 

    6 |      TABLE ACCESS BY INDEX ROWID| CUSTOMER     |     1 |    25 |     1   (0)| 00:00:01 |
|*  7 |       INDEX UNIQUE SCAN         | SYS_C0098101 |     1 |       |     0   (0)| 00:00:01 |
|*  8 |     INDEX RANGE SCAN            | PK_ORDERITEM |  2391 |       |    10   (0)| 00:00:01 |
|*  9 |    TABLE ACCESS BY INDEX ROWID  | ORDERITEM    |   108 |  2808 |    10   (0)| 00:00:01 

executions:               1
rows_processed_per_exec:  1
buffer_gets_per_exec:     2678
cpu_time_per_exec:        45866
cpu_time_per_exec_ms:     46
elapsed_time_per_exec:    41149
elapsed_time_per_exec_ms: 41
total_elapsed_time_ms:    41


Oracle performs a full table scan on the Order table and uses primary key indexes on Customer and OrderItem for the joins. 
The full scan on Order (cost 658) is the main source of IO, which is reflected in the high buffer_gets value (2678), 
even though the query returns only one aggregated row. 
This plan serves as the baseline before creating any additional indexes.
*/

DROP INDEX idx_cust_res_idc
CREATE INDEX idx_cust_res_idc
ON Customer(residence, idc);

select index_name
from user_indexes
where table_name = 'CUSTOMER';

/*residence first → used in WHERE
idc second → used in join to Order

Reason:
residence='Berlin' has 5.24% selectivity, not very selective.
Oracle decides a hash join is cheaper than nested loops.
Even though the index exists, Oracle is not forced to use it.

The index on Customer(residence, idc) does not force an index nested loop join because the residence predicate is not selective enough. 
Oracle chooses a hash join instead.*/

/*iteration2
Plan hash value: 2876827149
Id  | Operation                       | Name         | Rows  | Bytes | Cost (%CPU)| Time     |
------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT                |              |     1 |    86 |  1076   (1)| 00:00:01 |
|   1 |  SORT AGGREGATE                 |              |     1 |    86 |            |          |
|   2 |   NESTED LOOPS                  |              |  4082 |   342K|  1076   (1)| 00:00:01 |
|   3 |    NESTED LOOPS                 |              | 90858 |   342K|  1076   (1)| 00:00:01 |
|   4 |     NESTED LOOPS                |              |    38 |  2280 |   696   (2)| 00:00:01 |
|*  5 |      TABLE ACCESS FULL          | Order        |    38 |  1330 |   658   (2)| 00:00:01

*  6 |      TABLE ACCESS BY INDEX ROWID| CUSTOMER     |     1 |    25 |     1   (0)| 00:00:01 |
|*  7 |       INDEX UNIQUE SCAN         | SYS_C0098101 |     1 |       |     0   (0)| 00:00:01 |
|*  8 |     INDEX RANGE SCAN            | PK_ORDERITEM |  2391 |       |    10   (0)| 00:00:01 |
|*  9 |    TABLE ACCESS BY INDEX ROWID  | ORDERITEM    |   108 |  2808 |    10   (0)| 00:00:01 |

*/


CREATE INDEX idx_order_date_idc
ON "Order"(order_datetime, idc); 

/*
Most selective column FIRST = order_datetime is more selective than idc.
(not idc first → join to Customer
order_datetime second → very selective filter)*/

/*ITERATION 2
Plan hash value: 2876827149
 Id  | Operation                       | Name         | Rows  | Bytes | Cost (%CPU)| Time     |
------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT                |              |     1 |    86 |  1076   (1)| 00:00:01 |
|   1 |  SORT AGGREGATE                 |              |     1 |    86 |            |          |
|   2 |   NESTED LOOPS                  |              |  4082 |   342K|  1076   (1)| 00:00:01 |
|   3 |    NESTED LOOPS                 |              | 90858 |   342K|  1076   (1)| 00:00:01 |
|   4 |     NESTED LOOPS                |              |    38 |  2280 |   696   (2)| 00:00:01 |
|*  5 |      TABLE ACCESS FULL          | Order        |    38 |  1330 |   658   (2)| 00:00:01 

PLAN_TABLE_OUTPUT                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
|*  6 |      TABLE ACCESS BY INDEX ROWID| CUSTOMER     |     1 |    25 |     1   (0)| 00:00:01 |
|*  7 |       INDEX UNIQUE SCAN         | SYS_C0098101 |     1 |       |     0   (0)| 00:00:01 |
|*  8 |     INDEX RANGE SCAN            | PK_ORDERITEM |  2391 |       |    10   (0)| 00:00:01 |
|*  9 |    TABLE ACCESS BY INDEX ROWID  | ORDERITEM    |   108 |  2808 |    10   (0)| 00:00:01 |
*/

CREATE INDEX idx_oi_price_ido
ON OrderItem(unit_price, ido);

/*unit_price first → range filter
ido second → join to Order

Reason:
unit_price range returns 4.61% of 5 million rows.
This is not selective enough to drive the join.
Oracle prefers scanning ORDER first.

-Scanning 5 million rows is cheaper than using the index and doing 230k random lookups
-Oracle ignores this index because the unit_price predicate is not selective enough to change the join order. 
Therefore, the QEP remains unchanged.*/

DROP INDEX idx_order_date_idc;


 
 /*Iteration 2: Index "Order"(idc, order_datetime)
  - it is the first created index, it means it is necessary to have the key supporting the selection, i.e. order_datetime as the first attribute.
- Iteration 3: Index Customer(residence, idc)
  - It leads to hash join, not the index nested loop join. Why? When you change the order of attributes in the key, the index nested loop join is not used? You should understand the order of attributes in the key.
- Iteration 4: OrderItem(unit_price, ido)
  - The same notice.
  - Moreover, QEP does not include the created index.
*/