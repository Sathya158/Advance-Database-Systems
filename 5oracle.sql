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

select * from vsql_user;

-----------------------

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

select count (*) from Customer;


col fname format a15;
col lname format a15;
col residence format a15;

set feedback on SQL_ID;

select * from Customer
where fname = 'Jana' and lname='PokornĂˇ' and residence = 'Praha';

set feedback off SQL_ID;  --du7fk4756x6g2

explain plan for select * from Customer
where fname = 'Jana' and lname='Nováková' and residence = 'Jihlava';

select * from table(dbms_xplan.display);  --2844954298

exec PrintQueryStat('du7fk4756x6g2', 2844954298);


-- Task 4: Queries (continuation 0f 3)
SELECT lname, fname, residence, COUNT(*) AS cnt
FROM Customer
GROUP BY lname, fname, residence
ORDER BY cnt DESC;

SELECT 
    MIN(cnt) AS min_count,
    MAX(cnt) AS max_count
FROM (
    SELECT COUNT(*) AS cnt
    FROM Customer
    GROUP BY lname, fname, residence
);


--4  minimum number of result records
SELECT lname, fname, residence, COUNT(*) AS cnt
FROM Customer
GROUP BY lname, fname, residence
ORDER BY cnt ASC
FETCH FIRST 1 ROW ONLY;

--5
explain plan for SELECT /*+ NO_PARALLEL(c) */ *
FROM Customer c
WHERE lname = 'Pokorná'
  AND fname = 'Jana'
  AND residence = 'Berlin';  --5am1hcmqfsch3, 2844954298
exec PrintQueryStat('5am1hcmqfsch3', 2844954298);

--6 
CREATE INDEX idx_customer_ln_fn_rs
ON Customer(lname, fname, residence);

SET TIMING ON;
CREATE INDEX idx_customer_ln_fn_rs
ON Customer(lname, fname, residence);
SET TIMING OFF;  --index creation time

SELECT COUNT(*) AS index_entries
FROM Customer;--index entries

SELECT
    leaf_blocks,
    DISTINCT_KEYS,
    BLEVEL
FROM user_indexes
WHERE index_name = 'IDX_CUSTOMER_LN_FN_RS';  --index blocks


--7  maximum number of result records.
SELECT lname, fname, residence, COUNT(*) AS cnt
FROM Customer
GROUP BY lname, fname, residence
ORDER BY cnt DESC
FETCH FIRST 1 ROW ONLY;

SET FEEDBACK ON SQL_ID;
SELECT /*+ NO_PARALLEL(c) */ *
FROM Customer c
WHERE lname = 'Novák'
  AND fname = 'Jan'
  AND residence = 'Praha';
SET FEEDBACK OFF SQL_ID;  --0upy4stmzcf96

EXPLAIN PLAN FOR
SELECT /*+ NO_PARALLEL(c) */ *
FROM Customer c
WHERE lname = 'Novák'
  AND fname = 'Jan'
  AND residence = 'Praha';

SELECT * FROM TABLE(dbms_xplan.display);  --169474562

 --11
 SELECT 
    segment_name,
    bytes/1024/1024 AS size_mb
FROM user_segments
WHERE segment_name = 'CUSTOMER';   --Size of the Customer heap (table)

SELECT 
    segment_name AS index_name,
    bytes/1024/1024 AS size_mb
FROM user_segments
WHERE segment_name LIKE 'IDX_CUSTOMER%';--Size of all indexes on Customer

SELECT 
    SUM(bytes)/1024/1024 AS total_index_size_mb
FROM user_segments
WHERE segment_name LIKE 'IDX_CUSTOMER%';  --Total index size

SELECT 
    (SELECT bytes/1024/1024 
     FROM user_segments 
     WHERE segment_name = 'CUSTOMER') AS heap_mb,
    (SELECT SUM(bytes)/1024/1024 
     FROM user_segments 
     WHERE segment_name LIKE 'IDX_CUSTOMER%') AS index_mb
FROM dual;


