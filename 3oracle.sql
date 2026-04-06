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


--Task 3.1: Query plans and Execution statistics
explain plan for select * from OrderItem where orderitem.unit_price between 1 and 300; 
select * from table(dbms_xplan.display);  --Plan hash value: 892791741

set feedback on SQL_ID;
select * from OrderItem where orderitem.unit_price between 1 and 300; 
set feedback off SQL_ID;  --SQL_ID: 56xntf3s3yzbq

exec PrintQueryStat('56xntf3s3yzbq', 892791741 );

--Task 3.2: Disabling / Forcing Parallel Query Execution(have to repeat the task1)
ALTER SYSTEM SET parallel_degree_policy = AUTO;

select degree
from user_tables
where table_name='ORDERITEM';

alter table OrderItem parallel (degree 4);


ALTER TABLE OrderItem PARALLEL 4;
-- ALTER TABLE OrderItem NOPARALLEL; (SEQUENTIAL)

select * from table(dbms_xplan.display);

--Task 3.3: Physical Deletion and Sequential Heap Scan
DELETE FROM OrderItem
WHERE MOD(IDO, 4) = 0;

COMMIT;   --- have to run task 1

--physical deleteion
--select count (*) from Customer;

--exec printpages_space_usage('CUSTOMER', 'KRA28', 'TABLE');

alter table OrderItem enable row movement;
alter table OrderItem shrink space;
