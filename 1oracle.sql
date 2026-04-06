select blocks from user_segments 
  where segment_name = 'CUSTOMER';

select blocks, bytes/1024/1024 as MB from user_segments
 where segment_name = 'CUSTOMER';

select segment_type, sum(blocks) "Total Blocks", sum(bytes/1024/1024) "MB"
from user_segments
where segment_name in ('CUSTOMER', 'STAFF', 'Order', 'ORDERITEM', 'PRODUCT', 'STORE')
group by segment_type;

select table_name,blocks, empty_blocks,pct_free,pct_used from user_tables 
where table_name='CUSTOMER';



DROP PROCEDURE PRINTPAGES_UNUSED_SPACE;

create or replace procedure PrintPages_unused_space(
  p_table_name varchar,  p_user_name varchar, p_type varchar)
as
  blocks           number;
  bytes            number;
  unused_blocks    number;
  unused_bytes     number;
  expired_blocks   number;
  expired_bytes    number;
  unexpired_blocks number;
  unexpired_bytes  number;
  mega number := 1024.0 * 1024.0;
begin 
  dbms_space.unused_space(p_user_name, p_table_name, p_type,
    blocks, bytes, unused_blocks, unused_bytes, expired_blocks,
    expired_bytes, unexpired_blocks, unexpired_bytes);

  dbms_output.put_line('blocks: ' || blocks);
  dbms_output.put_line('size (MB): ' || (bytes / mega));
  dbms_output.put_line('used_blocks: ' || 
    (blocks - unused_blocks));
  dbms_output.put_line('size used (MB): ' || 
    ((bytes / mega) - (unused_bytes / mega)));
  dbms_output.put_line('unused_blocks: ' || unused_blocks);
  dbms_output.put_line('size unused (MB): ' || 
    (unused_bytes / mega));
end;



-- Variant IV
DROP PROCEDURE PRINTPAGES_SPACE_USAGE;
create or replace procedure PrintPages_space_usage(
  p_table_name varchar, p_user_name varchar, p_type varchar)
as
  unformatted_blocks NUMBER;
  unformatted_bytes  NUMBER;
  fs1_blocks         NUMBER;
  fs1_bytes          NUMBER;
  fs2_blocks         NUMBER;
  fs2_bytes          NUMBER;
  fs3_blocks         NUMBER;
  fs3_bytes          NUMBER;
  fs4_blocks         NUMBER;
  fs4_bytes          NUMBER;
  full_blocks        NUMBER;
  full_bytes         NUMBER;
begin
  dbms_space.space_usage(p_user_name, p_table_name, p_type,
    unformatted_blocks, unformatted_bytes, fs1_blocks, fs1_bytes,
    fs2_blocks, fs2_bytes, fs3_blocks, fs3_bytes, fs4_blocks,
    fs4_bytes, full_blocks, full_bytes, null); 

  dbms_output.put_line('unformatted_blocks: ' || 
    unformatted_blocks);
  dbms_output.put_line('fs1_blocks (0 to 25% free space): ' ||
    fs1_blocks);
  dbms_output.put_line('fs2_blocks (25 to 50% free space): ' ||
    fs2_blocks);
  dbms_output.put_line('fs3_blocks (50 to 75% free space): ' ||
    fs3_blocks);
  dbms_output.put_line('fs4_blocks (75 to 100% free space): ' ||
    fs4_blocks);
  dbms_output.put_line('full_blocks: ' || full_blocks);
end;


select blocks, bytes/1024/1024 as MB from user_segments
 where segment_name = 'CUSTOMER';
 
COLUMN table_name FORMAT A20;

select table_name,blocks, empty_blocks,pct_free,pct_used from user_tables 
where table_name='CUSTOMER'; 

exec PrintPages_unused_space('CUSTOMER', 'GUN0051', 'TABLE');
exec PrintPages_space_usage('CUSTOMER', 'GUN0051', 'TABLE');

DELETE FROM OrderItem
WHERE MOD(IDO, 5) = 0;
COMMIT;

alter table OrderItem enable row movement;
alter table OrderItem shrink space;

select count(*) from OrderItem;
exec PrintPages_unused_space('CUSTOMER', 'GUN0051', 'TABLE')

SELECT blocks, bytes/1024/1024 AS mb
FROM user_segments
WHERE segment_name = 'ORDERITEM';

