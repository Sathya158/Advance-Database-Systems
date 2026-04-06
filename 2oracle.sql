select blocks from user_segments 
  where segment_name = 'CUSTOMER';  --1792
  
-- diff: CUSTOMER = table segment -> stores rows (needs more blocks)
--SYS_C0083163 = index segment created ->  index stores sorted key values ( needs fewer block)

select blocks from user_segments 
  where segment_name = 'SYS_C0083294';  --1152  automatically created constraint index, PRIMARY KEY, UNIQUE constraint
   
  
select index_name from user_indexes  
where table_name='CUSTOMER';  -- SYS_C0083294
select index_name from user_indexes 
where table_name='ORDERITEM';

select blocks, bytes/1024/1024 as MB from user_segments
where segment_name = 'SYS_C0083294';
 
--Page Utilizatio
exec PrintPages_unused_space('SYS_C0083294', 'GUN0051', 'INDEX');
exec PrintPages_space_usage('SYS_C0083294', 'GUN0051', 'INDEX');



--Task 2.2: Dropping the Primary Key Index
DROP INDEX PK_ORDERITEM;


ALTER TABLE OrderItem DROP PRIMARY KEY;

SELECT index_name
FROM user_indexes
WHERE table_name = 'ORDERITEM';

ALTER TABLE OrderItem
ADD CONSTRAINT PK_ORDERITEM PRIMARY KEY (IDO, IDP);

SELECT index_name, uniqueness
FROM user_indexes
WHERE table_name = 'ORDERITEM';


-- Task 2.3: B-tree
col index_name for a15

select index_name, blevel, leaf_blocks
from user_indexes where table_name='CUSTOMER';
select index_name, blevel, leaf_blocks
from user_indexes where table_name='ORDERITEM';

--Task 2.4: Index Size Optimization(A-> rebild ->A)
ANALYZE INDEX SYS_C0083294 VALIDATE STRUCTURE;

select height-1 as h, blocks, lf_blks as leaf_pages, 
br_blks as inner_pages, lf_rows as leaf_items,
br_rows as inner_items, pct_used
from index_stats where name='SYS_C0083294';

ALTER INDEX SYS_C0083294 REBUILD;



