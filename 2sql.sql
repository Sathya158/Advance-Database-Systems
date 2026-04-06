create or alter procedure PrintIndexes 
  @table VARCHAR(30)
as
  select i.name as indexName
  from sys.indexes i
  inner join sys.tables t on t.object_id = i.object_id
  where T.Name = @table and i.name is not null;
go

create or alter procedure PrintPagesIndex 
  @index varchar(30)
as
  select 
    i.name as IndexName,
    p.rows as ItemCounts,
    sum(a.total_pages) as TotalPages, 
    round(cast(sum(a.total_pages) * 8 as float) / 1024, 1) 
      as TotalPages_MB, 
    sum(a.used_pages) as UsedPages,
    round(cast(sum(a.used_pages) * 8 as float) / 1024, 1) 
      as UsedPages_MB
  from sys.indexes i
  inner join sys.partitions p 
    on i.object_id = p.OBJECT_ID and i.index_id = p.index_id
  inner join sys.allocation_units a 
    on p.partition_id = a.container_id
  where i.name = @index
  group by i.name, p.Rows
  order by i.name
go


exec PrintIndexes 'Customer';
exec PrintIndexes 'OrderItem';
exec PrintPagesHeap 'Customer';

exec PrintPagesIndex 'PK__Customer__DC501A0C0334CF9F';
exec PrintPagesIndex 'pk_orderitem';

--Task 2.2: Dropping the Primary Key Index
drop index pk_orderitem on OrderItem; -- if it does not work need to work on the below

SELECT name
FROM sys.key_constraints
WHERE parent_object_id = OBJECT_ID('OrderItem')
  AND type = 'PK'; --Find the PK constraint name

ALTER TABLE OrderItem
DROP CONSTRAINT PK_OrderItem;  --automatically drops the PK index

SELECT name
FROM sys.indexes
WHERE object_id = OBJECT_ID('OrderItem'); --to verify the index is existed

ALTER TABLE OrderItem
ADD CONSTRAINT PK_OrderItem PRIMARY KEY (IDO, IDP);

SELECT name, type_desc
FROM sys.indexes
WHERE object_id = OBJECT_ID('OrderItem');




create or alter procedure PrintIndexStats @user varchar(30), @table varchar(30), @index varchar(30)
as
    select i.name, s.index_depth - 1 as height, 
      sum(s.page_count) as page_count 
    from sys.dm_db_index_physical_stats(DB_ID(@user),
      OBJECT_ID(@table), NULL, NULL , 'DETAILED') s
    join sys.indexes i 
      on s.object_id=i.object_id and s.index_id=i.index_id
    where name=@index
    group by i.name, s.index_depth
go

create or alter procedure PrintIndexLevelStats @user varchar(30), @table varchar(30), @index varchar(30)
as
    select s.index_level as level, s.page_count, 
      s.record_count, s.avg_record_size_in_bytes 
        as avg_record_size,
      round(s.avg_page_space_used_in_percent,1) 
        as page_utilization, 
      round(s.avg_fragmentation_in_percent,2) as avg_frag
    from sys.dm_db_index_physical_stats(DB_ID(@user), 
      OBJECT_ID(@table), NULL, NULL , 'DETAILED') s
    join sys.indexes i 
      on s.object_id=i.object_id and s.index_id=i.index_id
    where name=@index
go


--Task 2.3: B-tree
exec PrintIndexStats 'GUN0051', 'Customer', 'PK__Customer__DC501A0C0334CF9F'
exec PrintIndexLevelStats 'GUN0051', 'Customer', 'PK__Customer__DC501A0C0334CF9F'


--Task 2.4: Index Size Optimization(2.3 -> 2.4 -> 2.3)
ALTER INDEX PK__Customer__DC501A0C0334CF9F
ON Customer
REBUILD WITH (FILLFACTOR = 100);
--or
ALTER INDEX PK__Customer__DC501A0C0334CF9F
ON Customer
REORGANIZE;
