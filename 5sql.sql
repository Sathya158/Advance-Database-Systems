SET SHOWPLAN_TEXT ON;
SET SHOWPLAN_TEXT OFF;
SET SHOWPLAN_ALL ON;
SET SHOWPLAN_ALL OFF;

SET STATISTICS IO ON;
SET STATISTICS TIME ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;

exec PrintPagesHeap 'Customer';

--Task 4: Queries  (continuation from 3)

select * from Customer
where fname = 'Jana' and lname='Pokorná' and residence = 'Berlin';

SELECT TOP 1 lname, fname, residence, COUNT(*) AS cnt
FROM Customer
GROUP BY lname, fname, residence
ORDER BY cnt ASC;   --Find the (lname, fname, residence) combination with the minimum count

SELECT 
    MIN(cnt) AS min_count,
    MAX(cnt) AS max_count
FROM (
    SELECT COUNT(*) AS cnt
    FROM Customer
    GROUP BY lname, fname, residence
);

--4. Task 4: QEP: Heap and Index
SELECT *
FROM Customer
WHERE lname = N'Pokorná'
  AND fname = N'Jana'
  AND residence = N'Berlin'
OPTION (MAXDOP 1);  --minimum number of records


--6
CREATE INDEX idx_customer_ln_fn_rs
ON Customer(lname, fname, residence);

DROP INDEX idx_customer_ln_fn_rs on Customer;

SET STATISTICS TIME ON;
CREATE INDEX idx_customer_ln_fn_rs
ON Customer(lname, fname, residence);
SET STATISTICS TIME OFF;  ----index creation time

SELECT COUNT(*) AS index_entries
FROM Customer;    --index entries

SELECT    --index pages
    index_id,
    page_count,
    record_count
FROM sys.dm_db_index_physical_stats(
    DB_ID(), OBJECT_ID('Customer'), NULL, NULL, 'DETAILED'
)
WHERE index_id > 0;   -- index_id = 2 for nonclustered index


--7 maximum number of result records.
SELECT TOP 1 lname, fname, residence, COUNT(*) AS cnt
FROM Customer
GROUP BY lname, fname, residence
ORDER BY cnt DESC;


SET STATISTICS IO ON;
SET STATISTICS TIME ON;

SELECT *
FROM Customer
WHERE lname = N'Novák'
  AND fname = N'Jan'
  AND residence = N'Praha'
OPTION (MAXDOP 1);

SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;



--11

SELECT 
    SUM(a.total_pages) * 8 / 1024.0 AS heap_size_mb
FROM sys.allocation_units a
JOIN sys.partitions p 
    ON a.container_id = p.hobt_id
WHERE p.object_id = OBJECT_ID('Customer')
  AND p.index_id IN (0,1);   -- 0 = heap, 1 = clustered index


SELECT 
    i.name AS index_name,
    SUM(a.total_pages) * 8 / 1024.0 AS index_size_mb
FROM sys.indexes i
JOIN sys.partitions p 
    ON i.object_id = p.object_id AND i.index_id = p.index_id
JOIN sys.allocation_units a 
    ON a.container_id = p.hobt_id
WHERE i.object_id = OBJECT_ID('Customer')
  AND i.index_id > 0
GROUP BY i.name;  --all index

SELECT 
    SUM(a.total_pages) * 8 / 1024.0 AS total_index_size_mb
FROM sys.indexes i
JOIN sys.partitions p 
    ON i.object_id = p.object_id AND i.index_id = p.index_id
JOIN sys.allocation_units a 
    ON a.container_id = p.hobt_id
WHERE i.object_id = OBJECT_ID('Customer')
  AND i.index_id > 0;   --total index size

SELECT
    (SELECT SUM(a.total_pages) * 8 / 1024.0
     FROM sys.allocation_units a
     JOIN sys.partitions p 
         ON a.container_id = p.hobt_id
     WHERE p.object_id = OBJECT_ID('Customer')
       AND p.index_id IN (0,1)) AS heap_mb,

    (SELECT SUM(a.total_pages) * 8 / 1024.0
     FROM sys.indexes i
     JOIN sys.partitions p 
         ON i.object_id = p.object_id AND i.index_id = p.index_id
     JOIN sys.allocation_units a 
         ON a.container_id = p.hobt_id
     WHERE i.object_id = OBJECT_ID('Customer')
       AND i.index_id > 0) AS index_mb;



