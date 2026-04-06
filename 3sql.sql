
SET SHOWPLAN_TEXT ON;
SET SHOWPLAN_TEXT OFF;
SET SHOWPLAN_ALL ON;
SET SHOWPLAN_ALL OFF;

select * from Customer 
  where birthday =  '2000-01-01';

exec PrintPagesHeap 'Customer';


SELECT COUNT(*) FROM OrderItem;

---------------------------------------

SET STATISTICS IO ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME ON;
SET STATISTICS TIME OFF;

select * from Product 
where unit_price between 1300000 and 1800000;

--Task 3.1 – Sequential heap scan on OrderItem
SELECT TOP 100 *
FROM OrderItem;   -- no PK filter, no index predicate → table/heap scan

SELECT COUNT(*) AS rows_returned
FROM (
    SELECT TOP 100 *
    FROM OrderItem
) AS x;   --2. Number of rows returned

exec PrintPagesHeap 'OrderItem';  -- shows heap pages

--Task 3.2 – Disabling / forcing parallel execution
SELECT TOP 100 *
FROM OrderItem
OPTION (MAXDOP 1);   -- repeat task1

--Task 3.3 – Deletion + physical deletion
delete from OrderItem where ido % 4 = 0; 

--Total rows + heap blocks:
SELECT COUNT(*) AS total_rows FROM OrderItem;
exec PrintPagesHeap 'OrderItem';   -- heap pages(repeat task1)

--Physical deletion / compaction
ALTER TABLE OrderItem REBUILD;


select * from OrderItem 
where orderitem.unit_price between 1 and 300;

select * from OrderItem 
where orderitem.unit_price between 1 and 300
OPTION (MAXDOP 1);  -- force sequential

exec PrintPagesHeap 'OrderItem';

-----------------------------

truncate table OrderItem;
delete from "Order";


select * from Customer
where fname = 'Jana' and lname='Pokorná' and residence = 'Berlin';
-- 46

alter table Customer rebuild;




