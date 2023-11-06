--Deduplication Process - Deatiled

If object_id('tempdb..##DupData') IS NULL
drop table ##DupData

/*1. Creating a temp table.
Selecting all the values into a global temp table to see duplicates.*/
SELECT p.TransID, p.OrderID, p.AccountNumber, p.Supplier, p.City, p.DueDate, p.OrderDate 
INTO ##DupData 
FROM PurchaseTrans P 

/*2. Select all values from the temp table*/
SELECT TransID, OrderID, AccountNumber, Supplier, City, DueDate, OrderDate  FROM ##DupData
ORDER BY OrderID

/*3. Select the unique transactions (from the temp table) by grouping by mulitple columns.
Use ORDER BY to see that the records are no longer repeated*/
SELECT OrderID, AccountNumber, Supplier, City, DueDate, OrderDate  FROM ##DupData
GROUP BY OrderID, AccountNumber, Supplier, City, DueDate, OrderDate 
ORDER BY OrderID

/*4. Use the Min and Max functions to get the first or last transaction recorded with duplicate values.
-- Selecting the first transaction in each group, hence (MIN)*/
SELECT MIN(TransID) FirstTransID, OrderID, AccountNumber, Supplier, City, DueDate, OrderDate FROM ##DupData
GROUP BY OrderID, AccountNumber, Supplier, City, DueDate, OrderDate 

--5. Putting the Unduplicated Data into a new table 
SELECT  MIN(TransID) FirstTransID, OrderID, AccountNumber, Supplier, City, DueDate, OrderDate 
INTO ##UnDupData 
FROM ##DupData
GROUP BY OrderID, AccountNumber, Supplier, City, DueDate, OrderDate 

--6. Duplicating the PurchaseTrans table to make changes without changing the original table.
SELECT p.TransID, p.OrderID, p.AccountNumber, p.Supplier, p.City, p.DueDate, p.OrderDate 
INTO PurchaseTransDups 
FROM PurchaseTrans P 

--7. Deleting records from the actual table where the TransID is not in the UnDupData table. 
DELETE FROM PurchaseTransDups 
WHERE TransID not in 
(SELECT FirstTransID FROM ##UnDupData)

--8. Selecting from the original table to check results 
SELECT * FROM PurchaseTransDups