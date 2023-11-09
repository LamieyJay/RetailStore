--CREATE THREE DATABASES FOR THE SYSTEM 
-- TESCA OLTP
	--Backup of the db
--TESCA staging
--TESCA EDW 

Create databaSe TescaEDW

If not exists (select name from sys.databases where name ='TescaStaging')
	create database TescaStaging
else print ('database already exists')

SELECT * FROM SalesTransaction
SELECT * FROM PurchaseTransaction
SELECT MIN(TransDate), Max(TransDate) FROM SalesTransaction --To Shtift BY 3 Years
SELECT MIN(TransDate), Max(TransDate) FROM PurchaseTransaction -- To Shift by 3 years

--SHIFT DATE SO THAT WE CAN HAVE MORE CURRENT DATA TO WORK WITH.
Update SalesTransaction
SET 
TransDate = DATEADD(YEAR, 3, TransDate),
OrderDate = DATEADD(YEAR, 3, OrderDate),
DeliveryDate = DATEADD(YEAR, 3, DeliveryDate)

UPDATE PurchaseTransaction
set
TransDate = DATEADD(YEAR, 3, TransDate),
OrderDate = DATEADD(YEAR, 3, OrderDate),
DeliveryDate = DATEADD(YEAR, 3, DeliveryDate),
ShipDate = DATEADD(YEAR, 3, ShipDate)




