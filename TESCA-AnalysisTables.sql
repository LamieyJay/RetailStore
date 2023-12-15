SELECT * FROM SalesTransaction
order BY TransDATE DESC

-----------------------------------------------------------------------------
--This query produces no  results because of the time component in the cells. It will search for a date with the exact time stamp, 24 hours ago.
SELECT * FROM SalesTransaction WHERE TransDate = DATEADD(day, -1, getdate())

--The query below only calculates using the date and not the time. Convert(date, <date>)
SELECT * FROM SalesTransaction WHERE convert(Date, TransDate) = DATEADD(day, -1, convert(date, getdate()))

/*Therefore we use the Convert() function to convert the datetime to just date, so that we can find the records from
the previous days*/
-----------------------------------------------------------------------------

/*OLTP Sales Analysis
Select from the OLTP*/
--Check if there are any records in the table. 
----If there's no data, then we want to move all data from company inception.
----If there's data, we're moving only n-1 data
USE TescaOLTP
IF (SELECT COUNT(*) FROM TescaEDW.EDW.Fact_SalesAnalysis) <= 0
	SELECT 
		s.TransactionID,
		s.TransactionNO,
		DATEPART(HOUR, TransDate) TransHour,
		Convert(date, s.transdate) TransDate,
		Convert(date, s.orderdate) OrderDate,
		Datepart(HOUR, OrderDate) OrderHour,
		convert(Date, s.DeliveryDate) DeliveryDate,
		s.ChannelID,
		s.Customerid,
		S.EmployeeID, 
		S.ProductID, 
		S.StoreID, 
		S.PromotionID, 
		S.Quantity, 
		S.TaxAmount, 
		S.LineAmount, 
		s.linediscountamount,
		getdate() as LoadDate
		FROM SalesTransaction S WHERE convert(Date, TransDate) <= DATEADD(day, -1, convert(date, getdate())) 
		---- Run this first to load ALL records from before today
ELSE
	SELECT 
		s.TransactionID,
		s.TransactionNO,
		DATEPART(HOUR, TransDate) TransHour,
		Convert(date, s.transdate) TransDate,
		Convert(date, s.orderdate) OrderDate,
		Datepart(HOUR, OrderDate) OrderHour,
		convert(Date, s.DeliveryDate) DeliveryDate,
		s.ChannelID,
		s.Customerid,
		S.EmployeeID, 
		S.ProductID, 
		S.StoreID, 
		S.PromotionID, 
		S.Quantity, 
		S.TaxAmount, 
		S.LineAmount, 
		s.linediscountamount,
		getdate() as LoadDate
	FROM SalesTransaction S WHERE convert(Date, TransDate) = DATEADD(day, -1, convert(date, getdate())) 
	----Run this daily after to run only the records from the previous day (N-1 data)

SELECT DATEADD(day, -1, getdate())

--Check PRE-count 

IF (select count(*) FROM TescaEDW.EDW.Fact_SalesAnalysis) <=  0 
	SELECT count (*) as SourceCount FROM SalesTransaction WHERE convert(Date, TransDate) <= DATEADD(day, -1, convert(date, getdate()))
ELSE
	SELECT count (*) as SourceCount FROM SalesTransaction WHERE convert(Date, TransDate) = DATEADD(day, -1, convert(date, getdate()))

	
USE TescaStaging

CREATE TABLE staging.SalesAnalysis(
	TransactionID int,
	TransactionNo nvarchar(50),
	TransDate datetime,
	TransHour int,
	OrderDate date,
	OrderHour int,
	DeliveryDate date,
	ChannelID int,
	CustomerID int,
	EmployeeID int,
	ProductID int,
	StoreID int,
	PromotionID int,
	Quantity float,
	TaxAmount float,
	LineAmount float,
	LineDiscountAmount float,
	LoadDate datetime default getdate(),
	constraint staging_SalesAnalysis_pk primary key (TransactionID)
)

SELECT * FROM sTAGING.SalesAnalysis
select count (*) as DesCount from staging.SalesAnalysis
Truncate Table staging.SalesAnalysis

----EDW SalesAnalysis-----Fact Table----

select count (*) as CurrentCount from staging.SalesAnalysis

USE TescaEDW

SELECT COUNT (*) AS PreCount from EDW.fact_salesAnalysis

Create Table EDW.fact_salesAnalysis (
	SalesSk bigint identity (1,1),
	TransactionNo nvarchar (50),
	TransDateSk int,
	TransHourSk int,
	OrderDateSk int,
	OrderHourSk int,
	DeliverySk int,
	ChannelSk int,
	CustomerSK int,
	EmployeeSk int,
	ProductSk int,
	StoreSk int,
	PromotionSk int,
	Quantity float,
	TaxAmount float,
	LineAmount float,
	LineDiscountAmount float,
	LoadDate datetime default getdate(),
	constraint EDW_salesAnalysis_SlesSk primary key (SalesSk),
	constraint EDW_sales_Transdatesk foreign key (TransDateSk) references EDW.DimDate(DateSk),
	constraint EDW_sales_Transhoursk foreign key (TransHourSk) references EDW.DimTime(TimeSk),
	constraint EDW_sales_Orderdatesk foreign key (OrderDateSk) references EDW.DimDate(DateSk),
	constraint EDW_sales_Orderhoursk foreign key (OrderHourSk) references EDW.DimTime(TimeSk),
	constraint EDW_sales_Deliverysk foreign key (DeliverySk) references EDW.DimDate(DateSk),
	constraint EDW_sales_ChannelSk foreign key (ChannelSk) references EDW.DimPOSChannel(ChannelSk),
	constraint EDW_sales_Customersk foreign key (CustomerSk) references EDW.DimCustomer(CustomerSk),
	constraint EDW_sales_Employeesk foreign key (EmployeeSk) references EDW.DimEmployee(EmployeeSk),
	constraint EDW_sales_Productsk foreign key (ProductSk) references EDW.DimProduct(ProductSk),
	constraint EDW_sales_Storesk foreign key (StoreSk) references EDW.DimStore(StoreSk),
	constraint EDW_sales_Promotionsk foreign key (PromotionSk) references EDW.DimPromotion(PromotionSk),
)

	SELECT * FROM TESCAEDW.EDW.DimTime
	SELECT * FROM EDW.DimPromotion




	-----------Purchase Analysis------------
	---OLTP Purchase 

	USE TescaOLTP

	--Run query for data where TransDate is less than today
IF (SELECT count(*) from TescaEDW.EDW.Fact_PurchaseAnalysis) <= 0
	SELECT 
	P.TransactionID, 
	P.TransactionNo, 
	convert(date, P.TransDate) TransDate, 
	Convert(date, OrderDate) OrderDate, 
	Convert(date, DeliveryDate) DeliveryDate, 
	DATEDIFF(DAY, P.orderdate, P.deliverydate) + 1 as DifferentialDays,
	P.VendorID, 
	P.EmployeeID, 
	P.ProductID,
	P.StoreID,
	P.Quantity,
	P.TaxAmount,
	P.LineAmount,
	getdate() as LoadDate
	FROM PurchaseTransaction P
	WHERE Convert(date, p.TransDate) <= dateadd(day, -1, convert(date,getdate()))
	--Run query for data where TransDate is yesterday if there's already data in the DWH
ELSE
	SELECT 
	P.TransactionID, 
	P.TransactionNo, 
	convert(date, P.TransDate) TransDate, 
	Convert(date, OrderDate) OrderDate, 
	Convert(date, DeliveryDate) DeliveryDate, 
	DATEDIFF(DAY, P.orderdate, P.deliverydate) + 1 as DifferentialDays,
	P.VendorID, 
	P.EmployeeID, 
	P.ProductID,
	P.StoreID,
	P.Quantity,
	P.TaxAmount,
	P.LineAmount,
	getdate() as LoadDate
	FROM PurchaseTransaction P
	WHERE Convert(date, p.TransDate) = dateadd(day, -1, convert(date,getdate()))


IF (SELECT COUNT (*) FROM TESCA.EDW.Fact_PurchaseAnalysis) <= 0
SELECT COUNT(*) AS SourceCount from PurchaseTransaction p
WHERE Convert(date, p.TransDate) <= dateadd(day, -1, convert(date,getdate()))
ELSE
SELECT COUNT(*) AS SourceCount from PurchaseTransaction p
WHERE Convert(date, p.TransDate) = dateadd(day, -1, convert(date,getdate()))


------Staging PurchaseAnalysis
SELECT * FROM Staging.purchaseAnalysis
Use TescaStaging

select count(*) as DesCount from staging.PurchaseAnalysis  

Create Table Staging.PurchaseAnalysis (
	TransactionID int,
	TransactionNo nvarchar(50),
	TransDate Date,
	OrderDate Date,
	DeliveryDate Date,
	VendorID int,
	EmployeeID int,
	ProductID int,
	StoreID int,
	DifferentialDays int,
	Quantity float,
	TaxAmount float,
	LineAmount float,
	LoadDate datetime default getdate(),
	constraint staging_purchaseabalysis_pk primary key (TransactionID)
	)

SELECT COUNT(*) as CurrentCount from Staging.PurchaseAnalysis
Truncate Table Staging.PurchaseAnalysis 

-------EDW Purchase Analysis 
USE TescaEDW

Create Table EDW.Fact_PurchaseAnalysis(
	PurchaseAnalysisSK bigint identity (1,1),
	TransactionNo nvarchar(50),
	TransDateSK int,
	OrderDateSk int,
	DeliveryDateSk int,
	VendorSk int,
	EmployeeSk int,
	ProductSk int,
	StoreSk int,
	DifferentialDays int,
	Quantity float,
	TaxAmount float,
	LineAmount float,
	LoadDate datetime default getdate(),
	constraint edw_fact_PurchaseAnalysis_sk primary key(PurchaseAnalysisSk),
	constraint EDW_Purchase_Transdatesk foreign key (TransDateSk) references EDW.DimDate(DateSk),
	constraint EDW_Purchase_Orderdatesk foreign key (OrderDateSk) references EDW.DimDate(DateSk),
	constraint EDW_Purchase_DeliveryDatesk foreign key (DeliveryDateSk) references EDW.DimDate(DateSk),
	constraint EDW_Purchase_VendorSk foreign key (VendorSk) references EDW.DimVendor(VendorSk),
	constraint EDW_Purchase_Employeesk foreign key (EmployeeSk) references EDW.DimEmployee(EmployeeSk),
	constraint EDW_Purchase_Productsk foreign key (ProductSk) references EDW.DimProduct(ProductSk),
	constraint EDW_Purchase_Storesk foreign key (StoreSk) references EDW.DimStore(StoreSk),
	)


SELECT COUNT(*) as PreCount from EDW.Fact_PurchaseAnalysis


-------OVERTIME -------
---From CSV file into Staging area 
use TescaStaging

CREATE TABLE Staging.Overtime 
(
	OvertimeID int,
	EmployeeNo nvarchar(50),
	FirstName nvarchar(50),
	LastName nvarchar(50),
	StartOvertime datetime,
	EndOvertime datetime, 
	LoadDate datetime default getdate()
)

--SELECT COUNT(*) PreCount from EDW.fact_OvertimeAnalysis

SELECT COUNT(*) As Descount from Staging.Overtime
Truncate Table Staging.Overtime


--Deduplicate to get base table for EDW
SELECT Max(OvertimeID), EmployeeNo, FirstName, LastName, StartOvertime, EndOvertime from Staging.Overtime
group by EmployeeNo, FirstName, LastName, StartOvertime, EndOvertime

--Select from deduplicated data to move from Staging to EDW.
Select 
OvertimeID, 
EmployeeNo, 
CONVERT(date, StartOvertime) StartOvertimeDate, 
DATEPART(hour, StartOvertime) StartOvertimeHour,
CONVERT(Date, EndOvertime) EndOvertime,
DATEPART(hour, EndOvertime) EndOvertimehour,
--DATEDIFF(hour, StartOvertime, EndOvertime) as OvertimeHour
convert(float, DATEDIFF(Minute, StartOvertime, EndOvertime)/60)  as OvertimeHour
from (
	SELECT Max(OvertimeID) OvertimeID, EmployeeNo, FirstName, LastName, StartOvertime, EndOvertime from Staging.Overtime
	group by EmployeeNo, FirstName, LastName, StartOvertime, EndOvertime)

	 
Select count (*) CurrentCount from 
	(
	SELECT Max(OvertimeID), EmployeeNo, FirstName, LastName, StartOvertime, EndOvertime from Staging.Overtime
	group by EmployeeNo, FirstName, LastName, StartOvertime, EndOvertime
	)


----Fact table------
use TescaEDW

Select count(*) as EDWCount from EDW.fact_overtimeAnalysis

Create Table EDW.Fact_OvertimeAnalysis (
	OvertimeSk bigint identity(1,1),
	OvertimeID int,
	EmployeeSk int,
	StartDateSK int,
	StartHourSk int,
	EndDateSk int, 
	EndHourSk int,
	OvertimeHour float, -- metric
	LoadDate datetime default getdate(),
	constraint EDW_overtimeanalysis_sk primary key (OvertimeSk),
	constraint EDW_overtime_employeeSk foreign key (EmployeeSk) references EDW.dimEmployee(employeeSk),
	constraint EDW_overtime_startdateSk foreign key (StartDateSk) references EDW.dimdate(dateSk),
	constraint EDW_overtime_starthourSk foreign key (StartHourSk) references EDW.dimTime(TimeSk),
	constraint EDW_overtime_EnddateSk foreign key (EndDateSk) references EDW.dimDate(dateSk),
	constraint EDW_overtime_EndhourSk foreign key (EndHourSk) references EDW.DimTime(timeSk)
	)

	Drop table fact_overtimeanalysis

------Absence Analysis ------
--first entry for the day is the record to be retained. i.e min, when grouped by all data.
 
 /*Loading the data from the CSV file for this one is more tricky.There's no sure way to identify the data that was entered first or last
 because there is no ID column. So we're creating a new column and surrogate keys will be assigned. 
 */


 USE TescaStaging
 Create Table Staging.Absent_Analysis (
	AbsentSk bigint identity(1,1),
	empid int,
	Store int,
	Absent_Date date,
	Absent_hour int,
	Absent_category int,
	LoadDate datetime default getdate(),
	constraint staging_absent_pk primary key (AbsentSk)
	)

	SELECT * FROM STaging.Absent_Analysis

	SELECT COUNT(*) DesCount from Staging.Absent_Analysis
	Truncate table Staging.absent_analysis

	SELECT min(AbsentSK), EmpID, Store, Absent_Date, Absent_hour, Absent_Category from staging.Absent_Analysis
	group by empid, store, absent_date, absent_category

	---------DEDUPLICATE DATA
--What makes the data duplicate is the empID, store, date, and category. The absent_hour is not relevant here.
--4 ways to deduplicate 
	--1. Subquery
		
		SELECT EmpID, Store, Absent_Date, Absent_hour, Absent_Category from staging.Absent_Analysis 
		WHERE AbsentSk in
		(SELECT min(AbsentSK) from staging.Absent_Analysis
		group by empid, store, absent_date, absent_category)

	--2. CTE with subquery
		WITH Deduplicatedata As (
			SELECT min(absentsk) AbsentSk, from staging.Absent_Analysis
			group by empid, Store, Absent_Date, Absent_Category
			)
			SELECT empid, Store, Absent_Date, Absent_hour, absent_category from staging.Absent_Analysis 
		WHERE absentsk in (select absentsk from Deduplicatedata)

	--3. CTE with JOIN
		WITH Deduplicatedata As (
			SELECT min(absentsk) AbsentSk, from staging.Absent_Analysis
			group by empid, Store, Absent_Date, Absent_Category
			)
			SELECT empid, Store, Absent_Date, Absent_hour, absent_category from staging.Absent_Analysis a
			inner join Deduplicatedata d on d.AbsentSk = a.AbsentSk

	--4. TEMP Table and Join
	If Object_ID('tempdb..#dedupdata') in not null
	drop table #dedupdata

	select min(absentSk) absentSk into #dedupdata from Staging.Absent_Analysis a
	group by empid, Store, Absent_Date, Absent_Category

	SELECT empid, Store, Absent_Date, absent_hour, Absent_Category from Staging.Absent_Analysis a
	inner join #dedupdata d on d.absentSk = a.AbsentSk

	----	Current Count
	SELECT COUNT(*) CurrentCount FROM staging.Absent_Analysis 
		WHERE AbsentSk in
		(
		SELECT min(AbsentSK) from staging.Absent_Analysis
		group by empid, store, absent_date, absent_category
		 )

		 SELECT COUNT(*) AS edwCount from EDW.Fact_Absent_Analysis 
----Absent EDW 
USE TescaEDW

Create Table EDW.fact_Absent_Analysis
(
	AbsentSk bigint identity(1,1),
	employeeSk int,
	StoreSk int,
	Absent_DateSk int,
	Absent_hourSk int,
	Absent_categorySk int,
	constraint EDW_absentanalysis_sk primary key (AbsentSk),
	constraint EDW_Absent_StoreSK foreign key (StoreSk) references EDW.DimStore(StoreSk),
	constraint EDW_ABsent_EmployeeSk foreign Key (EmployeeSk) references EDW.DimEmployee(EmployeeSk),
	constraint EDW_absent_datesk foreign key (absent_datesk) references EDW.dimdate(datesk),
	constraint EDW_absent_categorysk foreign key (absent_categorysk) references EDW.dimAbsence(categorySk)
	)
	drop table edw.fact_Absent_Analysis
	---26/03/2023 - 2:35


----Misconduct Analysis ----

use TescaStaging

Create Table Staging.Misconduct_Analysis(
	MisconSk bigint identity(1,1),
	Empid int,
	StoreID int,
	Misconduct_date date,
	Misconduct_id int,
	Decision_id int,
	LoadDate datetime default getdate(),
	constraint staging_misconSk_pk primary key (misconSk)
	)

	SELECT Count(*) AS edwCount from EDW.Fact_misconduct_Analysis

	SELECT * FROM STaging.Misconduct_Analysis
	--Source Count is taken care of by the TEL
	--DesCount Count---

SELECT Count(*) as DesCount from staging.Misconduct_Analysis
TRUNCATE TABLE Staging.Misconduct_Analysis

--To get the last entry as the correct data to keep
		SELECT MAX(misconSk) MisconSk, EmpID, StoreId, Misconduct_date, misconduct_id, decision_id
		from staging.misconduct_analysis
		Group By EmpID, StoreId, Misconduct_date, misconduct_id, decision_id


---SELECT THE LAST RECORD FROM STAGING
		SELECT MisconSk, EmpID, StoreId, Misconduct_date, misconduct_id, decision_id
		from staging.misconduct_analysis where misconsk in 
		(
		SELECT MAX(misconSk) from staging.misconduct_analysis
		Group By EmpID, StoreId, Misconduct_date, misconduct_id, decision_id
		) 


		SELECT COUNT (*) AS CURRENTCOUNT
		FROM staging.misconduct_analysis where misconsk in 
		(
		SELECT MAX(misconSk) from staging.misconduct_analysis
		Group By EmpID, StoreId, Misconduct_date, misconduct_id, decision_id
		) 

		-----EDW Misconduct 
		--This is a factless fact table (read more on this)
		USE TescaEDW

Create Table EDW.Fact_Misconduct_Analysis (
	MisconSk bigint identity(1,1),
	EmployeeSk int,
	StoreSk int,
	Misconduct_datesk int,
	Misconductid_Sk int,
	Decisionid_sk int,
	LoadDate datetime default getdate(),
	constraint EDW_misconSk_sk primary key (misconSk),
	constraint EDW_Misconduct_employee_Sk foreign key(employeeSk) references edw.dimEmployee(employeeSk),
	constraint EDW_misconduct_store_sk foreign key(storesk) references edw.dimstore(storesk),
	constraint edw_misconduct_Date_sk foreign key(misconduct_datesk) references edw.dimdate(datesk),
	constraint edw_misconduct_misconduct_sk foreign key(misconductid_sk) references edw.dimmisconduct(MisconductSk),
	constraint edw_misconduct_decision_sk foreign key(decisionid_sk) references edw.dimDecision(DecisionSk)
	)

	--constraint schemaname_businessprocess_column foreign key(column) references schemaname.dimensiontable(primary key column)
	select count(*) as PreCount from EDW.Fact_Misconduct_Analysis



	DROP TABLE EDW.Fact_Misconduct_Analysis