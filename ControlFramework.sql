
                    ------ETL Control Framework------
Create database TescaControl 
USE TescaControl
create schema ctrl

---Staging, EDW----
create table ctrl.environment
	(
		EnvironmentID int,
		Environment nvarchar(255),
		CreatedDate datetime,
		constraint ctrl_environment_pk primary key (EnvironmentID)
	)
Insert into ctrl.environment(EnvironmentID, Environment, CreatedDate)
values 
(1, 'Staging', getdate()),
(2, 'EDW', getdate())


	--daily, weekly, monthly, yearly
	Create table ctrl.Frequency
	(
		FrequencyID int,
		Frequency nvarchar(255),
		CreatedDate datetime, 
		constraint ctrl_frequency_pk primary key (FrequencyID)
	)
	Insert into ctrl.Frequency(FrequencyID, Frequency, CreatedDate)
	Values
	/*(1, 'Daily', getdate()),
	(2, 'Weekly', getdate()),
	(3, 'Monthly', getdate()),*/
	(4, 'Yearly', getdate())


	--Fact, Dimensions
Create table ctrl.PackageType
(
	PackageTypeID int,
	PackageType nvarchar(255),
	CreatedDate datetime,
	constraint ctrl_packagetype_pk Primary Key (PackageTypeID)
)
Insert into ctrl.PackageType(PackageTypeID, PackageType, CreatedDate)
Values 
(1, 'Dimension', getdate()),
(2, 'Fact', getdate())


-----Package---
--The package table controls the activity in the pipeline.
Create Table ctrl.Package
(
	PackageID int,
	PackageName nvarchar(255), ------ dimProduct.dtsx
	PackageTypeID int, ------1 Dimension, 2 Fact
	SequenceNo int, ----100
	EnvironmentID int, -----2-EDW, 1- Staging
	FrequencyID int,   -----1 Daily
	RunStartDate date, ----04-01-2023
	RunEndDate Date,  ----04-07-2023
	Active bit, -----0-False, 1-True
	LastRunDate datetime,
	constraint ctrl_package_packageID_pk primary key (PackageID),
	constraint ctrl_package_packagetype_fk foreign key (PackageTypeID) references ctrl.packageType(PackageTypeID),
	constraint ctrl_package_environment_fk foreign key (EnvironmentID) references ctrl.Environment(EnvironmentID),
	constraint ctrl_package_Frequency_fk foreign key (FrequencyID) references ctrl.Frequency(FrequencyID),
)

		--Staging Facts and Dimensions
Insert into ctrl.package(PackageID, PackageName, PackageTypeID, SequenceNo, EnvironmentID, FrequencyID, RunStartDate, Active)
Values
/*(1, 'stgProduct.dtsx', 1, 100, 1, 1,  convert(date, getdate()), 1),
(2, 'stgPromotion.dtsx', 1, 200, 1, 1, convert(date, getdate()), 1),
(3, 'stgStore.dtsx', 1, 300, 1, 1, convert(date, getdate()), 1),
(4, 'stgCustomer.dtsx', 1, 400, 1, 1, convert(date, getdate()), 1),
(5, 'stgPOSChannel.dtsx', 1, 500, 1, 1, convert(date, getdate()), 1)
(6, 'stgEmployee.dtsx', 1, 600, 1, 1, convert(date, getdate()), 1)
(7, 'stgVendor.dtsx', 1, 700, 1, 1, convert(date, getdate()), 1),
(8, 'stgMisConduct.dtsx', 1, 800, 1, 1, convert(date, getdate()), 1),
(9, 'stgMisDecision.dtsx', 1, 900, 1, 1, convert(date, getdate()), 1),
(10, 'stgAbsence.dtsx', 1, 1000, 1, 1, convert(date, getdate()), 1),
(11, 'stgSalesAnalysis.dtsx', 2, 1100, 1, 1, convert(date, getdate()), 1),
(12, 'stgPurchaseAnalysis.dtsx', 2, 1200, 1, 1, convert(date, getdate()), 1),
(13, 'stgOvertimeAnalysis.dtsx', 2, 1300, 1, 1, convert(date, getdate()), 1)
(14, 'stgMisconductAnalysis.dtsx', 2, 1400, 1, 1, convert(date, getdate()), 1)*/
(15, 'stgAbsenceAnalysis.dtsx', 2, 1500, 1, 1, convert(date, getdate()), 1)

		--Staging mterics Script
declare @PackageID int = ?
declare @StgSourceCount int =?
declare @StgDesCount int =?
INSERT INTO CTRL.metrics (PackageID, StgSourceCount, StgDesCount, RunDate)
values (@PackageID, @StgSourceCount, @StgDesCount, getdate())
Update ctrl.package
set LastRunDate=getdate() where packageid = @PackageID



			--EDW Dimension tables
Insert into ctrl.package(PackageID, PackageName, PackageTypeID, SequenceNo, EnvironmentID, FrequencyID, RunStartDate, Active)
Values

	/*(16, 'dimProduct.dtsx', 1, 100, 2, 1, convert(date, getdate()), 1)
	(17, 'dimPromotion.dtsx', 1, 200, 2, 1, convert(date, getdate()), 1)
	(18, 'dimStore.dtsx', 1, 300, 2, 1, convert(date, getdate()), 1)
	(19, 'dimCustomer.dtsx', 1, 400, 2, 1, convert(date, getdate()), 1)
	(20, 'dimPOSChannel.dtsx', 1, 500, 2, 1, convert(date, getdate()), 1)
	(21, 'dimEmployee.dtsx', 1, 600, 2, 1, convert(date, getdate()), 1)
	(22, 'dimVendor.dtsx', 1, 700, 2, 1, convert(date, getdate()), 1)
	(23, 'dimMisconduct.dtsx', 1, 800, 2, 1, convert(date, getdate()), 1)
	(24, 'dimDecision.dtsx', 1, 900, 2, 1, convert(date, getdate()), 1)*/
	(25, 'dimAbsence.dtsx', 1, 1000, 2, 1, convert(date, getdate()), 1)

		---EDW metrics script
declare @PackageID int =?
declare @PreCount int = ?
declare @CurrentCount int = ?
declare @Type1Count int = ?
declare @Type2Count int = ?
declare @PostCount int = ?
Insert into ctrl.metrics (PackageID, PreCount, CurrentCount, Type1Count, Type2Count, PostCount, RunDate)
Values (@PackageID, @PreCount, @CurrentCount, @Type1Count, @Type2Count, @PostCount, getdate())
Update ctrl.Package
set LastRunDate=getdate() where PackageID = @PackageID



			--EDW Fact tables
	insert into ctrl.package(PackageID, PackageName, PackageTypeID, SequenceNo, EnvironmentID, FrequencyID, RunStartDate, Active)
	VALUES 
	/*(26, 'factSalesAnalysis.dtsx', 2, 1100, 2, 1, convert(date, getdate()), 1)
	(27, 'factPurchaseAnalysis.dtsx', 2, 1200, 2, 1, convert(date, getdate()), 1)
	(28, 'factOvertimeAnalysis.dtsx', 2, 1300, 2, 1, convert(date, getdate()), 1)
	(29, 'FactMisconductAnalysis.dtsx', 2, 1400, 2, 1, convert(date, getdate()), 1)*/
	(30, 'FactAbsenceAnalysis.dtsx', 2, 1500, 2, 1, convert(date, getdate()), 1)


	UPDATE ctrl.Package
	SET PackageName = 'FactAbsenceAnalysis.dtsx'
	WHERE PackageID=30

		---EDW metrics script for FACT tables
	--We do not need Type1 and Type2 count here because we are not loading 'Dimenions'
declare @PackageID int =?
declare @PreCount int = ?
declare @CurrentCount int = ?
declare @PostCount int = ?

Insert into ctrl.metrics (PackageID, PreCount, CurrentCount, PostCount, RunDate)
Values (@PackageID, @PreCount, @CurrentCount, @PostCount, getdate())
Update ctrl.Package
set LastRunDate=getdate() where PackageID = @PackageID



	use  TescaControl
	Create table ctrl.anomalies (
	anomalysk bigint identity(1,1),
	packageID int,
	Tablename nvarchar(255),
	Columnname nvarchar(255),
	RecordID int,
	CreatedDate datetime default getdate(),
	constraint control_anomalies_SK primary key(anomalysk),
	constraint anomalies_package_fk foreign key(PackageID) references ctrl.package(packageID)
	)

	select * from ctrl.anomalies
--drop table ctrl.Package 
--drop table ctrl.metrics


-------------METRICS--------------
Create Table Ctrl.metrics
(
	MetricID bigint identity(1,1),
	PackageID int,
	StgSourceCount int,
	StgDesCount int,
	PreCount int,
	CurrentCount int,
	Type1Count int,
	Type2Count int,
	PostCount int,
	RunDate datetime,
	constraint ctrl_metrics_metricID primary key (MetricID),
	constraint ctrl_metrics_package_fk foreign key (PackageID) references ctrl.package(PackageID)
)

 
Select * from ctrl.metrics
SELECT * FROM ctrl.Frequency
SELECT * FROM ctrl.Package
SELECT * FROM ctrl.anomalies
	/*
	Daily - runs everyday
	Weekly - Runs at the en dof every week
	Monthly - At the end of every month
	Yearly - At the end of every year
	*/
		SELECT PackageID, PackageName, SequenceNo, FrequencyID FROM (
					--These conditions must be true for my pipeline to run DAILY
			SELECT PackageID, PackageName, SequenceNo, FrequencyID FROM CTRL.Package
			Where (Active = 1 AND RunStartDate <= CONVERT(DATE, GETDATE()))
			AND (RunEndDate IS NULL OR RunEndDate >= convert(date, getdate()))
			AND EnvironmentID = 1 AND FrequencyID = 1

			UNION ALL 
					--Run the package WEEKLY.
			SELECT PackageID, PackageName, SequenceNo, FrequencyID FROM CTRL.Package
			Where (Active = 1 AND RunStartDate <= CONVERT(DATE, GETDATE()))
			AND (RunEndDate IS NULL OR RunEndDate >= convert(date, getdate()))
			AND EnvironmentID = 1 
			AND FrequencyID = 2 
			and DATEPART(WEEKDAY, DATEADD(DAY, -1, GETDATE())) = 7

			UNION ALL 
					--Run the package at the end of every month
			SELECT PackageID, PackageName, SequenceNo, FrequencyID FROM CTRL.Package
			Where (Active = 1 AND RunStartDate <= CONVERT(DATE, GETDATE()))
			AND (RunEndDate IS NULL OR RunEndDate >= convert(date, getdate()))
			AND EnvironmentID = 1 
			AND FrequencyID = 3
			AND EOMONTH(dateadd(day, -1, convert(date,getdate()))) = dateadd(day, -1, convert(date,getdate()))

			UNION ALL
			 ------Run the package at the end of the year.
			SELECT PackageID, PackageName, SequenceNo, FrequencyID FROM CTRL.Package
			Where (Active = 1 AND RunStartDate <= CONVERT(DATE, GETDATE()))
			AND (RunEndDate IS NULL OR RunEndDate >= convert(date, getdate()))
			AND EnvironmentID = 1                                                    
			AND FrequencyID = 4 
			AND EOMONTH(dateadd(day, -1, convert(date,getdate()))) = dateadd(day, -1, convert(date,getdate()))
			AND DATEPART(MONTH, dateadd(day, -1, GETDATE()))  = 12
		) RunPackage Order By FrequencyID, SequenceNo



		---Control package for EDW. Chnage Environment to 2
				SELECT PackageID, PackageName, SequenceNo, FrequencyID FROM (
					--These conditions must be true for my pipeline to run DAILY
			SELECT PackageID, PackageName, SequenceNo, FrequencyID FROM CTRL.Package
			Where (Active = 1 AND RunStartDate <= CONVERT(DATE, GETDATE()))
			AND (RunEndDate IS NULL OR RunEndDate >= convert(date, getdate()))
			AND EnvironmentID = 2 AND FrequencyID = 1

			UNION ALL 
					--Run the package WEEKLY.
			SELECT PackageID, PackageName, SequenceNo, FrequencyID FROM CTRL.Package
			Where (Active = 1 AND RunStartDate <= CONVERT(DATE, GETDATE()))
			AND (RunEndDate IS NULL OR RunEndDate >= convert(date, getdate()))
			AND EnvironmentID = 2 
			AND FrequencyID = 2 
			and DATEPART(WEEKDAY, DATEADD(DAY, -1, GETDATE())) = 7

			UNION ALL 
					--Run the package at the end of every month
			SELECT PackageID, PackageName, SequenceNo, FrequencyID FROM CTRL.Package
			Where (Active = 1 AND RunStartDate <= CONVERT(DATE, GETDATE()))
			AND (RunEndDate IS NULL OR RunEndDate >= convert(date, getdate()))
			AND EnvironmentID = 2 
			AND FrequencyID = 3
			AND EOMONTH(dateadd(day, -1, convert(date,getdate()))) = dateadd(day, -1, convert(date,getdate()))

			UNION ALL
			 ------Run the package at the end of the year.
			SELECT PackageID, PackageName, SequenceNo, FrequencyID FROM CTRL.Package
			Where (Active = 1 AND RunStartDate <= CONVERT(DATE, GETDATE()))
			AND (RunEndDate IS NULL OR RunEndDate >= convert(date, getdate()))
			AND EnvironmentID = 2                                                    
			AND FrequencyID = 4 
			AND EOMONTH(dateadd(day, -1, convert(date,getdate()))) = dateadd(day, -1, convert(date,getdate()))
			AND DATEPART(MONTH, dateadd(day, -1, GETDATE()))  = 12
		) RunPackage Order By FrequencyID, SequenceNo



--Select the end of month date for the date given
		SELECT EOMONTH('2024-12-01')
--Select the end of month date for yesterday's date
		SELECT EOMONTH(dateadd(day, -1, '2024-01-01'))
--Select yesterday's date
		SELECT dateadd(day, -1, convert(date,'2024-01-01'))
--Select month for yesterday's date
		SELECT DATEPART(MONTH, dateadd(day, -1, '2024-01-01'))


/*If we run the weekly package by 12am on Sunday, we have to check if yesterday equals Saturday, because if we run it on Saturday ny 12am, 
it'll run, not loading the data for that day. 
This is why we run it at 12am on Sunday; checking yesterday.




-- 04/29/2023
--3:06:01



		--SELECT EOMONTH(GETDATE())
		--SELECT MONTH(GETDATE()) as Month
		--SELECT DATEPART(MONTH, GETDATE())

		*/

sELECT * FROM TescaControl.ctrl.anomalies where Tablename = 'Absence Analysis'
AND columnname='cATEGORY ID'
order by RecordID


--SELECT * FROM TescaStaging.Staging.Overtime WHERE OvertimeID in ( Select recordID from TescaControl.ctrl.anomalies where tablename = 'Overtime Analysis')

SELECT * FROM TescaStaging.Staging.Absent_Analysis WHERE AbsentSk in (Select recordID from TescaControl.ctrl.anomalies where tablename = 'Absence Analysis' AND ColumnName='Category ID')
order by AbsentSk