
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


declare @PackageID int = ?
declare @StgSourceCount int =?
declare @StgDesCount int =?
INSERT INTO CTRL.metrics (PackageID, StgSourceCount, StgDesCount, RunDate)
values (@PackageID, @StgSourceCount, @StgDesCount, getdate())

Update ctrl.package
set LastRunDate=getdate() where packageid = @PackageID



select * from ctrl.Package
Select * from ctrl.metrics
SELECT * FROM ctrl.Frequency
	/*
	Daily - runs everyday
	Weekly - Runs at the en dof every week
	Monthly - At the end of every month
	Yearly - At the end of every year
	*/

			--These conditions must be true for my pipeline to run 
		SELECT PackageID, PackageName, SequenceNo FROM CTRL.Package
		Where (Active = 1 AND RunStartDate <= CONVERT(DATE, GETDATE()))
		AND (RunEndDate IS NULL OR RunEndDate >= convert(date, getdate()))
		AND EnvironmentID = 1



-- 04/15/2023 