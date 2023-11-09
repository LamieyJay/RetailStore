--Rename Column name              
EXEC sp_rename 'EDW.Dimdate.FrecnhMonth', 'FrenchMonth', 'COLUMN';

--Counter Basics
declare @counter int = 1

WHILE @Counter <=100
BEGIN
	Print @counter 
	SELECT @counter = @Counter+1
END
print('end of counter')

SELECT DATEADD(day, 100, getdate())

--CREATING OUR OWN DATE data
Declare @newday int = 1
WHILE @newday < 100
BEGIN
	Print DATEADD(day, @newday, getdate())
	Select @newday = @newday + 1 
END
print '100 days added'
--------------------------------------------------------------------------------

--DATE QUARTER 
select datepart(q, '12-12-2023')
---------------------------------------------------------------------------------
 
-- Create the EDW table with all dates 
USE TescaEDW
create table EDW.DimDate 
(
	dateSk int,
	BusinessDate date,
	BusinessYear int,
	BusinessMonth int,
	BusinessQuarter nvarchar(2),
	EnglishMonth nvarchar(50),
	SpanishMonth nvarchar(50),
	FrecnhMonth nvarchar(50),
	EnglishDayofWeek nvarchar(50),
	SpanishDayofWeek nvarchar(50),
	FrenchDayofWeek nvarchar(50),
	LoadDate datetime default getdate(),
	constraint edw_dimdate_sk primary key (datesk)
)

 
------------------------------------------------------------------------------------

/*Get the date to start your EDW data from. i.e The date the company 
operations started.*/
SELECT min (convert(date,MinDate)) FROM 
	(
 	SELECT min(transdate) MinDate FROM TescaOLTP.dbo.PurchaseTransaction
	UNION ALL
	SELECT min(transdate) FROM TescaOLTP.dbo.SalesTransaction
	) a
------------------------------------------------------------------------------------

-- Printing the dates from the EDW start date to the end date 
/*declare @currentdate int = 0
WHILE @currentdate <= @noofDays
BEGIN
	print(dateadd(day, @currentdate, @startdate))
	SELECT @currentdate = @currentdate+1
END*/
------------------------------------------------------------------------------------
BEGIN
SET NOCOUNT ON 
declare @StartDate date = (
		SELECT min (convert(date,MinDate)) FROM 
		(
 			SELECT min(transdate) MinDate FROM TescaOLTP.dbo.PurchaseTransaction
			UNION ALL
			SELECT min(transdate) FROM TescaOLTP.dbo.SalesTransaction
		) a
	)

--Number of days between the start date of the data and the dummy @end date
declare @enddate date = '2090-12-31'
declare @noofDays int = DATEDIFF(day, @StartDate, @endDate) ---- 27393

--Insert data into the EDW.Dimdate table
declare @currentday int = 0
declare @currentdate date 

IF (SELECT OBJECT_ID('EDW.DIMDATE')) IS NOT NULL
TRUNCATE TABLE EDW.DimDate

WHILE @currentday <= @noofDays ----- 0 <= 27393
BEGIN
	SELECT @currentdate = (dateadd(day, @currentday, @startdate))

	insert into EDW.DimDate(dateSk, BusinessDate, BusinessYear, BusinessMonth, 
	BusinessQuarter, EnglishMonth, EnglishDayofWeek, SpanishMonth, SpanishDayofWeek, FrenchMonth,  
	 FrenchDayofWeek, LoadDate)

		select convert (int, convert(nvarchar(8), @currentdate, 112)) as DateSk,
		@currentdate,
		year(@currentdate),
		month(@currentdate),
		'Q' + CAST(DATEPART(Q, @Currentdate) AS nvarchar), ----OR SELECT concat ('Q', datepart(q, getdate()))
		DATENAME(month, @currentdate),
		DATENAME(dw, @currentdate),
		CASE DATEPART(Month, @CurrentDate)
			WHEN 1 then 'Enero' WHEN 2 then 'Febrero' WHEN 3 then 'Marzo' WHEN 4 then 'Abril'
			WHEN 5 THEN 'Mayo' WHEN 6 THEN 'Junio' WHEN 7 THEN 'Julio' WHEN 8 THEN 'Agosto' 
			WHEN 9 THEN 'Septiembre' WHEN 10 THEN 'Octubre' WHEN 11 THEN 'Noviembre' WHEN 12 THEN 
			'Diciembre'
		END,
		CASE DATEPART (Weekday, @currentdate)
			WHEN 1 THEN 'Domingo' WHEN 2 THEN 'Lunes' WHEN 3 THEN 'Martes' WHEN 4 THEN 'Miercoles'
			WHEN 5 THEN 'Jueves' WHEN 6 THEN 'Viernes' WHEN 7 THEN 'Sabado'
		END,	
		CASE DATEPART(Month, @CurrentDate)
			WHEN 1 then 'Janvier' WHEN 2 then 'Février' WHEN 3 then 'Mars' WHEN 4 then 'Avril'
			WHEN 5 THEN 'Mai' WHEN 6 THEN 'Juin' WHEN 7 THEN 'Juillet' WHEN 8 THEN 'août' 
			WHEN 9 THEN 'Septembre' WHEN 10 THEN 'Octobre' WHEN 11 THEN 'Novembre' WHEN 12 THEN 
			'Décembre'
		END,
		CASE DATEPART (Weekday, @currentdate)
			WHEN 1 THEN 'Lundi' WHEN 2 THEN 'Mardi' WHEN 3 THEN 'Mercredi' WHEN 4 THEN 'Jeudi'
			WHEN 5 THEN 'Vendredi' WHEN 6 THEN 'Samedi' WHEN 7 THEN 'Dimanche'
		END,
		getdate()
	SELECT @currentday=@currentday+1
END
END