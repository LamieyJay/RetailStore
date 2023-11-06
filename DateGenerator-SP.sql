--Create Stored procedure to make the @enddate dynamic.
--DROP PROCEDURE EDW.DATAGENERATOR

CREATE OR ALTER PROCEDURE EDW.DateGenerator(@Enddate date)
AS
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
declare @noofDays int = DATEDIFF(day, @StartDate, @endDate)
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