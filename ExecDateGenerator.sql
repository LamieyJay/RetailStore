exec EDW.DateGenerator '3000-01-01'

SELECT * FROM EDW.DimDate
order by datesk desc

SELECT max(Businessdate), min (Businessdate), count(*) from edw.dimdate

SELECT DATEDIFF(day,'2016-01-01', '3000-01-01')+1	