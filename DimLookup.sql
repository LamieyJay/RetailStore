USE TESCAEDW

SELECT dateSK, BusinessDate FROM EDW.DimDate ORDER BY BusinessDate 
SELECT categorysk, categoryid FROM EDW.DimAbsence
SELECT CustomerSK, CustomerID FROM EDW.DimCustomer
SELECT DecisionSK, Decision_id FROM EDW.DimDecision
SELECT EmployeeSK, EmployeeID FROM EDW.DimEmployee
SELECT EmployeeSK, EmployeeNo FROM EDW.DimEmployee
SELECT MisconductSK, MisconductID FROM EDW.DimMisconduct
SELECT ChannelSK, ChannelID FROM EDW.DimPOSChannel
SELECT ProductSK, ProductID FROM EDW.DimProduct
SELECT PromotionSK, PromotionID FROM EDW.DimPromotion
SELECT StoreSK, StoreID FROM EDW.DimStore
SELECT Timesk, TimeHour FROM EDW.DimTime
SELECT VendorSK, VendorID FROM EDW.DimVendor

