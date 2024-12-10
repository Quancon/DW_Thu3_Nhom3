USE warehouse_db;
GO

-- Procedure để tạo daily aggregates
CREATE OR ALTER PROCEDURE sp_CreateDailyMart
AS
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Clear existing data
        TRUNCATE TABLE AggDailyGoldPrices;
        
        -- Insert daily aggregates
        INSERT INTO AggDailyGoldPrices (
            DateKey,
            AvgBuyPrice, MinBuyPrice, MaxBuyPrice,
            AvgSellPrice, MinSellPrice, MaxSellPrice,
            AvgPriceDifference
        )
        SELECT 
            CAST(FORMAT(UpdateTime, 'yyyyMMdd') AS INT) AS DateKey,
            AVG(BuyPrice) AS AvgBuyPrice,
            MIN(BuyPrice) AS MinBuyPrice,
            MAX(BuyPrice) AS MaxBuyPrice,
            AVG(SellPrice) AS AvgSellPrice,
            MIN(SellPrice) AS MinSellPrice,
            MAX(SellPrice) AS MaxSellPrice,
            AVG(SellPrice - BuyPrice) AS AvgPriceDifference
        FROM GoldPrices
        WHERE Is_deleted = 0
        GROUP BY CAST(FORMAT(UpdateTime, 'yyyyMMdd') AS INT);
        
        COMMIT;
        RETURN 0;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK;
        THROW;
        RETURN 1;
    END CATCH
END;
GO

-- Procedure để tạo monthly aggregates
CREATE OR ALTER PROCEDURE sp_CreateMonthlyMart
AS
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Clear existing data
        TRUNCATE TABLE AggMonthlyGoldPrices;
        
        -- Insert monthly aggregates
        INSERT INTO AggMonthlyGoldPrices (
            Year, Month,
            AvgBuyPrice, MinBuyPrice, MaxBuyPrice,
            AvgSellPrice, MinSellPrice, MaxSellPrice,
            AvgPriceDifference
        )
        SELECT 
            YEAR(UpdateTime) AS Year,
            MONTH(UpdateTime) AS Month,
            AVG(BuyPrice) AS AvgBuyPrice,
            MIN(BuyPrice) AS MinBuyPrice,
            MAX(BuyPrice) AS MaxBuyPrice,
            AVG(SellPrice) AS AvgSellPrice,
            MIN(SellPrice) AS MinSellPrice,
            MAX(SellPrice) AS MaxSellPrice,
            AVG(SellPrice - BuyPrice) AS AvgPriceDifference
        FROM GoldPrices
        WHERE Is_deleted = 0
        GROUP BY YEAR(UpdateTime), MONTH(UpdateTime);
        
        COMMIT;
        RETURN 0;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK;
        THROW;
        RETURN 1;
    END CATCH
END;
GO 