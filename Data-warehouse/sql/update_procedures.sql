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
            AvgPriceDifference,
            Created_at
        )
        SELECT 
            f.DateKey,
            CAST(AVG(f.BuyPrice) AS DECIMAL(18,2)) AS AvgBuyPrice,
            CAST(MIN(f.BuyPrice) AS DECIMAL(18,2)) AS MinBuyPrice,
            CAST(MAX(f.BuyPrice) AS DECIMAL(18,2)) AS MaxBuyPrice,
            CAST(AVG(f.SellPrice) AS DECIMAL(18,2)) AS AvgSellPrice,
            CAST(MIN(f.SellPrice) AS DECIMAL(18,2)) AS MinSellPrice,
            CAST(MAX(f.SellPrice) AS DECIMAL(18,2)) AS MaxSellPrice,
            CAST(AVG(f.PriceDifference) AS DECIMAL(18,2)) AS AvgPriceDifference,
            GETDATE() AS Created_at
        FROM FactGoldPrices f
        GROUP BY f.DateKey
        ORDER BY f.DateKey;
        
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
            AvgPriceDifference,
            Created_at
        )
        SELECT 
            d.Year,
            d.Month,
            CAST(AVG(f.BuyPrice) AS DECIMAL(18,2)) AS AvgBuyPrice,
            CAST(MIN(f.BuyPrice) AS DECIMAL(18,2)) AS MinBuyPrice,
            CAST(MAX(f.BuyPrice) AS DECIMAL(18,2)) AS MaxBuyPrice,
            CAST(AVG(f.SellPrice) AS DECIMAL(18,2)) AS AvgSellPrice,
            CAST(MIN(f.SellPrice) AS DECIMAL(18,2)) AS MinSellPrice,
            CAST(MAX(f.SellPrice) AS DECIMAL(18,2)) AS MaxSellPrice,
            CAST(AVG(f.PriceDifference) AS DECIMAL(18,2)) AS AvgPriceDifference,
            GETDATE() AS Created_at
        FROM FactGoldPrices f
        JOIN DimDate d ON f.DateKey = d.DateKey
        GROUP BY d.Year, d.Month
        ORDER BY d.Year, d.Month;
        
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