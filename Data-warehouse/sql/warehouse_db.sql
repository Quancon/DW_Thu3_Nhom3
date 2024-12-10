CREATE DATABASE warehouse_db;
GO

USE warehouse_db;
GO

DROP TABLE IF EXISTS GoldPrices;
GO
CREATE TABLE GoldPrices (
    gold_id INT IDENTITY(1,1) PRIMARY KEY,
    GoldType NVARCHAR(255) NOT NULL,
    BuyPrice FLOAT NULL,
    SellPrice FLOAT NULL,
    UpdateTime DATETIME NULL,
    Expired_Date DATETIME NULL,
    Is_deleted BIT NOT NULL DEFAULT 0,
    Created_at DATETIME DEFAULT GETDATE() NOT NULL,
    Updated_at DATETIME NULL
);
GO

-- Date Dimension
CREATE TABLE DimDate (
    DateKey INT PRIMARY KEY,
    Date DATE,
    Year INT,
    Month INT,
    Day INT,
    Quarter INT,
    Created_at DATETIME DEFAULT GETDATE()
);

-- Gold Type Dimension
CREATE TABLE DimGoldType (
    GoldTypeKey INT IDENTITY(1,1) PRIMARY KEY,
    GoldType NVARCHAR(255),
    Created_at DATETIME DEFAULT GETDATE()
);

-- Fact Table
CREATE TABLE FactGoldPrices (
    FactID INT IDENTITY(1,1) PRIMARY KEY,
    GoldTypeKey INT FOREIGN KEY REFERENCES DimGoldType(GoldTypeKey),
    DateKey INT FOREIGN KEY REFERENCES DimDate(DateKey),
    BuyPrice DECIMAL(18,2),
    SellPrice DECIMAL(18,2),
    PriceDifference DECIMAL(18,2),
    PriceDifferencePercentage DECIMAL(18,2),
    Created_at DATETIME DEFAULT GETDATE()
);

-- Aggregate Tables
CREATE TABLE AggDailyGoldPrices (
    DateKey INT PRIMARY KEY,
    AvgBuyPrice DECIMAL(18,2),
    MinBuyPrice DECIMAL(18,2),
    MaxBuyPrice DECIMAL(18,2),
    AvgSellPrice DECIMAL(18,2),
    MinSellPrice DECIMAL(18,2),
    MaxSellPrice DECIMAL(18,2),
    AvgPriceDifference DECIMAL(18,2),
    Created_at DATETIME DEFAULT GETDATE()
);

CREATE TABLE AggMonthlyGoldPrices (
    Year INT,
    Month INT,
    AvgBuyPrice DECIMAL(18,2),
    MinBuyPrice DECIMAL(18,2),
    MaxBuyPrice DECIMAL(18,2),
    AvgSellPrice DECIMAL(18,2),
    MinSellPrice DECIMAL(18,2),
    MaxSellPrice DECIMAL(18,2),
    AvgPriceDifference DECIMAL(18,2),
    Created_at DATETIME DEFAULT GETDATE(),
    PRIMARY KEY (Year, Month)
);
