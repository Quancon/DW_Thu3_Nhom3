CREATE DATABASE staging_db;
GO

USE staging_db;
GO

DROP TABLE IF EXISTS GoldPrices;
GO
CREATE TABLE GoldPrices (
    gold_id INT IDENTITY(1,1) PRIMARY KEY,
    GoldType NVARCHAR(255) NOT NULL,
    BuyPrice FLOAT NULL,
    SellPrice FLOAT NULL,
    UpdateTime DATETIME NULL
);
GO

DROP TABLE IF EXISTS GoldPrices_temp;
GO
CREATE TABLE GoldPrices_temp (
    gold_id INT IDENTITY(1,1) PRIMARY KEY,
    GoldType NVARCHAR(255) NOT NULL,
    BuyPrice FLOAT NULL,
    SellPrice FLOAT NULL,
    UpdateTime DATETIME NULL
);
GO