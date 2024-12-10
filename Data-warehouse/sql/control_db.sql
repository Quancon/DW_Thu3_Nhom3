CREATE DATABASE control_db;
GO

USE control_db;
GO

-- Bảng quản lý các job ETL
CREATE TABLE ETL_Jobs (
    job_id INT IDENTITY(1,1) PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL,
    description VARCHAR(500),
    source_type VARCHAR(50), -- 'WEB', 'CSV', 'EXCEL' etc
    is_active BIT DEFAULT 1,
    created_at DATETIME DEFAULT GETDATE(),
    last_modified DATETIME DEFAULT GETDATE()
);

-- Bảng theo dõi trạng thái của từng lần chạy job
CREATE TABLE Job_Status (
    status_id INT IDENTITY(1,1) PRIMARY KEY,
    job_id INT FOREIGN KEY REFERENCES ETL_Jobs(job_id),
    start_time DATETIME DEFAULT GETDATE(),
    end_time DATETIME,
    status VARCHAR(50), -- 'RUNNING', 'SUCCESS', 'FAILED', 'PENDING'
    records_processed INT DEFAULT 0,
    error_message VARCHAR(MAX),
    created_at DATETIME DEFAULT GETDATE()
);

-- Bảng quản lý dependencies giữa các job
CREATE TABLE Job_Dependencies (
    dependency_id INT IDENTITY(1,1) PRIMARY KEY,
    job_id INT FOREIGN KEY REFERENCES ETL_Jobs(job_id),
    depends_on INT FOREIGN KEY REFERENCES ETL_Jobs(job_id),
    created_at DATETIME DEFAULT GETDATE()
);

-- Bảng logs chi tiết
CREATE TABLE Logs (
    log_id INT IDENTITY(1,1) PRIMARY KEY,
    job_id INT FOREIGN KEY REFERENCES ETL_Jobs(job_id),
    status_id INT FOREIGN KEY REFERENCES Job_Status(status_id),
    message VARCHAR(MAX),
    level VARCHAR(50),
    created_at DATETIME DEFAULT GETDATE()
);

-- Bảng quản lý schedule
CREATE TABLE Job_Schedule (
    schedule_id INT IDENTITY(1,1) PRIMARY KEY,
    job_id INT FOREIGN KEY REFERENCES ETL_Jobs(job_id),
    schedule_type VARCHAR(50), -- 'DAILY', 'WEEKLY', 'MONTHLY'
    schedule_time TIME,
    is_active BIT DEFAULT 1,
    created_at DATETIME DEFAULT GETDATE(),
    last_modified DATETIME DEFAULT GETDATE()
);

-- Bảng cấu hình notification
CREATE TABLE Notification_Config (
    config_id INT IDENTITY(1,1) PRIMARY KEY,
    job_id INT FOREIGN KEY REFERENCES ETL_Jobs(job_id),
    notification_type VARCHAR(50), -- 'EMAIL', 'SLACK'
    email_recipient VARCHAR(255),
    slack_channel VARCHAR(100),
    notify_on_success BIT DEFAULT 0,
    notify_on_failure BIT DEFAULT 1,
    created_at DATETIME DEFAULT GETDATE()
);

-- Bảng notification history
CREATE TABLE Job_Notifications (
    notification_id INT IDENTITY(1,1) PRIMARY KEY,
    job_id INT FOREIGN KEY REFERENCES ETL_Jobs(job_id),
    status_id INT FOREIGN KEY REFERENCES Job_Status(status_id),
    notification_type VARCHAR(50),
    recipient VARCHAR(255),
    message VARCHAR(MAX),
    sent_at DATETIME DEFAULT GETDATE(),
    is_sent BIT DEFAULT 0
);

-- Insert sample ETL jobs
INSERT INTO ETL_Jobs (job_name, description, source_type) VALUES
('extract_pnj', 'Extract data from PNJ website', 'WEB'),
('extract_csv', 'Extract data from CSV files', 'CSV'),
('extract_excel', 'Extract data from Excel files', 'EXCEL'),
('load_staging', 'Load data to staging database', NULL),
('transform_gold_data', 'Transform extracted gold price data', NULL),
('load_warehouse', 'Load data to warehouse', NULL),
('create_daily_mart', 'Create daily aggregates', NULL),
('create_monthly_mart', 'Create monthly aggregates', NULL);

-- Insert dependencies
INSERT INTO Job_Dependencies (job_id, depends_on) VALUES
(4, 1), -- load_staging depends on web extract
(4, 2), -- load_staging depends on csv extract
(4, 3), -- load_staging depends on excel extract
(5, 4), -- transform depends on load_staging
(6, 5), -- warehouse load depends on transform
(7, 6), -- daily mart depends on warehouse load
(8, 6); -- monthly mart depends on warehouse load

-- Insert sample schedules
INSERT INTO Job_Schedule (job_id, schedule_type, schedule_time) VALUES
(1, 'DAILY', '08:00:00'), -- PNJ extract runs daily at 8 AM
(2, 'DAILY', '09:00:00'), -- CSV extract runs daily at 9 AM
(3, 'WEEKLY', '10:00:00'), -- Excel extract runs weekly
(4, 'DAILY', '10:30:00'), -- Load staging runs at 10:30 AM
(5, 'DAILY', '11:00:00'), -- Transform runs daily at 11 AM
(6, 'DAILY', '12:00:00'), -- Warehouse load runs daily at noon
(7, 'DAILY', '13:00:00'), -- Daily mart runs at 1 PM
(8, 'DAILY', '14:00:00'); -- Monthly mart runs at 2 PM

-- Insert sample notification config
INSERT INTO Notification_Config (job_id, notification_type, email_recipient, notify_on_failure) VALUES
(1, 'EMAIL', 'admin@example.com', 1),
(4, 'EMAIL', 'admin@example.com', 1),
(5, 'EMAIL', 'admin@example.com', 1),
(6, 'EMAIL', 'admin@example.com', 1);