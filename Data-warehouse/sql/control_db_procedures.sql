USE control_db;
GO

-- Procedure để thêm mới hoặc cập nhật job
CREATE OR ALTER PROCEDURE sp_UpsertJob
    @job_name VARCHAR(100),
    @description VARCHAR(500),
    @source_type VARCHAR(50),
    @is_active BIT = 1
AS
BEGIN
    IF EXISTS (SELECT 1 FROM ETL_Jobs WHERE job_name = @job_name)
    BEGIN
        UPDATE ETL_Jobs
        SET description = @description,
            source_type = @source_type,
            is_active = @is_active,
            last_modified = GETDATE()
        WHERE job_name = @job_name;
    END
    ELSE
    BEGIN
        INSERT INTO ETL_Jobs (job_name, description, source_type, is_active)
        VALUES (@job_name, @description, @source_type, @is_active);
    END
END;
GO

-- Procedure để thêm dependency giữa các jobs
CREATE OR ALTER PROCEDURE sp_AddJobDependency
    @dependent_job_name VARCHAR(100),
    @depends_on_job_name VARCHAR(100)
AS
BEGIN
    DECLARE @dependent_job_id INT, @depends_on_job_id INT;

    -- Lấy job IDs
    SELECT @dependent_job_id = job_id FROM ETL_Jobs WHERE job_name = @dependent_job_name;
    SELECT @depends_on_job_id = job_id FROM ETL_Jobs WHERE job_name = @depends_on_job_name;

    IF @dependent_job_id IS NULL OR @depends_on_job_id IS NULL
        THROW 50001, 'One or both jobs not found', 1;

    -- Kiểm tra dependency đã tồn tại chưa
    IF NOT EXISTS (
        SELECT 1 
        FROM Job_Dependencies 
        WHERE job_id = @dependent_job_id AND depends_on = @depends_on_job_id
    )
    BEGIN
        INSERT INTO Job_Dependencies (job_id, depends_on)
        VALUES (@dependent_job_id, @depends_on_job_id);
    END
END;
GO

-- Procedure để thêm lịch chạy cho job
CREATE OR ALTER PROCEDURE sp_AddJobSchedule
    @job_name VARCHAR(100),
    @schedule_type VARCHAR(50),
    @schedule_time TIME,
    @is_active BIT = 1
AS
BEGIN
    DECLARE @job_id INT;
    SELECT @job_id = job_id FROM ETL_Jobs WHERE job_name = @job_name;

    IF @job_id IS NULL
        THROW 50001, 'Job not found', 1;

    IF EXISTS (SELECT 1 FROM Job_Schedule WHERE job_id = @job_id)
    BEGIN
        UPDATE Job_Schedule
        SET schedule_type = @schedule_type,
            schedule_time = @schedule_time,
            is_active = @is_active,
            last_modified = GETDATE()
        WHERE job_id = @job_id;
    END
    ELSE
    BEGIN
        INSERT INTO Job_Schedule (job_id, schedule_type, schedule_time, is_active)
        VALUES (@job_id, @schedule_type, @schedule_time, @is_active);
    END
END;
GO

-- Procedure để cấu hình thông báo cho job
CREATE OR ALTER PROCEDURE sp_ConfigureJobNotification
    @job_name VARCHAR(100),
    @notification_type VARCHAR(50),
    @email_recipient VARCHAR(255) = NULL,
    @slack_channel VARCHAR(100) = NULL,
    @notify_on_success BIT = 0,
    @notify_on_failure BIT = 1
AS
BEGIN
    DECLARE @job_id INT;
    SELECT @job_id = job_id FROM ETL_Jobs WHERE job_name = @job_name;

    IF @job_id IS NULL
        THROW 50001, 'Job not found', 1;

    IF EXISTS (SELECT 1 FROM Notification_Config WHERE job_id = @job_id)
    BEGIN
        UPDATE Notification_Config
        SET notification_type = @notification_type,
            email_recipient = @email_recipient,
            slack_channel = @slack_channel,
            notify_on_success = @notify_on_success,
            notify_on_failure = @notify_on_failure
        WHERE job_id = @job_id;
    END
    ELSE
    BEGIN
        INSERT INTO Notification_Config (
            job_id, notification_type, email_recipient, 
            slack_channel, notify_on_success, notify_on_failure
        )
        VALUES (
            @job_id, @notification_type, @email_recipient,
            @slack_channel, @notify_on_success, @notify_on_failure
        );
    END
END;
GO

-- Procedure để thiết lập cấu hình ETL ban đầu
CREATE OR ALTER PROCEDURE sp_InitializeETLConfiguration
AS
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;

        -- Thêm các jobs
        EXEC sp_UpsertJob 'extract_pnj', 'Extract data from PNJ website', 'WEB';
        EXEC sp_UpsertJob 'extract_csv', 'Extract data from CSV files', 'CSV';
        EXEC sp_UpsertJob 'load_staging', 'Load data to staging database', NULL;
        EXEC sp_UpsertJob 'transform_gold_data', 'Transform extracted gold price data', NULL;
        EXEC sp_UpsertJob 'load_warehouse', 'Load data to warehouse', NULL;
        EXEC sp_UpsertJob 'create_daily_mart', 'Create daily aggregates', NULL;
        EXEC sp_UpsertJob 'create_monthly_mart', 'Create monthly aggregates', NULL;

        -- Thêm dependencies
        EXEC sp_AddJobDependency 'load_staging', 'extract_pnj';
        EXEC sp_AddJobDependency 'load_staging', 'extract_csv';
        EXEC sp_AddJobDependency 'transform_gold_data', 'load_staging';
        EXEC sp_AddJobDependency 'load_warehouse', 'transform_gold_data';
        EXEC sp_AddJobDependency 'create_daily_mart', 'load_warehouse';
        EXEC sp_AddJobDependency 'create_monthly_mart', 'load_warehouse';

        -- Thêm lịch chạy
        EXEC sp_AddJobSchedule 'extract_pnj', 'DAILY', '08:00:00';
        EXEC sp_AddJobSchedule 'extract_csv', 'DAILY', '09:00:00';
        EXEC sp_AddJobSchedule 'load_staging', 'DAILY', '10:00:00';
        EXEC sp_AddJobSchedule 'transform_gold_data', 'DAILY', '11:00:00';
        EXEC sp_AddJobSchedule 'load_warehouse', 'DAILY', '12:00:00';
        EXEC sp_AddJobSchedule 'create_daily_mart', 'DAILY', '13:00:00';
        EXEC sp_AddJobSchedule 'create_monthly_mart', 'DAILY', '14:00:00';

        -- Thêm cấu hình thông báo
        EXEC sp_ConfigureJobNotification 'extract_pnj', 'EMAIL', 'admin@example.com';
        EXEC sp_ConfigureJobNotification 'transform_gold_data', 'EMAIL', 'admin@example.com';
        EXEC sp_ConfigureJobNotification 'load_warehouse', 'EMAIL', 'admin@example.com';

        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK;
        THROW;
    END CATCH
END;
GO 