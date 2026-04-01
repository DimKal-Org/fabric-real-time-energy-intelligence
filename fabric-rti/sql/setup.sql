-- =============================================================================
-- Fabric Demo — Azure SQL Database CES Configuration
-- Execute each section in order.
-- =============================================================================

CREATE DATABASE [sql-fabric-rti];
GO

USE [master];
CREATE LOGIN sql_user WITH PASSWORD = 'YOUR PASSWORD HERE';
GO

USE [sql-fabric-rti];
GO
CREATE USER sql_user FROM LOGIN sql_user;
GO

CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'YOUR STRONGPASSWORD HERE';
GO

-- IMPORTANT: Make sure you have created a Shared Access policy first on an Event Hub instance
CREATE DATABASE SCOPED CREDENTIAL EventHubsCreds
    WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
    SECRET = '<YOUR SAS KEY HERE>';
GO

EXEC sys.sp_enable_event_stream
GO

EXEC sys.sp_create_event_stream_group
    @stream_group_name =      N'EventStreamToFabric',
    @destination_type =       N'AzureEventHubsApacheKafka',
    @destination_location =   N'myEventHubsNamespace.servicebus.windows.net:9093/myEventHubsInstance',
    @destination_credential = EventHubsCreds,
    @encoding = N'JSON';
    --@max_message_size_kb =    4048
    --@partition_key_scheme =   N'<PatitionKeyScheme>'
GO

CREATE TABLE dbo.EnergyReadings (
    ReadingId        UNIQUEIDENTIFIER NOT NULL DEFAULT NEWSEQUENTIALID(),
    Timestamp        DATETIME2(3)     NOT NULL,
    MachineId        NVARCHAR(50)     NOT NULL,
    PlantId          NVARCHAR(50)     NOT NULL DEFAULT 'PLANT-GR-01',
    LineId           NVARCHAR(50)     NOT NULL,
    ShiftId          NVARCHAR(20)     NOT NULL,
    PowerKw          FLOAT            NOT NULL,
    EnergyKwhCumul   FLOAT            NOT NULL,
    OeePercent       FLOAT            NOT NULL,
    ProductionUnits  BIGINT           NOT NULL,
    CostPerKwh       FLOAT            NOT NULL,
    CostThisTick     FLOAT            NOT NULL,
    CostPerUnit      FLOAT            NOT NULL,
    EventTag         NVARCHAR(50)     NULL
);
GO

-- Step 2: Add the table to the event stream group
EXEC sys.sp_add_object_to_event_stream_group
     N'EnergyReadingsStreamGroup',
     N'dbo.EnergyReadings'

-- Create a SQL login at the server level
USE [master];
CREATE LOGIN sql_user WITH PASSWORD = 'YOUR PASSWORD HERE';
GO

-- Create a database user mapped to the login
USE [sql-fabric-rti];
CREATE USER sql_user FROM LOGIN sql_user;
GO

-- Grant read/write access
ALTER ROLE db_datareader ADD MEMBER sql_user;
GO

ALTER ROLE db_datawriter ADD MEMBER sql_user;
GO

-- (Optional) Create an external user in the database mapped to the Service Principal
USE [sql-fabric-rti];
CREATE USER [your-app-registration-name] FROM EXTERNAL PROVIDER;
GO

ALTER ROLE db_datareader ADD MEMBER [your-app-registration-name];
ALTER ROLE db_datawriter ADD MEMBER [your-app-registration-name];
