--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--Server: SQL Server Management Studio 18
--Language: T-SQL
--Collation: SQL_Latin1_General_CP1_CI_AS
--Area of improvement: Partition agg table by year

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------


  IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = 'jobsity')
      EXEC('CREATE DATABASE jobsity');

USE jobsity
GO

IF NOT EXISTS ( SELECT  *
                FROM    sys.schemas
                WHERE   name = N'stg' )
    EXEC('CREATE SCHEMA [stg]');
GO

IF NOT EXISTS ( SELECT  *
                FROM    sys.schemas
                WHERE   name = N'agg' )
    EXEC('CREATE SCHEMA [agg]');
GO

IF NOT EXISTS ( SELECT  *
                FROM    sys.schemas
                WHERE   name = N'sp' )
    EXEC('CREATE SCHEMA [sp]');
GO

IF NOT EXISTS ( SELECT  *
                FROM    sys.schemas
                WHERE   name = N'log' )
    EXEC('CREATE SCHEMA [log]');
GO

DROP TABLE IF EXISTS stg.trips;

CREATE TABLE stg.trips
(
	region VARCHAR(255),
	origin_coord VARCHAR(255),
	destination_coord VARCHAR(255),
	datetime VARCHAR(255),
	datasource VARCHAR(255)
)

DROP TABLE IF EXISTS agg.vw_trips;

CREATE TABLE agg.vw_trips
(
	trip_id int IDENTITY(1,1) PRIMARY KEY,
	region VARCHAR(255) NOT NULL,
	datetime datetime NOT NULL,
	year_datetime bigint NOT NULL,
	week_datetime int NOT NULL,
	datasource VARCHAR(255) NOT NULL, 
	nbr_trips int NOT NULL
)


DROP TABLE IF EXISTS log.trips_logging;

CREATE TABLE log.trips_logging
(
	id int IDENTITY(1,1) PRIMARY KEY,
	table_schema VARCHAR(3) NOT NULL,
	table_name VARCHAR(50) NOT NULL,
	datetime datetime NOT NULL,
	records bigint NOT NULL
)

select * from sys.tables
where name = 'trips' and type = 'U'
