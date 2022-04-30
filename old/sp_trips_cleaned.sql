--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--Server: SQL Server Management Studio 18
--Language: T-SQL
--Collation: SQL_Latin1_General_CP1_CI_AS
--Area of improvement: Use a merge procedure, insert only new records

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------


CREATE PROCEDURE sp.trips_cleaned   
AS   

    SET NOCOUNT ON;  

	truncate table agg.vw_trips; 

	with source as  (
		  select 
			  region, 
			  datetime,
			  YEAR(datetime)			as year_datetime,
			  datepart(week, datetime)	as week_datetime,
			  max(datasource)			as datasource,
			  1							as nbr_trips
			  FROM [jobsity].[dbo].[trips]
			  group by 
					region, 
					origin_coord,
					destination_coord,
					datetime
	)

	insert into agg.vw_trips
	(
		region,
		datetime,
		year_datetime,
		week_datetime,
		datasource,
		nbr_trips
	)
	select * from source;

	insert into log.trips_logging
	(
	 table_schema,
	 table_name,
	 datetime,
	 records
	)
		select 
			 'agg',
			 'vw_trips',
			 CURRENT_TIMESTAMP, 
			 count(*) 
		FROM [jobsity].[dbo].[trips];

	truncate table [jobsity].[dbo].[trips];

GO  