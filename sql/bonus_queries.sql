--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--1-- Alternative to calculation made in the Tableau dahsboard to the Weekly average by region
--2-- From the two most commonly appearing regions, which is the latest datasource?
--3-- What regions has the "cheap_mobile" datasource appeared in?

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

		--1--
with avg_calc as (
	-- total trips by week
	SELECT 
		  region
		, year_datetime
		, week_datetime
		, sum(nbr_trips) total_trips
	  FROM [jobsity].[agg].[vw_trips]
	  group by 
		  region
		  , year_datetime
		  , week_datetime
	)

SELECT 
	--weekly average by region
	 region
	,avg(total_trips) avg_trips
	from avg_calc
	group by 	  
		region;



			--2--
with top_region as (
	SELECT 
		  region
		, sum(nbr_trips) total_trips
		, ROW_NUMBER () OVER ( order by sum(nbr_trips) desc) top_region_rn
	  FROM [jobsity].[agg].[vw_trips]
	  group by 
		  region
	)


, latest_datasource as (
select   region 
	   , datasource
	   , datetime
	   , ROW_NUMBER () OVER (partition by region order by datetime desc) rn
from [jobsity].[agg].[vw_trips]
where region in (select region from top_region where top_region_rn in (1,2))
)

Select region, datasource
from latest_datasource
where rn = 1


			--3--
SELECT DISTINCT  region
FROM [jobsity].[agg].[vw_trips]
WHERE datasource = 'cheap_mobile'
