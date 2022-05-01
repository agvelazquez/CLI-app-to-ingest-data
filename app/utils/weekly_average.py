import pandas as pd
import plotly.express as px
from dbengine import engine_setup

def calculate_weekly_average():
    engine = engine_setup()
    try:
        sql_query = pd.read_sql_query(
            """
            with avg_calc as (
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
                    region           as Region
                ,   avg(total_trips) as "Weekly Average Trips"
                from avg_calc
                group by 	  
                    region;
            """, engine)

        # saving SQL table in a pandas data frame
        df = pd.DataFrame(sql_query)
        return df
    except:
        print("Error at executing query")


def create_visualization(df):
    try:
        fig = px.bar(df, x='Region', y="Weekly Average Trips")
        fig.update_layout(width=1000, height=750)
        fig.show()
    except:
        print("Error at executing plot")
