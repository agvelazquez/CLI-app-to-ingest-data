import pandas as pd
import plotly.express as px
from app.utils.dbengine import config_setup
from app.utils.database import change_database

def calculate_weekly_average():
    config_file = config_setup()
    engine = change_database()
    try:
        avg_calc = \
            """
            with avg_calc as (
            SELECT 
                  region
                , year_datetime
                , week_datetime
                , sum(cast(nbr_trips as decimal(12,5))) as total_trips
              FROM agg.{0}
              group by 
                    region
                  , year_datetime
                  , week_datetime
                            )
        
            SELECT 
                    region           as "Region"
                ,   avg(total_trips) as "Weekly Average Trips"
                from avg_calc
                group by 	  
                    region;
            """.format(config_file['aggregated_table'])

        sql_query = pd.read_sql_query(avg_calc, engine)

        # saving SQL table in a pandas data frame
        df = pd.DataFrame(sql_query)
        return df
    except:
        print("Error at executing query")


def create_visualization(df):
    try:
        fig = px.bar(df, x='Region', y="Weekly Average Trips", text_auto='.2s', title="Weekly Average Trips by Region")
        fig.update_traces(textfont_size=16, textangle=0, textposition="outside", cliponaxis=False)
        fig.update_layout(width=1000,
                          height=750,
                          font=dict(size=18),
                          xaxis={'categoryorder':'total descending'})
        fig.show()
    except:
        print("Error at executing plot")
