import pandas as pd
from tqdm import tqdm
from app.utils.dbengine import engine_setup, config_setup


def insert_with_progress(pathfile, engine, table_destination):
    """
    Upload csv file to landing table with a progression bar
    :param df: csv file
    :param engine: SQL alchemy engine
    :param table_destination: landing table in DB
    """
    chunksize = 100
    df = pd.read_csv(pathfile, encoding='utf-8', chunksize=chunksize)

    with tqdm(total=1000000) as pbar:
        for i, cdf in enumerate(df):
            replace = "replace" if i == 0 else "append"
            cdf.to_sql(con=engine,
                       schema='stg',
                       name=table_destination,
                       if_exists=replace,
                       index=False)
            pbar.update(i)


def load():
    """
    Load csv file to staging table.
    Load aggregated table grouping trips with similar origin,destination and time
    """
    config_file = config_setup()
    try:
        pathfile = config_file['input_folder']+'/'+config_file['filename']
        table_destination = config_file['staging_table']
        engine = engine_setup()
        insert_with_progress(pathfile, engine, table_destination)
        print('Data uploaded successfully')

        try:
            vw_query = \
                """
                truncate table agg.{0}; 

                with source as  (
                      select 
                          region, 
                          datetime,
                          YEAR(datetime)			as year_datetime,
                          datepart(week, datetime)	as week_datetime,
                          max(datasource)			as datasource,
                          1							as nbr_trips
                          FROM [{2}].[stg].[{1}]
                          group by 
                                region, 
                                origin_coord,
                                destination_coord,
                                datetime
                )

                insert into agg.{0}
                (
                    region,
                    datetime,
                    year_datetime,
                    week_datetime,
                    datasource,
                    nbr_trips
                )
                select * from source;
                """.format(config_file['aggregated_table'], config_file['staging_table'], config_file['dbname'])
            engine.execute(vw_query)
            print('Aggregated table created successfully')
        except:
            print('Error at aggregating table')

    except:
        print('Error loading file. Check expected schema. File extension should be .csv')
