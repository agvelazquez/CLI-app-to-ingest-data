import pandas as pd
from tqdm import tqdm
from app.utils.dbengine import engine_setup, config_setup

def chunker(seq, size):
    # from http://stackoverflow.com/a/434328
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def insert_with_progress(df, engine):
    chunksize = int(len(df) / 10)  # 10%
    with tqdm(total=len(df)) as pbar:
        for i, cdf in enumerate(chunker(df, chunksize)):
            replace = "replace" if i == 0 else "append"
            cdf.to_sql(con=engine,
                       schema='stg',
                       name="trips",
                       if_exists=replace,
                       index=False)
            pbar.update(chunksize)


def load():
    config_file = config_setup()
    try:
        pathfile = config_file['input_folder']+'/'+config_file['filename']
        df = pd.read_csv(pathfile, encoding='utf-8')
        engine = engine_setup()
        insert_with_progress(df, engine)
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
