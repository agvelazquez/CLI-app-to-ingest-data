import pandas as pd
from tqdm import tqdm
from dbengine import engine_setup


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
    try:
        df = pd.read_csv(r'../data/trips.csv', encoding='utf-8')
        engine = engine_setup()
        insert_with_progress(df, engine)
        print('Data uploaded successfully')

        try:
            engine.execute(
                """
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
                """

            )
            print('Aggregated table created successfully')
        except:
            print('Error at aggregating table')

    except:
        print('Error loading file. Check expected schema. File extension should be .csv')
