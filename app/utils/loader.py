import random
import pandas as pd
import pandera as pa
import os
from tqdm import tqdm
from app.utils.dbengine import config_setup
from app.utils.database import change_database
from sqlalchemy import text


def insert_with_progress(pathfile, engine, table_destination):
    """
    Upload csv file to landing table with a progression bar
    :param df: csv file
    :param engine: SQL alchemy engine
    :param table_destination: landing table in DB
    """
    chunksize = 10000
    df = pd.read_csv(pathfile, encoding='utf-8', chunksize=chunksize)

    total_rows = (sum(1 for row in open(pathfile, 'r')) - 1)

    with tqdm(total=total_rows) as pbar:
        for i, cdf in enumerate(df):
            replace = "replace" if i == 0 else "append"
            cdf.to_sql(con=engine,
                       schema='stg',
                       name=table_destination,
                       if_exists="append",
                       index=False)
            pbar.update(len(cdf))


def validate_extension(folderpath):
    files = [a for a in os.listdir(folderpath) if a.endswith('.csv')]
    if len(files) == 0:
        raise Exception('No .csv files in the folder')
    else:
        return files


def validate_schema(folderpath, config_file, engine):
    valid_files = validate_extension(folderpath)
    schema = pa.DataFrameSchema({
        'region': pa.Column(pa.Object),
        'origin_coord': pa.Column(pa.Object),
        'destination_coord': pa.Column(pa.Object),
        'datetime': pa.Column(pa.Object),
        'datasource': pa.Column(pa.Object)
    })
    schema_valid_files = []
    for file in valid_files:
        p = 0.2
        total_rows = (sum(1 for row in open(folderpath + '/' + file, 'r')) - 1)
        if total_rows > 100:
            df = pd.read_csv(folderpath + '/' + file,
                             header=0,
                             skiprows=lambda i: i > 0 and random.random() > p)  # take a sample of the file
        else:
            df = pd.read_csv(folderpath + '/' + file, header=0)
        try:
            schema.validate(df)
            df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d %H:%M:%S')
            print('Schema validated for {0}'.format(file))
            schema_valid_files.append(file)
        except:
            print('Error validating schema for {0}'.format(file))
            log_stg_query = """
                    insert into log.{0} (table_schema, table_name, filename, datetime, records, message)
                    select 'stg', '{1}', '{2}', CURRENT_TIMESTAMP, 0, 'Error validating schema';
                    """.format(config_file['logging_table'], config_file['staging_table'], file)

            engine.execute(text(log_stg_query).execution_options(autocommit=True))

    if len(schema_valid_files) == 0:
        raise RuntimeError('None of the files where validated')
    else:
        return schema_valid_files

def load_to_agg(config_file, engine, f):
    try:
        vw_query = \
            """
            with source as  (
                  select 
                      region, 
                      cast(datetime as timestamp without time zone) 	as datetime,
                      extract(YEAR from CAST(datetime as DATE))			as year_datetime,
                      extract(WEEK from CAST(datetime as DATE))			as week_datetime,
                      max(datasource)									as datasource,
                      1													as nbr_trips
                      FROM stg.{1}
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
            ) select * from source;
            """.format(config_file['aggregated_table'], config_file['staging_table'])

        engine.execute(text(vw_query).execution_options(autocommit=True))

        log_stg_query = """
                insert into log.{0} (table_schema, table_name, filename, datetime, records, message)
                select 'agg', '{1}', '{2}', CURRENT_TIMESTAMP, count(*),'Inserted correctly' from agg.{1};
                """.format(config_file['logging_table'], config_file['aggregated_table'], f)

        engine.execute(text(log_stg_query).execution_options(autocommit=True))

        print('Aggregated table created successfully')
    except:
        print('Error at aggregating table')

def load():
    """
    Load csv file to staging table.
    Load aggregated table grouping trips with similar origin,destination and time
    """
    config_file = config_setup()
    engine = change_database()
    folderpath = config_file['input_folder']
    table_destination = config_file['staging_table']
    schema_valid_files = validate_schema(folderpath, config_file, engine)
    engine.execute(
        text("truncate table stg.{0};".format(config_file['staging_table'])).execution_options(autocommit=True))
    engine.execute(
        text("truncate table agg.{0};".format(config_file['aggregated_table'])).execution_options(autocommit=True))
    for f in schema_valid_files:
        try:
            print('Starting loading to stg process for file {0}...'.format(f))
            insert_with_progress(folderpath + '/' + f, engine, table_destination)
            log_stg_query = """
                    insert into log.{0} (table_schema, table_name, filename, datetime, records, message)
                    select 'stg', '{1}', '{2}', CURRENT_TIMESTAMP, count(*), 'Inserted correctly' from stg.{1};
                    """.format(config_file['logging_table'], config_file['staging_table'], f)
            engine.execute(text(log_stg_query).execution_options(autocommit=True))
            load_to_agg(config_file, engine, f)
            print('Data uploaded successfully for file {0}'.format(f))
        except:
            log_error_query = """
                    insert into log.{0} (table_schema, table_name, filename, datetime, records, message)
                    select 'stg', '{1}', '{2}',CURRENT_TIMESTAMP, count(*), 'Error loading data' from stg.{1};
                    """.format(config_file['logging_table'], config_file['staging_table'], f)
            engine.execute(text(log_error_query).execution_options(autocommit=True))
            print('Error loading data for file {0}'.format(f))
