from app.utils.dbengine import engine_setup, config_setup
from sqlalchemy import text

engine = engine_setup()
config_file = config_setup()

def create_database(engine):
    """
    :param engine: SQLalchemy engine
    :return: Create SQL database if not exists
    """
    try:
        db_query = " IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = '{}') \
              EXEC('CREATE DATABASE {}');".format(config_file['dbname'], config_file['dbname'])
        print("Database engine created successfully")
        engine.execute(text(db_query).execution_options(autocommit=True))
        print("Database executed")
    except:
        print("Error at creating DB")

def create_schema(engine):
    """
    :param engine: SQLalchemy engine
    :return: Create required schemas
    """
    try:
       engine.execute(text("USE {0}; IF NOT EXISTS ( SELECT  * FROM sys.schemas WHERE   name = N'stg') EXEC('CREATE SCHEMA [stg]');".format(config_file['dbname'])).execution_options(autocommit=True))
       engine.execute(text("USE {0}; IF NOT EXISTS ( SELECT  *  FROM sys.schemas  WHERE   name = N'agg') EXEC('CREATE SCHEMA [agg]');".format(config_file['dbname'])).execution_options(autocommit=True))
       engine.execute(text("USE {0}; IF NOT EXISTS ( SELECT  * FROM sys.schemas  WHERE   name = N'log') EXEC('CREATE SCHEMA [log]');".format(config_file['dbname'])).execution_options(autocommit=True))
       print("Schemas executed")
    except:
        print("Error at creating schemas")

def create_tables(engine):
    """
    :param engine: SQLalchemy engine
    :return: Create staging table, aggregated table and log table schemas
    """
    try:
        trips_query = " IF NOT EXISTS ( SELECT  * FROM sys.tables WHERE name = N'{1}' ) \
             CREATE TABLE {0}.stg.{1} (  \
                region VARCHAR(255), \
                origin_coord VARCHAR(255), \
                destination_coord VARCHAR(255), \
                datetime VARCHAR(255), \
                datasource VARCHAR(255) \
                    );".format(config_file['dbname'], config_file['staging_table'])
        engine.execute(text(trips_query).execution_options(autocommit=True))
        vw_trips_query = " IF NOT EXISTS ( SELECT  * FROM sys.tables WHERE name = N'{1}' ) \
             CREATE TABLE {0}.agg.{1} ( \
                trip_id int IDENTITY(1,1) PRIMARY KEY, \
                region VARCHAR(255) NOT NULL, \
                datetime datetime NOT NULL, \
                year_datetime bigint NOT NULL, \
                week_datetime int NOT NULL, \
                datasource VARCHAR(255) NOT NULL, \
                nbr_trips int NOT NULL \
                       );".format(config_file['dbname'], config_file['aggregated_table'])
        engine.execute(text(vw_trips_query).execution_options(autocommit=True))
        log_query = " IF NOT EXISTS ( SELECT  * FROM sys.tables WHERE name = N'{1}' ) \
                CREATE TABLE {0}.log.{1} ( \
                    id int IDENTITY(1,1) PRIMARY KEY, \
                    table_schema VARCHAR(3) NOT NULL, \
                    table_name VARCHAR(50) NOT NULL, \
                    datetime datetime NOT NULL, \
                    records bigint NOT NULL \
                       );".format(config_file['dbname'], config_file['logging_table'])
        engine.execute(text(log_query).execution_options(autocommit=True))
        print("Table creation executed")
    except:
        print("Error at creating tables")


