from app.utils.dbengine import engine_setup, config_setup

engine = engine_setup()
config_file = config_setup()

def create_database(engine):
    try:
        db_query = " IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = '{}') \
              EXEC('CREATE DATABASE {}');".format(config_file['dbname'], config_file['dbname'])
        print("Database engine created successfully")
        engine.execute(db_query)
        print("Database executed")
    except:
        print("Error at creating DB")

def create_schema(engine):
    try:
       engine.execute("IF NOT EXISTS ( SELECT  * FROM sys.schemas WHERE   name = N'stg') EXEC('CREATE SCHEMA [stg]');")
       engine.execute("IF NOT EXISTS ( SELECT  *  FROM sys.schemas  WHERE   name = N'agg') EXEC('CREATE SCHEMA [agg]');")
       engine.execute("IF NOT EXISTS ( SELECT  * FROM sys.schemas  WHERE   name = N'log') EXEC('CREATE SCHEMA [log]');")
       print("Schemas executed")
    except:
        print("Error at creating schemas")

def create_tables(engine):
    try:
        trips_query = " IF NOT EXISTS ( SELECT  * FROM sys.tables WHERE name = N'{}' ) \
             CREATE TABLE stg.{} (  \
                region VARCHAR(255), \
                origin_coord VARCHAR(255), \
                destination_coord VARCHAR(255), \
                datetime VARCHAR(255), \
                datasource VARCHAR(255) \
                    );".format(config_file['staging_table'], config_file['staging_table'])
        engine.execute(trips_query)
        vw_trips_query = " IF NOT EXISTS ( SELECT  * FROM sys.tables WHERE name = N'{}' ) \
             CREATE TABLE agg.{} ( \
                trip_id int IDENTITY(1,1) PRIMARY KEY, \
                region VARCHAR(255) NOT NULL, \
                datetime datetime NOT NULL, \
                year_datetime bigint NOT NULL, \
                week_datetime int NOT NULL, \
                datasource VARCHAR(255) NOT NULL, \
                nbr_trips int NOT NULL \
                       );".format(config_file['aggregated_table'], config_file['aggregated_table'])
        engine.execute(vw_trips_query)
        log_query = " IF NOT EXISTS ( SELECT  * FROM sys.tables WHERE name = N'{}' ) \
                CREATE TABLE log.{} ( \
                    id int IDENTITY(1,1) PRIMARY KEY, \
                    table_schema VARCHAR(3) NOT NULL, \
                    table_name VARCHAR(50) NOT NULL, \
                    datetime datetime NOT NULL, \
                    records bigint NOT NULL \
                       );".format(config_file['logging_table'], config_file['logging_table'])
        engine.execute(log_query)
        print("Table creation executed")
    except:
        print("Error at creating tables")


