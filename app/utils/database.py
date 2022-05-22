from app.utils.dbengine import engine_setup, config_setup
from sqlalchemy import text, create_engine

engine = engine_setup()
config_file = config_setup()

def create_database(engine):
    """
    :param engine: SQLalchemy engine
    :return: Create SQL database if not exists
    """
    print("Database engine created successfully")
    try:
        db_query = "CREATE DATABASE {0};".format(config_file['dbname'])

        query = "SELECT datname FROM pg_catalog.pg_database WHERE datname = 'jobsity'"
        databases_list = engine.execute(text(query).execution_options(autocommit=True))
        dbnames = [row[0] for row in databases_list]
        if 'jobsity' not in dbnames:
            conn = engine.connect()
            conn.execute("commit")
            conn.execute(db_query)
            conn.close()
            print("Database created")
        else:
            print("Database already created")
    except:
        print("Error at creating DB")

def change_database():
    """
    Create connection with database
    """
    config = config_setup()
    engine = create_engine(
        'postgresql+psycopg2://' + config['username'] + ':' + config['password'] +
        '@' + config['servername'] + ":" + str(config["port"]) + '/' + config['dbname'])

    return engine

def create_schema(engine):
    """
    :param engine: SQAlchemy engine
    :return: Create required schemas
    """
    try:
       engine.execute(text("CREATE SCHEMA IF NOT EXISTS stg AUTHORIZATION {0};".format(config_file['username'])).execution_options(autocommit=True))
       engine.execute(text("CREATE SCHEMA IF NOT EXISTS agg AUTHORIZATION {0};".format(config_file['username'])).execution_options(autocommit=True))
       engine.execute(text("CREATE SCHEMA IF NOT EXISTS log AUTHORIZATION {0};".format(config_file['username'])).execution_options(autocommit=True))
       print("Schemas executed")
    except:
        print("Error at creating schemas")

def create_tables(engine):
    """
    :param engine: SQAlchemy engine
    :return: Create staging table, aggregated table and log table schemas
    """
    try:
        trips_query = "CREATE TABLE IF NOT EXISTS stg.{0} (  \
                region character varying, \
                origin_coord character varying, \
                destination_coord character varying, \
                datetime character varying, \
                datasource character varying); \
                ALTER TABLE IF EXISTS stg.{0} OWNER to {1};".format(config_file['staging_table'],
                                                                    config_file['username'])
        engine.execute(text(trips_query).execution_options(autocommit=True))
        vw_trips_query = "CREATE TABLE IF NOT EXISTS agg.{0} ( \
                trip_id int GENERATED ALWAYS AS IDENTITY, \
                region character varying NOT NULL, \
                datetime timestamp without time zone NOT NULL, \
                year_datetime bigint NOT NULL, \
                week_datetime int NOT NULL, \
                datasource character varying NOT NULL, \
                nbr_trips int NOT NULL, \
                PRIMARY KEY (trip_id) );\
                ALTER TABLE IF EXISTS agg.{0} OWNER to {1};".format(config_file['aggregated_table'],
                                                                    config_file['username'])
        engine.execute(text(vw_trips_query).execution_options(autocommit=True))
        log_query = "CREATE TABLE IF NOT EXISTS log.{0} ( \
                    id int GENERATED ALWAYS AS IDENTITY, \
                    table_schema character varying NOT NULL, \
                    table_name character varying NOT NULL, \
                    filename character varying NOT NULL, \
                    datetime timestamp without time zone NOT NULL, \
                    records bigint NOT NULL, \
                    message character varying NOT NULL, \
                    PRIMARY KEY (id) );  \
                    ALTER TABLE IF EXISTS log.{0} OWNER to {1};".format(config_file['logging_table'],
                                                                        config_file['username'])
        engine.execute(text(log_query).execution_options(autocommit=True))
        print("Table creation executed")
    except:
        print("Error at creating tables")


