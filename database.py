from dbengine import engine

#create schemas and DB
try:
    engine.execute(" IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = 'jobsity') \
          EXEC('CREATE DATABASE jobsity');")
    print("Database executed")
except:
    print("Error at creating DB")

try:
   engine.execute("IF NOT EXISTS ( SELECT  * FROM    sys.schemas WHERE   name = N'stg' ) \
        EXEC('CREATE SCHEMA [stg]');")

   engine.execute("IF NOT EXISTS ( SELECT  *  FROM    sys.schemas  WHERE   name = N'agg' ) \
        EXEC('CREATE SCHEMA [agg]');")

   engine.execute("IF NOT EXISTS ( SELECT  * FROM    sys.schemas  WHERE   name = N'log' ) \
        EXEC('CREATE SCHEMA [log]');")
   print("Schemas executed")

except:

    print("Error at creating schemas")

try:
    engine.execute(" IF NOT EXISTS ( SELECT  * FROM sys.tables WHERE name = N'trips' ) \
         CREATE TABLE stg.trips (  \
            region VARCHAR(255), \
            origin_coord VARCHAR(255), \
            destination_coord VARCHAR(255), \
            datetime VARCHAR(255), \
            datasource VARCHAR(255) \
                );")

    engine.execute(" IF NOT EXISTS ( SELECT  * FROM sys.tables WHERE name = N'vw_trips' ) \
         CREATE TABLE agg.vw_trips ( \
            trip_id int IDENTITY(1,1) PRIMARY KEY, \
            region VARCHAR(255) NOT NULL, \
            datetime datetime NOT NULL, \
            year_datetime bigint NOT NULL, \
            week_datetime int NOT NULL, \
            datasource VARCHAR(255) NOT NULL, \
            nbr_trips int NOT NULL \
                   );")

    engine.execute(" IF NOT EXISTS ( SELECT  * FROM sys.tables WHERE name = N'trips_logging' ) \
            CREATE TABLE log.trips_logging ( \
                id int IDENTITY(1,1) PRIMARY KEY, \
                table_schema VARCHAR(3) NOT NULL, \
                table_name VARCHAR(50) NOT NULL, \
                datetime datetime NOT NULL, \
                records bigint NOT NULL \
                   );")

    print("Schemas executed")

except:

    print("Error at creating tables")


print("DDL executed succesfully")

