from app.utils.dbengine import config_setup
from app.utils.database import change_database
from sqlalchemy import text

def table_status():
    """

    Log the number of records inserted in staging and aggregated table

    """

    config_file = config_setup()
    engine = change_database()
    try:
        log_query = "SELECT records, datetime FROM log.{0}\
                                      WHERE table_name = '{1}' ORDER BY  datetime DESC LIMIT 1;".format(
            config_file['logging_table']
            , config_file['staging_table'])

        stg_records = engine.execute(text(log_query).execution_options(autocommit=True))

        for row in stg_records:
            print('Table: {0}'.format(config_file['staging_table']),
                  'Number of records:', row[0],
                  'Last upload date:', row[1])
        stg_records.close()

        log_agg_query = "SELECT records, datetime FROM log.{0} \
                                      WHERE table_name = '{1}' ORDER BY  datetime DESC LIMIT 1;".format(
            config_file['logging_table']
            , config_file['aggregated_table'])

        agg_records = engine.execute(text(log_agg_query).execution_options(autocommit=True))

        for row in agg_records:
            print('Table: {0}'.format(config_file['aggregated_table']),
                  'Number of records:', row[0],
                  'Last upload date:', row[1])
        agg_records.close()

    except:
        print("Error extracting info from trips_logging table")
