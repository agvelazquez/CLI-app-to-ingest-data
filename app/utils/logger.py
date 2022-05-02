from app.utils.dbengine import engine_setup, config_setup

def table_status():
    """

    Log the number of records inserted in staging and aggregated table

    """

    config_file = config_setup()
    engine = engine_setup()
    try:
        log_query = "SELECT TOP 1 records, datetime FROM [{1}].[log].[{0}] \
                                      WHERE table_name = '{2}' ORDER BY  datetime DESC;".format(config_file['logging_table']
                                                                                                , config_file['dbname']
                                                                                                , config_file['staging_table'])

        stg_records = engine.execute(log_query)

        for row in stg_records:
            print('Table: {0}'.format(config_file['staging_table']),
                  'Number of records:', row[0],
                  'Last upload date:', row[1])
        stg_records.close()

        log_agg_query = "SELECT TOP 1 records, datetime FROM [{1}].[log].[{0}] \
                                      WHERE table_name = '{2}' ORDER BY  datetime DESC;".format(config_file['logging_table']
                                                                                                     , config_file['dbname']
                                                                                                     , config_file['aggregated_table'])

        agg_records = engine.execute(log_agg_query)

        for row in agg_records:
            print('Table: {0}'.format(config_file['aggregated_table']),
                  'Number of records:', row[0],
                  'Last upload date:', row[1])
        agg_records.close()

    except:
        print("Error extracting info from trips_logging table")