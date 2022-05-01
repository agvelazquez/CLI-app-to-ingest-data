from dbengine import engine_setup


def table_status():
    engine = engine_setup()
    try:
        stg_records = engine.execute("SELECT TOP 1 records, datetime FROM [jobsity].[log].[trips_logging] \
                                      WHERE table_name = 'trips' ORDER BY  datetime DESC;")

        for row in stg_records:
            print('Table: trips',
                  'Number of records:', row[0],
                  'Last upload date:', row[1])
        stg_records.close()

        agg_records = engine.execute("SELECT TOP 1 records, datetime FROM [jobsity].[log].[trips_logging] \
                                      WHERE table_name = 'vw_trips' ORDER BY  datetime DESC;")

        for row in agg_records:
            print('Table: vw_trips',
                  'Number of records:', row[0],
                  'Last upload date:', row[1])
        agg_records.close()

    except:
        print("Error extracting info from trips_logging table")