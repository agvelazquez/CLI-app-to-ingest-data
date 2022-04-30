import pandas as pd
import os
import json
import sys
import sqlalchemy as sa



if __name__ == "__main__":
    configurationFileName = '.\\config.json'

    if len(sys.argv) > 1:
        configurationFileName = sys.argv[1]

    try:
        with open(configurationFileName, encoding='utf-8') as config_file:
            config = json.load(config_file)
            print("Configuration file:'" + os.path.realpath(config_file.name) + "' loaded successfully.")
    except:
        print('Error processing configuration file: ' + configurationFileName + '. Exiting.')
        exit(-1)

    engine = sa.create_engine(
        'mssql+pyodbc://@' + config['servername'] + '/' + config['dbname'] + '?trusted_connection=yes&driver=ODBC+Driver+18+for+SQL+Server')

    # printing names of the tables present in the database

    df = pd.read_csv(r'C:\Users\avelazquez\Documents\GitHub\jobsity-challenge\sample\trips.csv')

    print(engine.table_names()) #it is only searching in the dbo schema

    result = engine.execute('select * \
    from jobsity.agg.vw_trips')
    for row in result:
        print(row)
    result.close()
