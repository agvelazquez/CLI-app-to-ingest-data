import sys
import json
import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

def config_setup():
    configurationFileName = '.\\config.json'

    try:
        with open(configurationFileName, encoding='utf-8') as config_file:
            config = json.load(config_file)
            print("Configuration file:'" + os.path.realpath(config_file.name) + "' loaded successfully.")
    except:
        print('Error processing configuration file: ' + configurationFileName + '. Exiting.')
        exit(-1)

    return config

def engine_setup():
    config = config_setup()
    engine = create_engine(
        'mssql+pyodbc://@' + config['servername'] + '/' + config['dbname'] +
        '?trusted_connection=yes&driver=' + config['driver'])

    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    Base = declarative_base()

    return engine
