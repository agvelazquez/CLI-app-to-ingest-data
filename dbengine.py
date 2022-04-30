import sys
import json
import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


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

engine = create_engine(
    'mssql+pyodbc://@' + config['servername'] + '/' + config['dbname'] +
    '?trusted_connection=yes&driver=' + config['driver'])

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()
