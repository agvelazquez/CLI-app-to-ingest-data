# Command Line interface to ingest data
### DB connector


jobsity-challenge app is a command line tool build in Python that take the CSV file with trips taken by different vehicles and calculate the weekly average by Region.

Output:
- Calculate the weekly average number of trips by region. 
- The app will create a chart with the data.

Requirements:
- A SQL database.
- Database connection uses Windows Authentication.
- Python 3 installed in your machine.
- Check requirements.txt for Python packages.
        
In order to run the app you will need to follow next steps:
1. Install PostgreSQL in your local system.
2. Install Python 3 and packages in requirements.txt
3. Download Git repository keeping the same structure
4. Complete the config.json file with your setup 
5. Load the csv file in the input folder. The expected schema is (region, origin_coord, destination_coord, datetime, datasource)  
6. Create the Database and the connection using command create_database
7. Load file with load_file given in the input folder in the json file 
8. Calculate the weekly average using weekly_average 
            
To check the status of the file loaded in the DB use file_status command. 

Use the command --help for more information.

For more details on jobsite-app commands please go to [RunningtheApp.md](https://github.com/agvelazquez/jobsity-challenge/blob/main/RunningtheApp.md)

### Output Example

The app will output a barchart showing the weekly average by Region

![plot](docs/output_chart.PNG)


### Areas of improvement 
- Use partitions and indexes in SQL tables
- Include a table with values in the output plot
- Improve notations and exceptions 
- Add a command to "run everything"

### Late Additions
- Replacing SQL server DB by PostgreSQL or MySQL because can work with Windows, Linux and Mac. Postgres seems to be the "python" of the DBs.
