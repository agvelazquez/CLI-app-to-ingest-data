# jobsity-challenge
### Data Engineering Challenge


jobsity-challenge app is a command line tool build in Python that take the CSV file with trips taken by different vehicles and calculate the weekly average by Region.

Output:
- Calculate the weekly average number of trips by region. 
- The app will create a chart with the data.

Requirements:
- A SQL sever Management Studio database (could be local).
- Database connection use Windows Authentication.
- Python 3 installed in your machine.
- Check requirements.txt for Python packages.
        
In order to run the app you will need to follow next steps:
1. Complete the config.json file with your setup 
2. Load the csv file in the input folder. The expected schema is (region, origin_coord, destination_coord, datetime, datasource)  
3. Create the Database and the connection using command create_database
4. Load file with load_file given in the input folder in the json file 
5. Calculate the weekly average using weekly_average 
            
To check the status of the file loaded in the DB use file_status command. 

Use the command --help for more information.

### Areas of improvement 
- Use an open source SQL database such as SQLite 
- Improve code notation
- Add decimals to weekly average
- Add title, labels, references and data table to the chart
- Read file in chunks instead using a df with all the data
- Test the scalability of the app
- Clean repo