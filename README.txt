**Scope of the Project**

This project is to Process immigration, city, airport, tempreture data for analytic purposes for
international travel corporation or large travel agency. By having the above information, large travel
corporation could get insight on travelers activities and predict marketing demand in the future.

**Data Model**
*Main Table*
immigration: cicid, i94yr, i94mon, i94port, i94cit, i94res, arrdate, depdate, i94addr,
biryear, gender, airline

Tempreture: date, city, country, latitude, longitude, avg_temp, avg_temp_uncertain

airport: airport_id, type, name, region, city

Demographics: city, state, race, mid_age, male_population,female_population,
total_population,num_of_verterans,foreign_born,avg_household_size,state_cd

*Look up table for immigration*
Lui94cntyl: i94citres,country_name
Lui94prtl: i94port, city, state
Lui94mode: i94mode,type
Lui94addrl:i94addr,name

**ETL STEP**
Step 1: Read data using spark. ApacheSpark.py contains the class/functions that can be take file path as an input(it could be either from AWS
S3 or local share), read it through pyspark. The class will also determine whether the file is larger than 100mb, if true,
then it will create a parquet file location on user's choice.

Step 2: Cleansing data using spark sql. ApacheSpark.py contains the class/functions that can clean the data including eliminate
null value, remove duplicate records using spark sql, returning the cleansed spark dataframe.

Step 3: Open connection with on premise PostgreSql database. Postgre.py contains the class/functions that will check whether
the database has the predefined data tables, if not it will create the set of tables. Also, it will load the cleansed spark
dataframe to the set of staging tables in an overwrite mode.

Step 4: Load meta data. meta.py contains the class/functions that will process the SAS description files, extract four lookup
table into pandas dataframe. Postgre.py include the class/functions that will load these data set into postgre sql database.

Step 5: Merge data from staging to destination table. Postgre.py and sql_queries.py contains the functions/queries that
load data from staging table to the final main tables.

Step 6: Data quality checks. This step will check the data quality for any existing table and existing column dynamically
by accepting sql queries as an user input. If the query result doesn't meet expectation, it will failed the step. configuration.py
includes the user input query and expected result, and the Postgre.py includes the class/function that can use to make the
quality check.

**Other Scenarios**
1. Data has been increased by 100x
If the data volumn has been increased for a single file, we should still be able to handle it as spark is the tool to
read the big data, but we need to check the CPU usage and increase the node of the cluster if needed.
If the data has been sent as 100 files each time, for example, there are 100 immigration files arrive to the source folder
every day. We need to write another script to loop through the folder and get the path of each file and parse in the current
etl program.

2. The pipelines would be run on a daily basis by 7 am every day.
We can deploy the python script to apache airflow using the python operator to call the script, set up the job to run every day.
Alternatively, we could also set up the task schedule on windows task scheduler to run at every day.

3. The database needed to be accessed by 100+ people.
Create a DNS service for the database connection
Build Restful API on top of the postgreSql database also ensure the API can be call be multiple users at the same time.

**Execution Instructions**
1. Inter configuration in the immigration.cfg files, including postgre sql connection properties, and file paths.
2. etl.py file is the main script which will read/load the data.
Execute the etl.py files using python.exe.