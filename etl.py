
from create_table import table_refresh
from ApacheSpark import spark
from sql_queries import *
from Postgre import postgre

def SparkReadData():
    s = spark()
    s.get()
    s.createSparkSql()
    return s

def loadSparkToStagTable(spark,postgre):
    # Get cleansed spark dataframe
    immgr = spark.getCleansedDataFrame(spark_clean_imrr)
    temp = spark.getCleansedDataFrame(spark_clean_temp)
    demo = spark.getCleansedDataFrame(spark_clean_demo)
    airport = spark.getCleansedDataFrame(spark_clean_airport)
    
    #load the cleansed dataframe to postgre staging table.
    p.sparkWriteToPostgre(immgr,"stg_immigration")
    p.sparkWriteToPostgre(temp, "stg_tempreture")
    p.sparkWriteToPostgre(demo, "stg_demographic")
    p.sparkWriteToPostgre(airport, "stg_airport")

def main():

    #create spark session read immigration, tempreture, airport and demographic data
    spark = SparkReadData()
    p = postgre()
    
    #Load staging table from spark
    loadSparkToStagTable(spark,p)

    #create postgreSql database connection
    cur,conn = p.get()
    #Create destination tables
    cur.execute(table_refresh)
    
    #read data from SAS descirption file
    metadata = sasDesc.getMetaDataDict(MetaData)
    #Upsert meta data i94cntyl,i94prtl, i94mode, i94addrl
    p.upsertAllMetaData(cur,metadata,dic_query)
    
    #Upsert destination tables
    conn.close()
    
if __name__ == "__main__":
    main()