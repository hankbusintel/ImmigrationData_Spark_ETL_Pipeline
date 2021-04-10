
from ApacheSpark import spark
from sql_queries import *
from Postgre import postgre
from meta import sasDesc
from configuration import MetaData,dic_query,dic_mainInsertList,data_quality

def SparkReadData():
    print ("Building Spark session and creating view for Spark sql.")
    s = spark()
    s.get()
    s.createSparkSql()
    print ("success")
    return s

def loadSparkToStagTable(spark,postgre):
    # Get cleansed spark dataframe
    print ("Cleansing spark data...")
    immgr = spark.getCleansedDataFrame(spark_clean_imrr)
    temp = spark.getCleansedDataFrame(spark_clean_temp)
    demo = spark.getCleansedDataFrame(spark_clean_demo)
    airport = spark.getCleansedDataFrame(spark_clean_airport)
    print("loading to postgre staging table...")
    #load the cleansed dataframe to postgre staging table.
    postgre.sparkWriteToPostgre(immgr,"stg_immigration")
    postgre.sparkWriteToPostgre(temp, "stg_tempreture")
    postgre.sparkWriteToPostgre(demo, "stg_demographic")
    postgre.sparkWriteToPostgre(airport, "stg_airport")
    print ("success")

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
    p.executeBatch(cur,dic_mainInsertList)
    
    #data quality check
    p.check_data_quality(cur,data_quality)
    
    conn.close()
    
if __name__ == "__main__":
    main()