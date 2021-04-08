
from create_table import table_refresh
from ApacheSpark import spark
from sql_queries import *

def SparkReadData():
    s = spark()
    s.get()
    s.createSparkSql()
    return s



def main():

    spark = SparkReadData()
    immgr = spark.getCleansedDataFrame(spark_clean_temp)

main()