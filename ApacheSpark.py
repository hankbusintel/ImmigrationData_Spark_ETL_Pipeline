from pyspark.sql import SparkSession
from pathlib import Path
import os

class spark():
    def __init__(self):
        self.spark = None
        self.i94path = r'C:\Users\hangg\OneDrive\Desktop\OneDrive\Udacity DE\Project 6\data\i94_apr16_sub.sas7bdat'
        self.demoPath = r"C:\Users\hangg\OneDrive\Desktop\OneDrive\Udacity DE\Project 6\data\us-cities-demographics.csv"
        self.airportPath = r'C:\Users\hangg\OneDrive\Desktop\OneDrive\Udacity DE\Project 6\data\airport-codes_csv.csv'
        self.tempreturePath = r'C:\Users\hangg\OneDrive\Desktop\OneDrive\Udacity DE\Project 6\data\GlobalLandTemperaturesByCity.csv'
        self.paqutRoot = ''

    def get(self):
        spark = SparkSession.builder\
        .config("spark.jars.packages","saurfang:spark-sas7bdat:3.0.0-s_2.12") \
        .config("spark.jars", "/jar/postgresql-42.2.19.jar") \
        .enableHiveSupport().getOrCreate()
        self.spark=spark

    def registerSparkSqlTable(self,sparkFileObject,FilePath,parquetPath,tablename):
        size = Path(FilePath).stat().st_size
        if size > 101990272:
            #sparkFileObject.write.mode('overwrite').parquet(parquetPath)
            df = self.spark.read.parquet(parquetPath)
            df.registerTempTable(tablename)
        else:
            sparkFileObject.registerTempTable(tablename)

    def createSparkSql(self):
        df_spark_immgrate = self.spark.read.format('com.github.saurfang.sas.spark').options(header='true',delimiter=None)\
            .load(self.i94path)
        df_spark_airport = self.spark.read.format('csv').options(header='true', delimiter=",")\
            .load(self.airportPath)
        df_spark_demo = self.spark.read.format('csv').options(header='true', delimiter=";")\
            .load(self.demoPath)
        df_spark_tempreture = self.spark.read.format('csv').options(header='true', delimiter=",")\
            .load(self.tempreturePath)

        #i94 file
        self.registerSparkSqlTable(df_spark_immgrate, self.i94path, os.path.join(self.paqutRoot, "sas_data"),"immigration")
        self.registerSparkSqlTable(df_spark_demo, self.demoPath, os.path.join(self.paqutRoot, "demo_data"),"demo")
        self.registerSparkSqlTable(df_spark_airport, self.airportPath, os.path.join(self.paqutRoot, "airport_data"),"airport")
        self.registerSparkSqlTable(df_spark_tempreture, self.tempreturePath, os.path.join(self.paqutRoot, "temp_data"),"tempreture")

    def getCleansedDataFrame(self, query):
        df = self.spark.sql(query)
        return df