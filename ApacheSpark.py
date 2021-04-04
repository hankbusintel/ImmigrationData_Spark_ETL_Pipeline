from pyspark.sql import SparkSession
from pathlib import Path
import os

class spark():
    def __init__(self):
        self.spark = None
        self.i94path = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
        self.demoPath = "us-cities-demographics.csv"
        self.airportPath = 'airport-codes_csv.csv'
        self.tempreturePath = '../../data2/GlobalLandTemperaturesByCity.csv'
        self.paqutRoot = ''

    def get(self):
        spark = SparkSession.builder.\
        config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport().getOrCreate()
        self.spark=spark

    def registerSparkSqlTable(self,sparkFileObject,FilePath,parquetPath,tablename):
        size = Path(FilePath).stat().st_size
        if size > 101990272:
            sparkFileObject.write.mode('overwrite').parquet(parquetPath)
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
        self.registerSparkSqlTable(df_spark_immgrate, self.demoPath, os.path.join(self.paqutRoot, "demo_data"),"demo")
        self.registerSparkSqlTable(df_spark_immgrate, self.airportPath, os.path.join(self.paqutRoot, "airport_data"),"airport")
        self.registerSparkSqlTable(df_spark_immgrate, self.tempreturePath, os.path.join(self.paqutRoot, "temp_data"),"tempreture")
