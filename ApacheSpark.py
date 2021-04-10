from pyspark.sql import SparkSession
from pathlib import Path
import os


class spark():
    def __init__(self):
        from configuration import config
        config = config.get()
        self.spark = None
        self.i94path = config.get("File","i94Path")
        self.demoPath = config.get("File","demoPath")
        self.airportPath = config.get("File","airportPath")
        self.tempreturePath = config.get("File","tempreturePath")
        self.paqutRoot = config.get("File","paqutRoot")

    def get(self):
        """
        Create the spark session.
        :return: return the spark object.
        """
        spark = SparkSession.builder\
        .config("spark.jars.packages","saurfang:spark-sas7bdat:3.0.0-s_2.12") \
        .config("spark.jars", "/jar/postgresql-42.2.19.jar") \
        .enableHiveSupport().getOrCreate()
        self.spark=spark

    def registerSparkSqlTable(self,sparkFileObject,FilePath,parquetPath,tablename):
        """
        :param sparkFileObject:  Readed spark object(sas/csv).
        :param FilePath: source file location.
        :param parquetPath: Path where to store the parquet file.
        :param tablename: Spark sql temp view name.
        :return: void
        """
        size = Path(FilePath).stat().st_size
        if size > 101990272:
            sparkFileObject.write.mode('overwrite').parquet(parquetPath)
            df = self.spark.read.parquet(parquetPath)
            df.registerTempTable(tablename)
        else:
            sparkFileObject.registerTempTable(tablename)

    def createSparkSql(self):
        """
        Get readed spark object, create parquet file/ register spark temp table view respectively.
        :return: void
        """
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
        """
        :param query: Sparked cleansd query from sql_queries.py
        :return: cleasned dataframe.
        """
        df = self.spark.sql(query)
        return df