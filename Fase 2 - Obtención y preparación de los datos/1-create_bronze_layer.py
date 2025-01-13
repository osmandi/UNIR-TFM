"""
Description: This script load raw data and clean.
"""
# Import packages
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T
import subprocess
import functools

if __name__ == "__main__":

    # Create and configure SparkSession
    spark = SparkSession.builder.appName("BronzeLayer").getOrCreate()
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")  # To prevent this error: "Fail to parse '4/1/2020 9:15:00 AM'

    try:
        # Read each CSV one by one and join in one PySpark DataFrame
        p = subprocess.Popen("hdfs dfs -ls /TFM/data/raw/*.csv |  awk '{print $8}'",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        file_list = [file.decode('utf-8').replace("\n","") for file in p.stdout.readlines()]
        dfs = [spark.read.option("header", True).option("inferSchema", True).csv(file) for file in file_list]
        df = functools.reduce(functools.partial(DataFrame.unionByName, allowMissingColumns=True) , dfs)

        # Change data format for each column
        df = df.withColumns(
            {
                "INICIO": F.to_timestamp(F.col("INICIO"), "M/d/y h:mm:ss a"),
                "FIN": F.to_timestamp(F.col("FIN"), "M/d/y h:mm:ss a"),
                "COEF_BRT": F.col("COEF_BRT").cast(T.DoubleType()),
                "COEF_MIXTO": F.col("COEF_MIXTO").cast(T.DoubleType()),
                "VEL_MEDIA_BRT": F.col("VEL_MEDIA_BRT").cast(T.DoubleType()),
                "VEL_MEDIA_MIXTO": F.col("VEL_MEDIA_MIXTO").cast(T.DoubleType()),
                "VEL_MEDIA_PONDERADA": F.col("VEL_MEDIA_PONDERADA").cast(T.DoubleType()),
                "Shape__Length": F.col("Shape__Length").cast(T.DoubleType()),
                "NUMDISPOSITIVOS": F.col("NUMDISPOSITIVOS").cast(T.IntegerType()),
                "TYPE": F.col("TYPE").cast(T.IntegerType())
            }
        )

        # Save in parquet format
        df.write.mode("overwrite").parquet("/TFM/data/bronze/velocidades_bitcarrier.parquet")

    finally:
        spark.stop()