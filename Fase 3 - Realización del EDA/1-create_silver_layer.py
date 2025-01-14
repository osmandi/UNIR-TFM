"""
Description: Script to create gold layer to traffic congestion analyst
"""

# Import packages
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

if __name__ == "__main__":
    # Create and configure SparkSession
    spark = SparkSession.builder.appName("SilverLayer").getOrCreate()

    try:
        # Read dataframe in silver
        df = spark.read.parquet("/TFM/data/bronze/velocidades_bitcarrier.parquet")

        # Create ROUTE_ID column to identify unique combination of NAME_FROM and NAME_TO columns
        df_route_id = df.select(F.col("NAME_FROM"), F.col("NAME_TO")).distinct().withColumn("ROUTE_ID", F.monotonically_increasing_id())
        df = df.join(F.broadcast(df_route_id), ["NAME_FROM", "NAME_TO"], how="inner")

        # Filter DataFrame
        (
            df.withColumns(
                    {
                        "YEAR": F.year(F.col("INICIO")).cast(T.IntegerType()),
                        "MINUTE": F.minute(F.col("INICIO")).cast(T.IntegerType()),
                        "NAME_TO": F.trim(F.col("NAME_TO")),
                        "NAME_FROM": F.trim(F.col("NAME_FROM")),
                        "NAME_TO_1": F.replace(F.split_part(F.col("NAME_TO"), F.lit(";"), F.lit(1)), F.lit("CL"), F.lit("")).cast(T.IntegerType()),
                        "NAME_TO_2": F.replace(F.split_part(F.col("NAME_TO"), F.lit(";"), F.lit(2)), F.lit("CL"), F.lit("")).cast(T.IntegerType()),
                        "DIRECTION": F.when(F.col("NAME_TO_1") < F.col("NAME_TO_2"), F.lit("SOUTH-NORTH")).otherwise(F.lit("NORTH-SOUTH")),
                        "DIA_SEMANA": F.date_format(F.col("INICIO"), "EEEE"),
                        "NUMERO_SEMANA": F.dayofweek(F.col("INICIO")),
                        "MES": F.date_format(F.col("INICIO"), "LLLL"),
                        "MONTH_NUMBER": F.month(F.col("INICIO"))
                    }
            ).select(
                F.col("INICIO"),
                F.col("YEAR"),
                F.col("HORA"),
                F.col("MINUTE"),
                F.col("CUARTO_HORA"),
                F.col("DISTANCE"),
                F.col("VEL_PROMEDIO"),
                F.col("VEL_PONDERADA"),
                F.col("DIA_SEMANA"),
                F.col("NUMERO_SEMANA"),
                F.col("MES"),
                F.col("MONTH_NUMBER"),
                F.col("NAME_FROM"),
                F.col("NAME_TO"),
                F.col("NAME_TO_1"),
                F.col("NAME_TO_2"),
                F.col("ROUTE_ID"),
                F.col("DIRECTION"),
                F.col("NUMDISPOSITIVOS")
            ).withColumnsRenamed({
                "INICIO": "start_timestamp",
                "YEAR": "year",
                "HORA": "hour",
                "MINUTE": "minute",
                "CUARTO_HORA": "time",
                "DISTANCE": "distance",
                "VEL_PROMEDIO": "speed_average",
                "VEL_PONDERADA": "speed_weighted",
                "DIA_SEMANA": "day_week_name",
                "NUMERO_SEMANA": "day_week_number",
                "MES": "month_name",
                "MONTH_NUMBER": "month_number",
                "NAME_FROM": "name_from",
                "NAME_TO": "name_to",
                "NAME_TO_1": "name_to_1",
                "NAME_TO_2": "name_to_2",
                "ROUTE_ID": "route_id",
                "DIRECTION": "direction",
                "NUMDISPOSITIVOS": "num_devices"
            })
        ).write.mode("overwrite").parquet("/TFM/data/silver/velocidades_bitcarrier.parquet")

    finally:
        spark.stop()

