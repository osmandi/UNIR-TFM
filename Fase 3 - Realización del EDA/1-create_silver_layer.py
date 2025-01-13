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
                        "MES": F.date_format(F.col("INICIO"), "LLLL"),
                    }
            ).select(
                F.col("INICIO"),
                F.col("YEAR"),
                F.col("HORA"),
                F.col("MINUTE"),
                F.col("DISTANCE"),
                F.col("VEL_PROMEDIO"),
                F.col("DIA_SEMANA"),
                F.col("MES"),
                F.col("NAME_FROM"),
                F.col("NAME_TO"),
                F.col("NAME_TO_1"),
                F.col("NAME_TO_2"),
                F.col("DIRECTION"),
                F.col("NUMDISPOSITIVOS")
            ).withColumnsRenamed({
                "INICIO": "start",
                "YEAR": "year",
                "HORA": "hour",
                "MINUTE": "minute",
                "DISTANCE": "distance",
                "VEL_PROMEDIO": "vel_avg",
                "DIA_SEMANA": "day_week_name",
                "MES": "month_name",
                "NAME_FROM": "name_from",
                "NAME_TO": "name_to",
                "NAME_TO_1": "name_to_1",
                "NAME_TO_2": "name_to_2",
                "DIRECTION": "direction",
                "NUMDISPOSITIVOS": "numdispositivos"
            })
        ).write.mode("overwrite").parquet("/TFM/data/silver/velocidades_bitcarrier.parquet")

    finally:
        spark.stop()

