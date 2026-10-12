from pyspark.sql import SparkSession


def get_spark(app_name="orders-pipeline"):
    """Local Spark: driver and executors all in this one process.
    On AWS the same code would run on an EMR cluster or as a Glue job."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")                              # every CPU core acts as an executor slot
        .config("spark.sql.shuffle.partitions", "4")     # default 200 is far too many for small data
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .getOrCreate()
    )