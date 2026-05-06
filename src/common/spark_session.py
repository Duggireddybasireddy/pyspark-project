from pyspark.sql import SparkSession
from src.common.logger import get_logger

def get_spark_session(appname, log_path):
    logger=get_logger("spark_session", log_path)
    lst=["store_returns", "catalog_returns", "web_returns", "store_sales", "catalog_sales", "web_sales"]
    if appname in lst:
        spark = SparkSession.builder.appName(appname).\
                config("spark.sql.shuffle.partitions", 200).\
                config("spark.sql.adaptive.enabled", "true").\
                config("spark.sql.session.timeZone", "UTC").\
                config("spark.executor.memory", "4g").\
                master("spark://spark-master:7077").getOrCreate()

        return spark
    
    spark = SparkSession.builder.appName(appname).\
                config("spark.sql.shuffle.partitions", 200).\
                config("spark.sql.adaptive.enabled", "true").\
                config("spark.sql.session.timeZone", "UTC").\
                master("spark://spark-master:7077").getOrCreate()
            
    return spark

if __name__=="__main__":
    logger=get_logger("main", "log.log")
    logger.info("starting sparkSession with main method")
    get_spark_session("Running_as_main")
