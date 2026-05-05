from src.common.spark_session import get_spark_session
from src.common.logger import get_logger
from src.config.config_read import load_config
from pyspark.sql.functions import col
import time, os

def cust_demo():
    start_time=time.time()
    config=load_config()
    log_path=config['path']['logs']['std_log']
    logger=get_logger("std_layer", log_path)
    logger.info("Creating SparkSession...........")
    spark=get_spark_session("std_layer", log_path)
    inbound_file=config["path"]["std_source"]["customer_demographics"]
    out_path=config["path"]["gold_source"]["customer_demographics"]

    logger.info(f"Input file path is {inbound_file}")
    try:
        logger.info("Reading data from inbound path")
        df=spark.read.format("csv").option("header", "true").load(inbound_file)
        cnt=df.count()
        if not cnt:
            logger.info("File is empty")

        else:
            logger.info("Changing the datatype of attributes")
            df_data=df.select(col("cd_demo_sk").cast("string").alias("cd_demo_sk"),
                              col("cd_gender").cast("string").alias("cd_gender"),
                              col("cd_marital_status").cast("string").alias("cd_marital_status"),
                              col("cd_education_status").cast("string").alias("cd_education_status"),
                              col("cd_purchase_estimate").cast("string").alias("cd_purchase_estimate"),
                              col("cd_credit_rating").cast("string").alias("cd_credit_rating")
                              )
            logger.info("Filtering the data based the cd_demo_sk, when cd_demo_sk is null")
            df_filter=df_data.na.drop(subset=["cd_demo_sk"])
            ext_cnt=df_filter.count()
            diff=cnt - ext_cnt
            logger.info(f"Total records read is {cnt}")
            logger.info(f"Dropped Records count is {diff}")
            logger.info(f"Writing {ext_cnt} records to {out_path} in csv format")
            df_filter.coalesce(3).write.mode("overwrite").parquet(out_path)
            logger.info(f"Successfully wrote {ext_cnt} records to {out_path}")

    except Exception:
        logger.exception()
    
    logger.info(f"Time taken to run {os.path.basename(__file__)} : {round(time.time() - start_time, 2)} seconds")
    spark.stop()

if __name__=="__main__":
    cust_demo()