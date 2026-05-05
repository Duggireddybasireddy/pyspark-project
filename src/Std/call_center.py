from src.common.spark_session import get_spark_session
from src.common.logger import get_logger
from src.config.config_read import load_config
from pyspark.sql.functions import col, to_date
import time, os

def call_center():
    start_time=time.time()
    config=load_config()
    log_path=config['path']['logs']['std_log']
    logger=get_logger("call_center", log_path)
    logger.info("Creating SparkSession...........")
    spark=get_spark_session("call_center", log_path)
    inbound_file=config["path"]["std_source"]["call_center"]
    out_path=config["path"]["gold_source"]["call_center"]

    logger.info(f"Input file path is {inbound_file}")
    try:
        logger.info("Reading data from inbound path")
        df=spark.read.format("csv").option("header", "true").load(inbound_file)
        cnt = df.count()
        if not cnt:
            logger.info("File is empty")
        

        else:
            logger.info("Changing the datatype of attributes")
            df_data=df.select(col("cc_call_center_sk").cast("string").alias("cc_call_center_sk"),
                              col("cc_call_center_id").cast("string").alias("cc_call_center_id"),
                              col("cc_name").cast("string").alias("cc_name"),
                              col("cc_city").cast("string").alias("cc_city"),
                              col("cc_state").cast("string").alias("cc_state"),
                              col("cc_country").cast("string").alias("cc_country")
                              )

            logger.info("Performing the Data Validation")
            df_filter=df_data.na.drop(subset=["cc_call_center_sk"])
            ext_cnt=df_filter.count()
            diff = cnt - ext_cnt
            logger.info(f"Total records read is {cnt}")
            logger.info(f"Dropped Records count is {diff}")
            logger.info(f"Writing {ext_cnt} records to {out_path} in csv format")
            df_filter.coalesce(1).write.mode("overwrite").parquet(out_path)
            logger.info(f"Successfully wrote {ext_cnt} records to {out_path}")
    
    except Exception as e:
        logger.exception("File Not Found")

    logger.info(f"Time taken to run {os.path.basename(__file__)} : {round(time.time() - start_time, 2)} seconds")
    spark.stop()

if __name__=="__main__":
    call_center()