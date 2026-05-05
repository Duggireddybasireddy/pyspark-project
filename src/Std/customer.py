from src.common.spark_session import get_spark_session
from src.common.logger import get_logger
from src.config.config_read import load_config
from pyspark.sql.functions import col, concat, to_date, row_number, lpad, rpad
from pyspark.sql.window import Window
import time, os

def cust():
    start_time=time.time()
    config=load_config()
    log_path=config['path']['logs']['std_log']
    logger=get_logger("std_layer", log_path)
    logger.info("Creating SparkSession...........")
    spark=get_spark_session("std_layer", log_path)
    logger.info("SparkSession created successfully")
    file_path=config["path"]["std_source"]["customer"]
    out_path=config["path"]["gold_source"]["customer"]
    logger.info(f"File Path is {file_path}")
    logger.info("creating Dataframe")
    try:
        df = spark.read.format("csv").option("header", "true").load(file_path)
        cnt=df.count()
        if not cnt:
            logger.warning("DataFrame is Empty")
            
        else:
            logger.info("DataFrame created successfully")
            logger.info("Changing the data types of attributes")
            df_type=df.select(col("c_customer_sk").cast("string").alias("c_customer_sk"),
                              col("c_customer_id").cast("string").alias("c_customer_id"),
                              col("c_current_cdemo_sk").cast("string").alias("c_current_cdemo_sk"),
                              col("c_current_hdemo_sk").cast("string").alias("c_current_hdemo_sk"),
                              col("c_current_addr_sk").cast("string").alias("c_current_addr_sk"),
                              col("c_first_shipto_date_sk").cast("string").alias("c_first_shipto_date_sk"),
                              col("c_first_sales_date_sk").cast("string").alias("c_first_sales_date_sk"),
                              col("c_last_review_date").cast("string").alias("c_last_review_date_sk"),
                              col("c_salutation").cast("string").alias("c_salutation"),
                              col("c_first_name").cast("string").alias("c_first_name"),
                              col("c_last_name").cast("string").alias("c_last_name"),
                              col("c_preferred_cust_flag").cast("string").alias("c_preferred_cust_flag"),
                              to_date(concat(lpad(col("c_birth_day"), 2, "0"),lpad(col("c_birth_month"), 2, "0"),col("c_birth_year")), "ddMMyyyy").alias("DOB"),
                              col("c_birth_country").cast("string").alias("c_birth_country"),
                              col("c_login").cast("string").alias("c_login"),
                              col("c_email_address").cast("string").alias("c_email_address")
                              )
            logger.info("Performing the filter operations and selection valid sks")
            #df_filter = df.type.where(col("c_customer_sk").isNotNull() & col("c_current_cdemo_sk").isNotNull() & col("c_current_hdemo_sk").isNotNull() & col("c_current_addr_sk").isNotNull() & col("c_first_shipto_date_sk").isNotNull() & col("c_first_sales_date_sk").isNotNull())
            df_filter = df_type.na.drop(subset=["c_customer_sk", "c_current_cdemo_sk", "c_current_hdemo_sk", "c_current_addr_sk", "c_first_shipto_date_sk", "c_first_sales_date_sk"])
            logger.info("Applying deduplication logic using window fuction")
            window = Window.partitionBy("c_customer_sk", "c_current_cdemo_sk", "c_current_hdemo_sk", "c_current_addr_sk", "c_first_shipto_date_sk", "c_first_sales_date_sk").orderBy(col("c_first_sales_date_sk").desc(), col("c_first_shipto_date_sk").desc())
            df_final = df_filter.withColumn("RN", row_number().over(window)).where(col("RN")==1).drop(col("RN"))
            ext_cnt=df_final.count()
            diff= cnt - ext_cnt
            logger.info(f"Total records read is {cnt}")
            logger.info(f"Dropped Records count is {diff}")
            logger.info(f"Writing {ext_cnt} records to {out_path} in csv format")
            df_final.coalesce(1).write.mode("overwrite").parquet(out_path)
            logger.info(f"Successfully wrote {ext_cnt} records to {out_path}")

    except Exception as e:
        logger.exception()
    
    logger.info(f"Time taken to run {os.path.basename(__file__)} : {round(time.time() - start_time, 2)} seconds")
    spark.stop()


if __name__=="__main__":
    cust()

