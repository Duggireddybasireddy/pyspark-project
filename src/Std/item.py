from src.common.spark_session import get_spark_session
from src.common.logger import get_logger
from src.config.config_read import load_config
from pyspark.sql.functions import col, to_date
import time, os

def item():
    start_time=time.time()
    config=load_config()
    log_path=config['path']['logs']['std_log']
    logger=get_logger("std_layer", log_path)
    logger.info("Creating SparkSession...........")
    spark=get_spark_session("std_layer", log_path)
    inbound_file=config["path"]["std_source"]["item"]
    out_path=config["path"]["gold_source"]["item"]

    logger.info(f"Input file path is {inbound_file}")
    try:
        logger.info("Reading data from inbound path")
        df=spark.read.format("csv").option("header", "true").load(inbound_file)
        cnt=df.count()
        if not cnt:
            logger.info("File is empty")

        else:
            logger.info("Changing the datatype of attributes")
            df_data=df.select(col("i_item_sk").cast("string").alias("i_item_sk"),
                              col("i_item_id").cast("string").alias("i_item_id"),
                              to_date(col("i_rec_start_date"), "yyyy-MM-dd").alias("i_rec_start_date"),
                              to_date(col("i_rec_end_date"), "yyyy-MM-dd").alias("i_rec_end_date"),
                              col("i_item_desc").cast("string").alias("i_item_desc"),
                              col("i_current_price").cast("double").alias("i_current_price"),
                              col("i_wholesale_cost").cast("double").alias("i_wholesale_cost"),
                              col("i_brand_id").cast("string").alias("i_brand_id"),
                              col("i_brand").cast("string").alias("i_brand"),
                              col("i_class_id").cast("string").alias("i_class_id"),
                              col("i_class").cast("string").alias("i_class"),
                              col("i_category_id").cast("string").alias("i_category_id"),
                              col("i_category").cast("string").alias("i_category"),
                              col("i_manufact_id").cast("string").alias("i_manufact_id"),
                              col("i_manufact").cast("string").alias("i_manufact"),
                              col("i_size").cast("double").alias("i_size"),
                              col("i_formulation").cast("string").alias("i_formulation"),
                              col("i_color").cast("string").alias("i_color"),
                              col("i_units").cast("int").alias("i_units"),
                              col("i_container").cast("string").alias("i_container"),
                              col("i_manager_id").cast("string").alias("i_manager_id"),
                              col("i_product_name").cast("string").alias("i_product_name")
                              )

            logger.info("Performing the Data Validation")
            df_num=df_data.na.fill(0, subset=["i_current_price", "i_wholesale_cost", "i_units","i_size"])
            df_filter=df_num.where((col("i_current_price") >= 0) & \
                                    (col("i_wholesale_cost") >= 0) & \
                                    (col("i_units") >= 0) & 
                                    ((col("i_size") >= 0)) & \
                                    (col("i_rec_start_date") <= col("i_rec_end_date"))).na.drop(subset=["i_item_sk", "i_category_id", "i_manager_id", "i_manufact_id"])

            ext_cnt = df_filter.count()
            diff =cnt - ext_cnt
            logger.info(f"Total records read is {cnt}")
            logger.info(f"Dropped Records count is {diff}")
            logger.info(f"Writing {ext_cnt} records to {out_path} in csv format")
            df_filter.coalesce(1).write.mode("overwrite").parquet(out_path)
            logger.info(f"Successfully wrote {ext_cnt} records to {out_path}")
            
    except Exception as e:
        logger.exception()
    
    logger.info(f"Time taken to run {os.path.basename(__file__)} : {round(time.time() - start_time, 2)} seconds")
    spark.stop()

if __name__=="__main__":
    item()