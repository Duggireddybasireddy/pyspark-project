from src.common.spark_session import get_spark_session
from src.common.logger import get_logger
from src.config.config_read import load_config
from pyspark.sql.functions import col, to_date
import time, os

def store_sales():
    start_time=time.time()
    config=load_config()
    log_path=config['path']['logs']['std_log']
    logger=get_logger("std_layer", log_path)
    logger.info("Creating SparkSession...........")
    spark=get_spark_session("std_layer", log_path)
    inbound_file=config["path"]["std_source"]["store_sales"]
    out_path=config["path"]["gold_source"]["store_sales"]

    logger.info(f"Input file path is {inbound_file}")
    try:
        logger.info("Reading data from inbound path")
        df=spark.read.format("csv").option("header", "true").load(inbound_file)
        cnt=df.count()
        if not cnt:
            logger.info("File is empty")

        else:
            logger.info("Changing the datatype of attributes")
            df_data=df.select(col("ss_sold_date_sk").cast("string").alias("ss_sold_date_sk"),
                              col("ss_item_sk").cast("string").alias("ss_item_sk"),
                              col("ss_customer_sk").cast("string").alias("ss_customer_sk"),
                              col("ss_cdemo_sk").cast("string").alias("ss_cdemo_sk"),
                              col("ss_hdemo_sk").cast("string").alias("ss_hdemo_sk"),
                              col("ss_addr_sk").cast("string").alias("ss_addr_sk"),
                              col("ss_store_sk").cast("string").alias("ss_store_sk"),
                              col("ss_promo_sk").cast("string").alias("ss_promo_sk"),
                              col("ss_ticket_number").cast("string").alias("ss_ticket_number"),

                              col("ss_quantity").cast("int").alias("ss_quantity"),
                              col("ss_wholesale_cost").cast("decimal(18,2)").alias("ss_wholesale_cost"),
                              col("ss_list_price").cast("decimal(18,2)").alias("ss_list_price"),
                              col("ss_sales_price").cast("decimal(18,2)").alias("ss_sales_price"),
                              col("ss_ext_discount_amt").cast("decimal(18,2)").alias("ss_ext_discount_amt"),
                              col("ss_ext_sales_price").cast("decimal(18,2)").alias("ss_ext_sales_price"),
                              col("ss_ext_wholesale_cost").cast("decimal(18,2)").alias("ss_ext_wholesale_cost"),
                              col("ss_ext_list_price").cast("decimal(18,2)").alias("ss_ext_list_price"),
                              col("ss_ext_tax").cast("decimal(18,2)").alias("ss_ext_tax"),
                              col("ss_coupon_amt").cast("decimal(18,2)").alias("ss_coupon_amt"),
                              col("ss_net_paid").cast("decimal(18,2)").alias("ss_net_paid"),
                              col("ss_net_paid_inc_tax").cast("decimal(18,2)").alias("ss_net_paid_inc_tax"),
                              col("ss_net_profit").cast("decimal(18,2)").alias("ss_net_profit")
                              )

            logger.info("Performing the Data Validation")
            df_num=df_data.na.fill(0, subset=["ss_quantity", "ss_wholesale_cost", "ss_list_price", "ss_sales_price", "ss_ext_discount_amt", "ss_ext_sales_price", "ss_ext_wholesale_cost", "ss_ext_list_price", "ss_ext_tax", "ss_coupon_amt", "ss_net_paid", "ss_net_paid_inc_tax","ss_net_profit"])
            df_filter=df_num.where((col("ss_quantity") >= 0) &\
                                    (col("ss_wholesale_cost") >= 0) &\
                                    (col("ss_list_price") >= 0) & \
                                    (col("ss_sales_price") >= 0) & \
                                    (col("ss_ext_discount_amt") >= 0) & \
                                    (col("ss_ext_sales_price") >= 0) & \
                                    (col("ss_ext_wholesale_cost") >= 0) & \
                                    (col("ss_ext_list_price") >= 0) & \
                                    (col("ss_ext_tax") >= 0) & \
                                    (col("ss_coupon_amt") >= 0) & \
                                    (col("ss_net_profit") >= 0)
                                    ).na.drop(subset=["ss_sold_date_sk", "ss_item_sk", "ss_customer_sk", "ss_cdemo_sk","ss_hdemo_sk", "ss_addr_sk", "ss_store_sk", "ss_promo_sk"])

            ext_cnt =df_filter.count()
            diff = cnt - ext_cnt
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
    store_sales()