from src.common.spark_session import get_spark_session
from src.common.logger import get_logger
from src.config.config_read import load_config
from pyspark.sql.functions import col, to_date
import time, os

def web_sales():
    start_time=time.time()
    config=load_config()
    log_path=config['path']['logs']['std_log']
    logger=get_logger("std_layer", log_path)
    logger.info("Creating SparkSession...........")
    spark=get_spark_session("std_layer", log_path)
    inbound_file=config["path"]["std_source"]["web_sales"]
    out_path=config["path"]["gold_source"]["web_sales"]

    logger.info(f"Input file path is {inbound_file}")
    try:
        logger.info("Reading data from inbound path")
        df=spark.read.format("csv").option("header", "true").load(inbound_file)
        cnt=df.count()
        if not cnt:
            logger.info("File is empty")

        else:
            logger.info("Changing the datatype of attributes")
            df_data=df.select(col("ws_sold_date_sk").cast("string").alias("ws_sold_date_sk"),
                              col("ws_ship_date_sk").cast("string").alias("ws_ship_date_sk"),
                              col("ws_item_sk").cast("string").alias("ws_item_sk"),
                              col("ws_bill_customer_sk").cast("string").alias("ws_bill_customer_sk"),
                              col("ws_bill_cdemo_sk").cast("string").alias("ws_bill_cdemo_sk"),
                              col("ws_bill_hdemo_sk").cast("string").alias("ws_bill_hdemo_sk"),
                              col("ws_bill_addr_sk").cast("string").alias("ws_bill_addr_sk"),
                              col("ws_ship_customer_sk").cast("string").alias("ws_ship_customer_sk"),
                              col("ws_ship_cdemo_sk").cast("string").alias("ws_ship_cdemo_sk"),
                              col("ws_ship_hdemo_sk").cast("string").alias("ws_ship_hdemo_sk"),
                              col("ws_ship_addr_sk").cast("string").alias("ws_ship_addr_sk"),
                              col("ws_web_page_sk").cast("string").alias("ws_web_page_sk"),
                              col("ws_web_site_sk").cast("string").alias("ws_web_site_sk"),
                              col("ws_ship_mode_sk").cast("string").alias("ws_ship_mode_sk"),
                              col("ws_warehouse_sk").cast("string").alias("ws_warehouse_sk"),
                              col("ws_order_number").cast("string").alias("ws_order_number"),

                              col("ws_quantity").cast("int").alias("ws_quantity"),
                              col("ws_wholesale_cost").cast("decimal(18,2)").alias("ws_wholesale_cost"),
                              col("ws_list_price").cast("decimal(18,2)").alias("ws_list_price"),
                              col("ws_sales_price").cast("decimal(18,2)").alias("ws_sales_price"),
                              col("ws_ext_discount_amt").cast("decimal(18,2)").alias("ws_ext_discount_amt"),
                              col("ws_ext_sales_price").cast("decimal(18,2)").alias("ws_ext_sales_price"),
                              col("ws_ext_wholesale_cost").cast("decimal(18,2)").alias("ws_ext_wholesale_cost"),
                              col("ws_ext_list_price").cast("decimal(18,2)").alias("ws_ext_list_price"),
                              col("ws_ext_tax").cast("decimal(18,2)").alias("ws_ext_tax"),
                              col("ws_coupon_amt").cast("decimal(18,2)").alias("ws_coupon_amt"),
                              col("ws_ext_ship_cost").cast("decimal(18,2)").alias("ws_ext_ship_cost"),
                              col("ws_net_paid").cast("decimal(18,2)").alias("ws_net_paid"),
                              col("ws_net_paid_inc_tax").cast("decimal(18,2)").alias("ws_net_paid_inc_tax"),
                              col("ws_net_paid_inc_ship").cast("decimal(18,2)").alias("ws_net_paid_inc_ship"),
                              col("ws_net_paid_inc_ship_tax").cast("decimal(18,2)").alias("ws_net_paid_inc_ship_tax"),
                              col("ws_net_profit").cast("decimal(18,2)").alias("ws_net_profit")
                              )

            logger.info("Performing the Data Validation")
            df_num=df_data.na.fill(0, subset=["ws_quantity", "ws_wholesale_cost", "ws_list_price", "ws_sales_price", "ws_ext_discount_amt", "ws_ext_sales_price", "ws_ext_wholesale_cost", "ws_ext_list_price", "ws_ext_tax", "ws_coupon_amt", "ws_ext_ship_cost", "ws_net_paid", "ws_net_paid_inc_tax", "ws_net_paid_inc_ship", "ws_net_paid_inc_ship_tax", "ws_net_profit"])
            df_filter=df_num.where((col("ws_quantity") >= 0) &\
                                    (col("ws_wholesale_cost") >= 0) &\
                                    (col("ws_list_price") >= 0) & \
                                    (col("ws_sales_price") >= 0) & \
                                    (col("ws_ext_discount_amt") >= 0) & \
                                    (col("ws_ext_sales_price") >= 0) & \
                                    (col("ws_ext_wholesale_cost") >= 0) & \
                                    (col("ws_ext_list_price") >= 0) & \
                                    (col("ws_ext_tax") >= 0) & \
                                    (col("ws_coupon_amt") >= 0) & \
                                    (col("ws_ext_ship_cost") >= 0) & \
                                    (col("ws_net_paid") >= 0) & \
                                    (col("ws_net_paid_inc_tax") >= 0) & \
                                    (col("ws_net_paid_inc_ship") >= 0) & \
                                    (col("ws_net_paid_inc_ship_tax") >= 0) &\
                                    (col("ws_net_profit") >=0)
                                    ).na.drop(subset=["ws_sold_date_sk", "ws_ship_date_sk", "ws_item_sk", "ws_bill_customer_sk", "ws_bill_cdemo_sk", "ws_bill_hdemo_sk", "ws_bill_addr_sk", "ws_ship_customer_sk", "ws_ship_cdemo_sk", "ws_ship_hdemo_sk", "ws_ship_addr_sk", "ws_web_page_sk", "ws_ship_mode_sk", "ws_warehouse_sk"])

            ext_cnt = df_filter.count()
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
    web_sales()