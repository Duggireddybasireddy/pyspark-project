from src.common.spark_session import get_spark_session
from src.common.logger import get_logger
from src.config.config_read import load_config
from pyspark.sql.functions import col, to_date
import time, os

def catalog_sales():
    start_time=time.time()
    config=load_config()
    log_path=config['path']['logs']['std_log']
    logger=get_logger("std_layer", log_path)
    logger.info("Creating SparkSession...........")
    spark=get_spark_session("std_layer", log_path)
    inbound_file=config["path"]["std_source"]["catalog_sales"]
    out_path=config["path"]["gold_source"]["catalog_sales"]

    logger.info(f"Input file path is {inbound_file}")
    try:
        logger.info("Reading data from inbound path")
        df=spark.read.format("csv").option("header", "true").load(inbound_file)
        cnt=df.count()
        if not cnt:
            logger.info("File is empty")

        else:
            logger.info("Changing the datatype of attributes")
            df_data=df.select(col("cs_sold_date_sk").cast("string").alias("cs_sold_date_sk"),
                              col("cs_ship_date_sk").cast("string").alias("cs_ship_date_sk"),
                              col("cs_bill_customer_sk").cast("string").alias("cs_bill_customer_sk"),
                              col("cs_bill_cdemo_sk").cast("string").alias("cs_bill_cdemo_sk"),
                              col("cs_bill_hdemo_sk").cast("string").alias("cs_bill_hdemo_sk"),
                              col("cs_bill_addr_sk").cast("string").alias("cs_bill_addr_sk"),
                              col("cs_ship_customer_sk").cast("string").alias("cs_ship_customer_sk"),
                              col("cs_ship_cdemo_sk").cast("string").alias("cs_ship_cdemo_sk"),
                              col("cs_ship_hdemo_sk").cast("string").alias("cs_ship_hdemo_sk"),
                              col("cs_ship_addr_sk").cast("string").alias("cs_ship_addr_sk"),
                              col("cs_call_center_sk").cast("string").alias("cs_call_center_sk"),
                              col("cs_catalog_page_sk").cast("string").alias("cs_catalog_page_sk"),
                              col("cs_ship_mode_sk").cast("string").alias("cs_ship_mode_sk"),
                              col("cs_warehouse_sk").cast("string").alias("cs_warehouse_sk"),
                              col("cs_item_sk").cast("string").alias("cs_item_sk"),
                              col("cs_promo_sk").cast("string").alias("cs_promo_sk"),
                              col("cs_order_number").cast("string").alias("cs_order_number"),

                              col("cs_quantity").cast("int").alias("cs_quantity"),
                              col("cs_wholesale_cost").cast("decimal(18,2)").alias("cs_wholesale_cost"),
                              col("cs_list_price").cast("decimal(18,2)").alias("cs_list_price"),
                              col("cs_sales_price").cast("decimal(18,2)").alias("cs_sales_price"),
                              col("cs_ext_discount_amt").cast("decimal(18,2)").alias('cs_ext_discount_amt'),
                              col("cs_ext_sales_price").cast("decimal(18,2)").alias("cs_ext_sales_price"),
                              col("cs_ext_wholesale_cost").cast("decimal(18,2)").alias("cs_ext_wholesale_cost"),
                              col("cs_ext_list_price").cast("decimal(18,2)").alias("cs_ext_list_price"),
                              col("cs_ext_tax").cast("decimal(18,2)").alias("cs_ext_tax"),
                              col("cs_coupon_amt").cast("decimal(18,2)").alias("cs_coupon_amt"),
                              col("cs_ext_ship_cost").cast("decimal(18,2)").alias("cs_ext_ship_cost"),
                              col("cs_net_paid").cast("decimal(18,2)").alias("cs_net_paid"),
                              col("cs_net_paid_inc_tax").cast("decimal(18,2)").alias("cs_net_paid_inc_tax"),
                              col("cs_net_paid_inc_ship").cast("decimal(18,2)").alias("cs_net_paid_inc_ship"),
                              col("cs_net_paid_inc_ship_tax").cast("decimal(18,2)").alias("cs_net_paid_inc_ship_tax"),
                              col("cs_net_profit").cast("decimal(18,2)").alias("cs_net_profit")
                              )

            logger.info("Performing the Data Validation")
            df_num=df_data.na.fill(0,subset=["cs_wholesale_cost", "cs_quantity", "cs_list_price", "cs_sales_price", "cs_ext_discount_amt", "cs_ext_sales_price", "cs_ext_wholesale_cost", "cs_ext_list_price", "cs_ext_tax", "cs_coupon_amt", "cs_ext_ship_cost", "cs_net_paid", "cs_net_paid_inc_tax", "cs_net_paid_inc_ship", "cs_net_paid_inc_ship_tax", "cs_net_profit"])
            df_filter=df_num.na.drop(subset=["cs_sold_date_sk", "cs_ship_date_sk", "cs_bill_customer_sk", "cs_bill_cdemo_sk", "cs_bill_hdemo_sk", "cs_bill_addr_sk", "cs_ship_customer_sk", "cs_call_center_sk", "cs_catalog_page_sk", "cs_ship_mode_sk", "cs_warehouse_sk", "cs_item_sk", "cs_promo_sk", "cs_order_number"])
            ext_cnt=df_filter.count()
            diff=cnt - ext_cnt
            logger.info(f"Total records read is {cnt}")
            logger.info(f"Dropped Records count is {diff}")
            logger.info(f"Writing {ext_cnt} records to {out_path} in csv format")
            df_filter.coalesce(3).write.mode("overwrite").parquet(out_path)
            logger.info(f"Successfully wrote {ext_cnt} records to {out_path}")
    except Exception as e:
        logger.exception("File Not Found")
    
    logger.info(f"Time taken to run {os.path.basename(__file__)} : {round(time.time() - start_time, 2)} seconds")
    spark.stop()

if __name__=="__main__":
    catalog_sales()