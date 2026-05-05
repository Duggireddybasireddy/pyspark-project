from src.common.spark_session import get_spark_session
from src.common.logger import get_logger
from src.config.config_read import load_config
from pyspark.sql.functions import col, to_date, broadcast
import time, os

def web_returns():
    start_time=time.time()
    config=load_config()
    log_path=config['path']['logs']['std_log']
    logger=get_logger("std_layer", log_path)
    logger.info("Creating SparkSession...........")
    spark=get_spark_session("std_layer", log_path)
    inbound_file=config["path"]["std_source"]["web_returns"]
    out_path=config["path"]["gold_source"]["web_returns"]

    logger.info(f"Input file path is {inbound_file}")
    try:
        logger.info("Reading data from inbound path")
        df=spark.read.format("csv").option("header", "true").load(inbound_file)
        cnt=df.count()
        if not cnt:
            logger.info("File is empty")

        else:
            logger.info("Changing the datatype of attributes")
            df_data=df.select(col("wr_returned_date_sk").cast("string").alias("wr_returned_date_sk"),
                              col("wr_item_sk").cast("string").alias("wr_item_sk"),
                              col("wr_refunded_cdemo_sk").cast("string").alias("wr_refunded_cdemo_sk"),
                              col("wr_refunded_hdemo_sk").cast("string").alias("wr_refunded_hdemo_sk"),
                              col("wr_refunded_addr_sk").cast("string").alias("wr_refunded_addr_sk"),
                              col("wr_returning_customer_sk").cast("string").alias("wr_returning_customer_sk"),
                              col("wr_refunded_customer_sk").cast("string").alias("wr_refunded_customer_sk"),
                              col("wr_returning_cdemo_sk").cast("string").alias("wr_returning_cdemo_sk"),
                              col("wr_returning_hdemo_sk").cast("string").alias("wr_returning_hdemo_sk"),
                              col("wr_returning_addr_sk").cast("string").alias("wr_returning_addr_sk"),
                              col("wr_web_page_sk").cast("string").alias("wr_web_page_sk"),
                              col("wr_reason_sk").cast("string").alias("wr_reason_sk"),
                              col("wr_order_number").cast("string").alias("wr_order_number"),

                              col("wr_return_quantity").cast("int").alias("wr_return_quantity"),
                              col("wr_return_amt").cast("decimal(18,2)").alias("wr_return_amt"),
                              col("wr_return_tax").cast("decimal(18,2)").alias("wr_return_tax"),
                              col("wr_return_amt_inc_tax").cast("decimal(18,2)").alias("wr_return_amt_inc_tax"),
                              col("wr_fee").cast("decimal(18,2)").alias("wr_fee"),
                              col("wr_return_ship_cost").cast("decimal(18,2)").alias("wr_return_ship_cost"),
                              col("wr_refunded_cash").cast("decimal(18,2)").alias("wr_refunded_cash"),
                              col("wr_reversed_charge").cast("decimal(18,2)").alias("wr_reversed_charge"),
                              col("wr_account_credit").cast("decimal(18,2)").alias("wr_account_credit"),
                              col("wr_net_loss").cast("decimal(18,2)").alias("wr_net_loss")
                              )

            logger.info("Performing the Data Validation")
            df_num=df_data.na.fill(0, subset=["wr_return_quantity", "wr_return_amt", "wr_return_tax", "wr_return_amt_inc_tax", "wr_fee", "wr_return_ship_cost", "wr_refunded_cash", "wr_reversed_charge", "wr_account_credit", "wr_net_loss"])
            df_filter=df_num.where((col("wr_return_quantity") >= 0) &\
                                    (col("wr_return_amt") >= 0) &\
                                    (col("wr_return_tax") >= 0) & \
                                    (col("wr_return_amt_inc_tax") >= 0) & \
                                    (col("wr_fee") >= 0) & \
                                    (col("wr_return_ship_cost") >= 0) & \
                                    (col("wr_refunded_cash") >= 0) & \
                                    (col("wr_reversed_charge") >= 0) & \
                                    (col("wr_account_credit") >= 0) & \
                                    (col("wr_net_loss") >= 0)
                                    ).na.drop(subset=["wr_returned_date_sk", "wr_item_sk", "wr_refunded_cdemo_sk", "wr_refunded_hdemo_sk", "wr_refunded_addr_sk", "wr_web_page_sk", "wr_reason_sk"])

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
    web_returns()