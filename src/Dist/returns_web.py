from src.common.spark_session import get_spark_session
from src.common.logger import get_logger
from src.config.config_read import load_config
from pyspark.sql.functions import col, to_date, year, broadcast
import time, os

def web_returns():
    start_time=time.time()
    config=load_config()
    log_path=config['path']['logs']['dist_log']
    logger=get_logger("web_returns", log_path)
    logger.info("Creating SparkSession...........")
    spark=get_spark_session("web_returns", log_path)
    logger.info("SparkSession created")
    customer_path=config["path"]["gold_source"]["customer"]
    datedim_path=config["path"]["gold_source"]["date_dim"]
    webpage_path=config["path"]["gold_source"]["web_page"]
    webreturns_path=config["path"]["gold_source"]["web_returns"]
    item_path=config["path"]["gold_source"]["item"]
    address_path=config["path"]["gold_source"]["customer_address"]
    reason_path=config["path"]["gold_source"]["reason"]
    output_path=config["path"]["Extract"]["web_returns"]
    try:
        logger.info(f"Customer dataframe creating from inbound path {customer_path}")
        df_customer=spark.read.format("parquet").load(customer_path)
        logger.info(f"web_page dataframe creating from inbound path {webpage_path}")
        df_webpage=spark.read.format("parquet").load(webpage_path)
        logger.info(f"website dataframe creating from inbound path {webreturns_path}")
        df_webreturns=spark.read.format("parquet").load(webreturns_path)
        logger.info(f"date dataframe creating from inbound path {datedim_path}")
        df_date=spark.read.format("parquet").load(datedim_path)
        logger.info(f"items dataframe creating from inbound path {item_path}")
        df_item=spark.read.format("parquet").load(item_path)
        logger.info(f"Address dataframe  creating from inbound path {address_path}")
        df_address=spark.read.format("parquet").load(address_path)
        logger.info(f"Reason dataframe creating from inbound parh {reason_path}")
        df_reason=spark.read.format("parquet").load(reason_path)

        logger.info("Validating the dataframes")
        dataframes = {"customer": df_customer,
                      "webpage": df_webpage,
                      "webreturns": df_webreturns,
                      "date": df_date,
                      "item": df_item,
                      "address": df_address,
                      "reason": df_reason
                      }
    
        for name, df in dataframes.items():
            if not df.head(1):
                logger.warning(f"{name} dataframe is empty")
        
        logger.info("Joining the dataframes")
        df_join=(df_webreturns.alias("df_webreturns").join(df_item.alias("df_item"), df_webreturns.wr_item_sk==df_item.i_item_sk, how="left") 
                             .join(df_customer.alias("cust1"), df_webreturns.wr_refunded_customer_sk==df_customer.c_customer_sk, how="left") 
                             .join(df_customer.alias("cust2"), df_webreturns.wr_returning_customer_sk==col("cust2.c_customer_sk"), how="left") 
                             .join(broadcast(df_reason).alias("df_reason"), df_webreturns.wr_reason_sk==df_reason.r_reason_sk, how="left") 
                             .join(df_webpage.alias("df_webpage"), df_webreturns.wr_web_page_sk==df_webpage.wp_web_page_sk, how="left") 
                             .join(df_date.alias("df_date"), df_webreturns.wr_returned_date_sk==df_date.d_date_sk, how="left") 
                             .join(df_address.alias("addr1"), df_webreturns.wr_refunded_addr_sk==df_address.ca_address_sk, how="left") 
                             .join(df_address.alias("addr2"), df_webreturns.wr_returning_addr_sk==col("addr2.ca_address_sk"), how="left")
                             .select(col("df_date.d_date").alias("return_date"),
                                     col("cust1.c_customer_id").alias("refunded_customer_id"),
                                     col("cust1.c_first_name").alias("refunded_first_name"),
                                     col("cust1.c_last_name").alias("refunded_last_name"),
                                     col("cust2.c_customer_id").alias("returning_customer_id"),
                                     col("cust2.c_first_name").alias("returning_first_name"),
                                     col("cust2.c_last_name").alias("returning_last_name"),
                                     col("df_item.i_item_id").alias("item_id"),
                                     col("df_item.i_product_name").alias("product_name"),
                                     col("df_item.i_category").alias("category"),
                                     col("df_webpage.wp_type").alias("page_type"),
                                     col("df_reason.r_reason_desc").alias("return_reason"),
                                     col("addr1.ca_country").alias("refunded_country"),
                                     col("addr1.ca_zip").alias("refunded_zip"),
                                     col("addr2.ca_country").alias("return_country"),
                                     col("addr2.ca_zip").alias("return_zip"),
                                     col("df_webreturns.wr_order_number").alias("order_number"),
                                     col("df_webreturns.wr_return_quantity").alias("return_quantity"),
                                     col("df_webreturns.wr_return_amt").alias("return_amount"),
                                     col("df_webreturns.wr_return_tax").alias("return_tax"),
                                     col("df_webreturns.wr_net_loss").alias("Net_loss")
                             ))

        df_final = df_join.withColumn("return_year", year(col("return_date")))
        logger.info(f"Writing {df_final.count()} records to {output_path} in csv format")
        logger.info("partitioning the data using year of sold_date after that, writing the data to output_path")
        df_final.write.partitionBy("return_year").mode("overwrite").option("header", "true").csv(output_path)
        logger.info("Successfully extracted the web returns Data")

    except Exception:
        logger.exception()

    logger.info(f"Time taken to run {os.path.basename(__file__)} : {round(time.time() - start_time, 2)} seconds")
    spark.stop()


if __name__=="__main__":
    web_returns()
