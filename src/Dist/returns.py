from src.common.spark_session import get_spark_session
from src.common.logger import get_logger
from src.config.config_read import load_config
from pyspark.sql.functions import col, to_date, year, broadcast
import time, os

def store_returns():
    start_time=time.time()
    config=load_config()
    log_path=config['path']['logs']['dist_log']
    logger=get_logger("store_returns", log_path)
    logger.info("Creating SparkSession...........")
    spark=get_spark_session("store_returns", log_path)
    
    logger.info("SparkSession created")
    customer_path=config["path"]["gold_source"]["customer"]
    item_path=config["path"]["gold_source"]["item"]
    store_path=config["path"]["gold_source"]["store"]
    storereturns_path=config["path"]["gold_source"]["store_returns"]
    datedim_path=config["path"]["gold_source"]["date_dim"]
    address_path=config["path"]["gold_source"]["customer_address"]
    reason_path=config["path"]["gold_source"]["reason"]
    output_path=config["path"]["Extract"]["returns"]
    try:
        logger.info(f"Customer dataframe creating from inbound path {customer_path}")
        df_customer=spark.read.format("parquet").load(customer_path)
        logger.info(f"items dataframe creating from inbound path {item_path}")
        df_items=spark.read.format("parquet").load(item_path)
        logger.info(f"store dataframe creating from inbound path {store_path}")
        df_store=spark.read.format("parquet").load(store_path)
        logger.info(f"Returns dataframe creating from inbound path {storereturns_path}")
        df_returns=spark.read.format("parquet").load(storereturns_path)
        logger.info(f"date dataframe creating from inbound path {datedim_path}")
        df_date=spark.read.format("parquet").load(datedim_path)
        logger.info(f"address dataframe creating from inbound path {address_path}")
        df_address=spark.read.format("parquet").load(address_path)
        logger.info(f"Reason dataframe creating from inbound path {reason_path}")
        df_reason=spark.read.format("parquet").load(reason_path)

        logger.info("Validating the dataframes")
        dataframes = {"customer": df_customer,
                      "items": df_items,
                      "store": df_store, 
                      "Returns": df_returns,
                      "data": df_date, 
                      "address": df_address,
                      "Reason": df_reason
                      }
        for name, df in dataframes.items():
            if not df.head(1):
                logger.warning(f"{name} dataframe is empty")

        logger.info("Joining the dataframes")
        df_join=df_returns.alias("df_returns").join(df_items.alias("df_items"), df_returns.sr_item_sk==df_items.i_item_sk, how="left") \
                          .join(df_customer.alias("df_customer"), df_returns.sr_customer_sk==df_customer.c_customer_sk, how="left") \
                          .join(df_store.alias("df_store"), df_returns.sr_store_sk==df_store.s_store_sk, how="left") \
                          .join(broadcast(df_reason).alias("df_reason"), df_returns.sr_reason_sk==df_reason.r_reason_sk, how="left") \
                          .join(df_date.alias("df_date"), df_returns.sr_returned_date_sk==df_date.d_date_sk, how="left") \
                          .join(df_address.alias("df_address"), df_returns.sr_addr_sk==df_address.ca_address_sk, how="inner") \
                          .select(col("df_date.d_date").alias("Return_date"),
                                  col("df_customer.c_customer_id").alias("customer_id"),
                                  col("df_customer.c_first_name").alias("first_name"),
                                  col("df_customer.c_last_name").alias("last_name"),
                                  col("df_items.i_item_id").alias("item_id"),
                                  col("df_items.i_product_name").alias("product_name"),
                                  col("df_items.i_category").alias("category"),
                                  col("df_store.s_store_name").alias("store_name"),
                                  col("df_store.s_city").alias("store_city"),
                                  col("df_store.s_state").alias("store_state"),
                                  col("df_reason.r_reason_desc").alias("return_reason"),
                                  col("df_address.ca_country").alias("country"),
                                  col("df_address.ca_zip").alias("zip"),
                                  col("df_returns.sr_ticket_number").alias("ticket_number"),
                                  col("df_returns.sr_return_quantity").alias("return_quantity"),
                                  col("df_returns.sr_return_amt").alias("return_amount"),
                                  col("df_returns.sr_return_tax").alias("return_tax"),
                                  col("df_returns.sr_net_loss").alias("Net_loss")
                          )
        
        df_final = df_join.withColumn("return_year", year(col("Return_date")))
        logger.info(f"Writing {df_final.count()} records to {output_path} in csv format")
        logger.info("partitioning the data using year of Return_date after that, writing the data to output_path")
        df_final.write.partitionBy("return_year").mode("overwrite").option("header", "true").csv(output_path)
        logger.info("Successfully extracted the Orders Return Data")

    except Exception:
        logger.exception()

    logger.info(f"Time taken to run {os.path.basename(__file__)} : {round(time.time() - start_time, 2)} seconds")
    spark.stop()


if __name__=="__main__":
    store_returns()
