from src.common.spark_session import get_spark_session
from src.common.logger import get_logger
from src.config.config_read import load_config
from pyspark.sql.functions import col, to_date, year, broadcast
import time, os

def catalog_returns():
    start_time=time.time()
    config=load_config()
    log_path=config['path']['logs']['dist_log']
    logger=get_logger("catalog_returns", log_path)
    logger.info("Creating SparkSession...........")
    spark=get_spark_session("catalog_returns", log_path)
    logger.info("SparkSession created")
    customer_path=config["path"]["gold_source"]["customer"]
    item_path=config["path"]["gold_source"]["item"]
    callcenter_path=config["path"]["gold_source"]["call_center"]
    returns_path=config["path"]["gold_source"]["catalog_returns"]
    catalog_page_path=config["path"]["gold_source"]["catalog_webpage"]
    datedim_path=config["path"]["gold_source"]["date_dim"]
    address_path=config["path"]["gold_source"]["customer_address"]
    reason_path=config["path"]["gold_source"]["reason"]
    output_path=config["path"]["Extract"]["catalog_returns"]
    try:
        logger.info(f"Customer dataframe creating from inbound path {customer_path}")
        df_customer=spark.read.format("parquet").load(customer_path)
        logger.info(f"items dataframe creating from inbound path {item_path}")
        df_item=spark.read.format("parquet").load(item_path)
        logger.info(f"Call center dataframe creating from inbound path {callcenter_path}")
        df_callcenter=spark.read.format("parquet").load(callcenter_path)
        logger.info(f"Returns dataframe creating from inbound path {returns_path}")
        df_returns=spark.read.format("parquet").load(returns_path)
        logger.info(f"date dataframe creating from inbound path {datedim_path}")
        df_date=spark.read.format("parquet").load(datedim_path)
        logger.info(f"address dataframe creating from inbound path {address_path}")
        df_address=spark.read.format("parquet").load(address_path)
        logger.info(f"Reason dataframe creating from inbound path {reason_path}")
        df_reason=spark.read.format("parquet").load(reason_path)
        logger.info(f"Catalog Page dataframe creating from inbound path {catalog_page_path}")
        df_page=spark.read.format("parquet").load(catalog_page_path)

        logger.info("Validating the dataframes")
        dataframes = {"customer": df_customer,
                      "items": df_item,
                      "call_center": df_callcenter,
                      "Returns": df_returns,
                      "date": df_date,
                      "address": df_address,
                      "Reason": df_reason,
                      "catalog_page": df_page
                      }

        for name, df in dataframes.items():
            if not df.head(1):
                logger.warning(f"{name} dataframe is empty")


        logger.info("Joining the dataframes")
        df_join=(df_returns.alias("df_returns").join(df_item.alias("df_item"), df_returns.cr_item_sk==df_item.i_item_sk, how="left") 
                          .join(df_customer.alias("cust1"), df_returns.cr_refunded_customer_sk==df_customer.c_customer_sk, how="left") 
                          .join(df_customer.alias("cust2"), df_returns.cr_returning_customer_sk==col("cust2.c_customer_sk"), how="left") 
                          .join(broadcast(df_reason).alias("df_reason"), df_returns.cr_reason_sk==df_reason.r_reason_sk, how="left") 
                          .join(broadcast(df_callcenter).alias("df_callcenter"), df_returns.cr_call_center_sk==df_callcenter.cc_call_center_sk, how="left") 
                          .join(df_page.alias("df_page"), df_returns.cr_catalog_page_sk==df_page.cp_catalog_page_sk, how="left") 
                          .join(df_date.alias("df_date"), df_returns.cr_returned_date_sk==df_date.d_date_sk, how="left") 
                          .join(df_address.alias("rfd_addr"), df_returns.cr_refunded_addr_sk==df_address.ca_address_sk, how="left") 
                          .join(df_address.alias("return_addr"), df_returns.cr_refunded_addr_sk==col("return_addr.ca_address_sk"), how="left") 
                          .select(col("df_date.d_date").alias("return_date"),
                                  col("cust1.c_customer_id").alias("refunded_customer_id"),
                                  col("cust1.c_first_name").alias("refunded_customer_first_name"),
                                  col("cust1.c_last_name").alias("refunded_customer_last_name"),
                                  col("cust2.c_customer_id").alias("returning_customer_id"),
                                  col("cust2.c_first_name").alias("returning_customer_first_name"),
                                  col("cust2.c_last_name").alias("returning_customer_last_name"),
                                  col("df_item.i_item_id").alias("item_id"),
                                  col("df_item.i_product_name").alias("product_name"),
                                  col("df_item.i_category").alias("category"),
                                  col("df_callcenter.cc_name").alias("call_center_name"),
                                  col("df_page.cp_catalog_number").alias("catalog_number"),
                                  col("df_reason.r_reason_desc").alias("return_reason"),
                                  col("rfd_addr.ca_country").alias("refunded_country"),
                                  col("rfd_addr.ca_zip").alias("refunded_zip"),
                                  col("return_addr.ca_country").alias("return_country"),
                                  col("return_addr.ca_zip").alias("return_zip"),
                                  col("df_returns.cr_order_number").alias("order_number"),
                                  col("df_returns.cr_return_quantity").alias("return_quantity"),
                                  col("df_returns.cr_return_amount").alias("return_amount"),
                                  col("df_returns.cr_return_tax").alias("return_tax"),
                                  col("df_returns.cr_net_loss").alias("Net_loss")    
                          ))
        
        df_final = df_join.withColumn("return_year", year(col("return_date")))
        logger.info(f"Writing {df_final.count()} records to {output_path} in csv format")
        logger.info("partitioning the data using year of sold_date after that, writing the data to output_path")
        df_final.write.partitionBy("return_year").mode("overwrite").option("header", "true").csv(output_path)
        logger.info("Successfully extracted the catalog Return Data")

    except Exception:
        logger.exception()

    logger.info(f"Time taken to run {os.path.basename(__file__)} : {round(time.time() - start_time, 2)} seconds")
    spark.stop()


if __name__=="__main__":
    #logger=get_logger("store_sales", "log.log")
    #logger.info(f"Executing the Return data by {__name__}")
    catalog_returns()
