from src.common.spark_session import get_spark_session
from src.common.logger import get_logger
from src.config.config_read import load_config
from pyspark.sql.functions import col, to_date, year, broadcast
import time, os

def web_sales():
    start_time=time.time()
    config=load_config()
    log_path=config['path']['logs']["dist_log"]
    logger=get_logger("web_sales", log_path)
    logger.info("Creating SparkSession...........")
    spark=get_spark_session("web_sales", log_path)
    logger.info("SparkSession created")
    customer_path=config["path"]["gold_source"]["customer"]
    datedim_path=config["path"]["gold_source"]["date_dim"]
    webpage_path=config["path"]["gold_source"]["web_page"]
    website_path=config["path"]["gold_source"]["web_site"]
    websales_path=config["path"]["gold_source"]["web_sales"]
    shipmode_path=config["path"]["gold_source"]["ship_mode"]
    item_path=config["path"]["gold_source"]["item"]
    address_path=config["path"]["gold_source"]["customer_address"]
    output_path=config["path"]["Extract"]["web_sales"]
    try:
        logger.info(f"Customer dataframe creating from inbound path {customer_path}")
        df_customer=spark.read.format("parquet").load(customer_path)
        logger.info(f"web_page dataframe creating from inbound path {webpage_path}")
        df_webpage=spark.read.format("parquet").load(webpage_path)
        logger.info(f"website dataframe creating from inbound path {website_path}")
        df_website=spark.read.format("parquet").load(website_path)
        logger.info(f"websales dataframe creating from inbound path {websales_path}")
        df_websales=spark.read.format("parquet").load(websales_path)
        logger.info(f"date dataframe creating from inbound path {datedim_path}")
        df_date=spark.read.format("parquet").load(datedim_path)
        logger.info(f"ship_mode dataframe creating from inbound path {shipmode_path}")
        df_shipmode=spark.read.format("parquet").load(shipmode_path)
        logger.info(f"items dataframe creating from inbound path {item_path}")
        df_item=spark.read.format("parquet").load(item_path)
        logger.info(f"Address dataframe  creating from inbound path {address_path}")
        df_address=spark.read.format("parquet").load(address_path)

        logger.info("Validating the dataframes")
        dataframes = {"customer": df_customer,
                      "webpage": df_webpage,
                      "website": df_website, 
                      "websales": df_websales,
                      "date": df_date,
                      "ship_mode": df_shipmode,
                      "item": df_item,
                      "address": df_address
                      }
    
        for name, df in dataframes.items():
            if not df.head(1):
                logger.warning(f"{name} dataframe is empty")
                
        logger.info("Joining the dataframes")
        df_join=df_websales.alias("df_websales").join(df_item.alias("df_item"), df_websales.ws_item_sk==df_item.i_item_sk, how="left") \
                           .join(df_customer.alias("cust"), df_websales.ws_bill_customer_sk==df_customer.c_customer_sk, how="left") \
                           .join(df_customer.alias("ship_cust"), df_websales.ws_ship_customer_sk==col("ship_cust.c_customer_sk"), how="left") \
                           .join(df_webpage.alias("df_webpage"), df_websales.ws_web_page_sk==df_webpage.wp_web_page_sk, how="left") \
                           .join(df_website.alias("df_website"), df_websales.ws_web_site_sk==df_website.web_site_sk, how="left") \
                           .join(broadcast(df_shipmode).alias("df_shipmode"), df_websales.ws_ship_mode_sk==df_shipmode.sm_ship_mode_sk, how="left") \
                           .join(df_date.alias("sold_d"), df_websales.ws_sold_date_sk==df_date.d_date_sk, how="left") \
                           .join(df_date.alias("ship_d"), df_websales.ws_ship_date_sk==col("ship_d.d_date_sk"), how="left") \
                           .join(df_address.alias("bill_addr"), df_websales.ws_bill_addr_sk==df_address.ca_address_sk, how="left") \
                           .join(df_address.alias("ship_addr"), df_websales.ws_ship_addr_sk==col("ship_addr.ca_address_sk"), how="left") \
                           .select(col("sold_d.d_date").alias("sold_date"),
                                   col("ship_d.d_date").alias("ship_date"),
                                   col("cust.c_customer_id").alias("billing_customer_id"),
                                   col("cust.c_first_name").alias("billing_first_name"),
                                   col("cust.c_last_name").alias("billing_last_name"),
                                   col("ship_cust.c_customer_id").alias("shipping_customer_id"),
                                   col("ship_cust.c_first_name").alias("shipping_first_name"),
                                   col("ship_cust.c_last_name").alias("shipping_last_name"),
                                   col("df_item.i_item_id").alias("item_id"),
                                   col("df_item.i_product_name").alias("product_name"),
                                   col("df_item.i_category").alias("category"),
                                   col("df_website.web_name").alias("website_name"),
                                   col("df_webpage.wp_type").alias("webpage_type"),
                                   col("df_shipmode.sm_type").alias("ship_mode"),
                                   col("df_shipmode.sm_carrier").alias("carrier"),
                                   col("bill_addr.ca_country").alias("billing_country"),
                                   col("bill_addr.ca_zip").alias("billing_zip"),
                                   col("ship_addr.ca_country").alias("shipping_country"),
                                   col("ship_addr.ca_zip").alias("shipping_zip"),
                                   col("df_websales.ws_quantity").alias("quantity"),
                                   col("df_websales.ws_sales_price").alias("sales_price"),
                                   col("df_websales.ws_ext_sales_price").alias("Total_sales"),
                                   col("df_websales.ws_ext_discount_amt").alias("Discount_amount"),
                                   col("df_websales.ws_ext_tax").alias("Tax_amount"),
                                   col("df_websales.ws_ext_ship_cost").alias("shipping_cost"),
                                   col("df_websales.ws_net_paid").alias("Net_paid"),
                                   col("df_websales.ws_net_profit").alias("Net_profit")
                           )

        df_final = df_join.withColumn("sold_year", year(col("sold_date")))
        logger.info(f"Writing {df_final.count()} records to {output_path} in csv format")
        logger.info("partitioning the data using year of sold_date after that, writing the data to output_path")
        df_final.write.partitionBy("sold_year").mode("overwrite").option("header", "true").csv(output_path)
        logger.info("Successfully extracted the web Sales Data")

    except Exception:
        logger.exception()

    logger.info(f"Time taken to run {os.path.basename(__file__)} : {round(time.time() - start_time, 2)} seconds")
    spark.stop()


if __name__=="__main__":
    web_sales()
