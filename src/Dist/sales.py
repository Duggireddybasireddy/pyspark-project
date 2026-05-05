from src.common.spark_session import get_spark_session
from src.common.logger import get_logger
from src.config.config_read import load_config
from pyspark.sql.functions import col, to_date, year, broadcast
import time, os

def store_sales():
    start_time=time.time()
    config=load_config()
    log_path=config['path']['logs']['dist_log']
    logger=get_logger("store_sales", log_path)
    logger.info("Creating SparkSession...........")
    spark=get_spark_session("store_sales", log_path)
    logger.info("SparkSession created")
    customer_path=config["path"]["gold_source"]["customer"]
    item_path=config["path"]["gold_source"]["item"]
    store_path=config["path"]["gold_source"]["store"]
    storesales_path=config["path"]["gold_source"]["store_sales"]
    datedim_path=config["path"]["gold_source"]["date_dim"]
    address_path=config["path"]["gold_source"]["customer_address"]
    customer_demographics_path=config["path"]["gold_source"]["customer_demographics"]
    household_demographics_path=config["path"]["gold_source"]["household_demographics"]
    output_path=config["path"]["Extract"]["sales"]
    try:
        logger.info(f"Customer dataframe creating from inbound path {customer_path}")
        df_customer=spark.read.format("parquet").load(customer_path)
        logger.info(f"items dataframe creating from inbound path {item_path}")
        df_items=spark.read.format("parquet").load(item_path)
        logger.info(f"store dataframe creating from inbound path {store_path}")
        df_store=spark.read.format("parquet").load(store_path)
        logger.info(f"sales dataframe creating from inbound path {storesales_path}")
        df_sales=spark.read.format("parquet").load(storesales_path)
        logger.info(f"date dataframe creating from inbound path {datedim_path}")
        df_date=spark.read.format("parquet").load(datedim_path)
        logger.info(f"address dataframe creating from inbound path {address_path}")
        df_address=spark.read.format("parquet").load(address_path)
        logger.info(f"customer_demographics dataframe creating from inbound path {customer_demographics_path}")
        df_cdemographics=spark.read.format("parquet").load(customer_demographics_path)
        logger.info(f"household_demographics dataframe creating from inbound path {household_demographics_path}")
        df_hdemographics=spark.read.format("parquet").load(household_demographics_path)

        logger.info("Validating the dataframes")
        dataframes = {"customer": df_customer,
                      "items": df_items,
                      "store": df_store, 
                      "sales": df_sales,
                      "data": df_date, 
                      "address": df_address,
                      "customer_demographics": df_cdemographics,
                      "household_demographics": df_hdemographics
                      }
        for name, df in dataframes.items():
            if not df.head(1):
                logger.warning(f"{name} dataframe is empty")
            
        logger.info("Joining the dataframes")
        df_join=df_sales.alias("df_sales").join(df_customer.alias("df_customer"), df_sales.ss_customer_sk==df_customer.c_customer_sk, how="left") \
                        .join(df_items.alias("df_items"), df_sales.ss_item_sk==df_items.i_item_sk, how="left") \
                        .join(broadcast(df_store).alias("df_store"), df_sales.ss_store_sk==df_store.s_store_sk, how="left") \
                        .join(df_date.alias("df_date"), df_sales.ss_sold_date_sk==df_date.d_date_sk, how="left") \
                        .join(df_address.alias("df_address"), df_sales.ss_addr_sk==df_address.ca_address_sk, how="left") \
                        .join(df_cdemographics.alias("df_cdemographics"), df_sales.ss_cdemo_sk==df_cdemographics.cd_demo_sk, how="left") \
                        .join(df_hdemographics.alias("df_hdemographics"), df_sales.ss_hdemo_sk==df_hdemographics.hd_demo_sk, how="left") \
                        .select(col("df_sales.ss_ticket_number").alias("transaction_id"),
                                col("df_date.d_date").alias("sold_date"),
                                col("df_customer.c_customer_id").alias("customer_id"),
                                col("df_items.i_item_id").alias("item_id"),
                                col("df_store.s_store_id").alias("store_id"),
                                col("df_customer.c_first_name").alias("first_name"),
                                col("df_customer.c_last_name").alias("last_name"),
                                col("df_cdemographics.cd_gender").alias("gender"),
                                col("df_cdemographics.cd_marital_status").alias("marital_status"),
                                col("df_address.ca_country").alias("country"),
                                col("df_address.ca_zip").alias("zip"),
                                col("df_items.i_product_name").alias("product_name"),
                                col("df_items.i_category").alias("category"),
                                col("df_items.i_brand").alias("brand"),
                                col("df_store.s_store_name").alias("store_name"),
                                col("df_store.s_city").alias("store_city"),
                                col("df_store.s_state").alias("store_state"),
                                col("df_store.s_country").alias("store_country"),
                                col("df_sales.ss_quantity").alias("quantity"),
                                col("df_sales.ss_sales_price").alias("sales_price"),
                                col("df_sales.ss_ext_sales_price").alias("ext_sales_price"),
                                col("df_sales.ss_ext_discount_amt").alias("ext_discount_amt"),
                                col("df_sales.ss_ext_tax").alias("ext_tax"),
                                col("df_sales.ss_net_paid").alias("Net_paid"),
                                col("df_sales.ss_net_profit").alias("Net_profit")
                                )
        df_final = df_join.withColumn("sold_year", year(col("sold_date")))
        #logger.info(f"Writing {df_final.count()} records to {output_path} in csv format")
        logger.info("partitioning the data using year of sold_date after that, writing the data to output_path")
        df_final.write.partitionBy("sold_year").mode("overwrite").option("header", "true").csv(output_path)
        #df_final.write.partitionBy("sold_year").mode("overwrite").parquet(output_path)
        logger.info("Successfully extracted the Sales Data")

    except Exception:
        logger.exception()

    logger.info(f"Time taken to run {os.path.basename(__file__)} : {round(time.time() - start_time, 2)} seconds")
    spark.stop()


if __name__=="__main__":
    #3logger=get_logger("store_sales", "log.log")
    #logger.info(f"Executing the sales data by {__name__}")
    store_sales()
