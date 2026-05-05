from src.common.spark_session import get_spark_session
from src.common.logger import get_logger
from src.config.config_read import load_config
from pyspark.sql.functions import col, year, broadcast
import time, os

def catalog_sales():
    start_time=time.time()
    config=load_config()
    log_path=config['path']['logs']['dist_log']
    logger=get_logger("catalog_sales", log_path)
    logger.info("Creating SparkSession...........")
    spark=get_spark_session("catalog_sales", log_path)
    logger.info("SparkSession created")
    customer_path=config["path"]["gold_source"]["customer"]
    datedim_path=config["path"]["gold_source"]["date_dim"]
    catalogsales_path=config["path"]["gold_source"]["catalog_sales"]
    page_path=config["path"]["gold_source"]["catalog_webpage"]
    call_center_path=config["path"]["gold_source"]["call_center"]
    shipmode_path=config["path"]["gold_source"]["ship_mode"]
    item_path=config["path"]["gold_source"]["item"]
    address_path=config["path"]["gold_source"]["customer_address"]
    output_path=config["path"]["Extract"]["catalog_sales"]
    try:
        logger.info(f"Customer dataframe creating from inbound path {customer_path}")
        df_customer=spark.read.format("parquet").load(customer_path)
        logger.info(f"call_center dataframe creating from inbound path {call_center_path}")
        df_callcenter=spark.read.format("parquet").load(call_center_path)
        logger.info(f"catalog_page dataframe creating from inbound path {page_path}")
        df_page=spark.read.format("parquet").load(page_path)
        logger.info(f"catalogsales dataframe creating from inbound path {catalogsales_path}")
        df_catalogsales=spark.read.format("parquet").load(catalogsales_path)
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
                      "callcenter": df_callcenter,
                      "catalog_page": df_page,
                      "catalog_sales": df_catalogsales,
                      "date": df_date,
                      "ship_mode": df_shipmode,
                      "item": df_item,
                      "address": df_address
                      }
    
        for name, df in dataframes.items():
            if not df.head(1):
                logger.warning(f"{name} dataframe is empty")
                

        #df_customer1=df_customer.withColumnRenamed("c_customer_sk", "customer_sk")
        #df_date1=df_date.withColumnRenamed("d_date_sk", "date_sk")
        #df_address1=df_address.withColumnRenamed("ca_address_sk", "address_sk")
        logger.info("Joining the dataframes")
        df_join=df_catalogsales.alias("df_catalogsales").join(df_item.alias("df_item"), df_catalogsales.cs_item_sk==df_item.i_item_sk, how="left") \
                               .join(df_customer.alias("bill_cust"), df_catalogsales.cs_bill_customer_sk==df_customer.c_customer_sk, how="left") \
                               .join(df_customer.alias("ship_cust"), df_catalogsales.cs_ship_customer_sk==col("ship_cust.c_customer_sk"), how="left") \
                               .join(broadcast(df_callcenter).alias("df_callcenter"), df_catalogsales.cs_call_center_sk==df_callcenter.cc_call_center_sk, how="left") \
                               .join(df_page.alias("df_page"), df_catalogsales.cs_catalog_page_sk==df_page.cp_catalog_page_sk, how="left") \
                               .join(broadcast(df_shipmode).alias("df_shipmode"), df_catalogsales.cs_ship_mode_sk==df_shipmode.sm_ship_mode_sk, how="left") \
                               .join(df_date.alias("sold_date"), df_catalogsales.cs_sold_date_sk==df_date.d_date_sk, how="left") \
                               .join(df_date.alias("ship_date"), df_catalogsales.cs_ship_date_sk==col("ship_date.d_date_sk"), how="left") \
                               .join(df_address.alias("bill_addr"), df_catalogsales.cs_bill_addr_sk==df_address.ca_address_sk, how="left") \
                               .join(df_address.alias("ship_addr"), df_catalogsales.cs_ship_addr_sk==col("ship_addr.ca_address_sk"), how="left") \
                               .select(col("df_catalogsales.cs_order_number").alias("order_number"),
                                       col("ship_date.d_date").alias("Ship_date"),
                                       col("sold_date.d_date").alias("Bill_date"),
                                       col("bill_cust.c_customer_id").alias("Bill_customer_id"),
                                       col("bill_cust.c_first_name").alias("Bill_customer_first_name"),
                                       col("bill_cust.c_last_name").alias("Bill_customer_last_name"),
                                       col("ship_cust.c_customer_id").alias("Ship_customer_id"),
                                       col("ship_cust.c_first_name").alias("Ship_customer_first_name"),
                                       col("ship_cust.c_last_name").alias("Ship_customer_last_name"),
                                       col("df_item.i_item_id").alias("item_id"),
                                       col("df_item.i_product_name").alias("product_name"),
                                       col("df_item.i_category").alias("category"),
                                       col("df_callcenter.cc_name").alias("call_center_name"),
                                       col("df_page.cp_catalog_number").alias("catalog_number"),
                                       col("df_shipmode.sm_type").alias("ship_mode"),
                                       col("df_shipmode.sm_carrier").alias("carrier"),
                                       col("bill_addr.ca_country").alias("Billing_country"),
                                       col("bill_addr.ca_zip").alias("Billing_zip"),
                                       col("ship_addr.ca_country").alias("shipping_country"),
                                       col("ship_addr.ca_zip").alias("shipping_zip"),
                                       col("df_catalogsales.cs_quantity").alias("quantity"),
                                       col("df_catalogsales.cs_sales_price").alias("sales_price"),
                                       col("df_catalogsales.cs_ext_sales_price").alias("Total_sales"),
                                       col("df_catalogsales.cs_ext_discount_amt").alias("Discount_amount"),
                                       col("df_catalogsales.cs_ext_tax").alias("Tax_amount"),
                                       col("df_catalogsales.cs_ext_ship_cost").alias("Shipping_amount"),
                                       col("df_catalogsales.cs_net_paid").alias("Net_paid"),
                                       col("df_catalogsales.cs_net_profit").alias("Net_profit")
                               )
        df_final = df_join.withColumn("Bill_year", year(col("Bill_date")))
        #logger.info(f"Writing {df_final.count()} records to {output_path} in csv format")
        logger.info("partitioning the data using year of sold_date after that, writing the data to output_path")

        df_final.write.partitionBy("Bill_year").mode("overwrite").option("header", "true").csv(output_path)
        #df_final.write.partitionBy("Bill_year").mode("overwrite").parquet(output_path)
        logger.info("Successfully extracted the Catalog sales Data")

    except Exception:
        logger.exception()

    logger.info(f"Time taken to run {os.path.basename(__file__)} : {round(time.time() - start_time, 2)} seconds")
    spark.stop()


if __name__=="__main__":
    #logger=get_logger("store_catalog", "log.log")
    #logger.info(f"Executing the sales data by {__name__}")
    catalog_sales()
