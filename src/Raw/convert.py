from src.common.spark_session import get_spark_session
from src.common.logger import get_logger
from src.config.config_read import load_config
import time

def read(file_name):
    config=load_config()
    log_path=config['path']['logs']['raw_log']
    logger = get_logger("raw", log_path)
    spark=get_spark_session("raw", log_path)
    input_path = config['path']['raw_source']['file']
    read_path = f"{input_path}/{file_name}/*.parquet"
    logger.info(f"File path is:  {read_path}")
    try:
        df_parquet = spark.read.format("parquet").load(read_path)
        logger.info(f"{file_name} dataframe created")
        out_path = config['path']['std_source'][file_name]
        file_size = df_parquet.count()
        logger.info(f"Total No.of records in dataframe is {file_size}")
        logger.info(f"Writing the data frame into {out_path} in csv format")
        if file_size > 1000000:
            df_parquet.repartition(5).write.mode("overwrite").option("header", "true").csv(out_path)
        elif file_size > 200000:
            df_parquet.repartition(3).write.mode("overwrite").option("header", "true").csv(out_path)
        else:
            df_parquet.repartition(1).write.mode("overwrite").option("header", "true").csv(out_path)
        
        logger.info(f"Data successfully written to {out_path}")
    except Exception:
        logger.exception("Error occured while converting parquet to csv file")
    spark.stop()

def main():
    start_time = time.time()
    config=load_config()
    log_path=config['path']['logs']['raw_log']
    logger = get_logger("raw", log_path)
    
    file_names=["call_center", "catalog_sales", "customer_demographics", "income_band",  "store",  
                "web_returns", "catalog_page", "customer", "date_dim",  "reason", "store_returns", "warehouse", "web_sales", 
                "catalog_returns", "customer_address", "household_demographics", "item", "ship_mode", "store_sales", "web_page", 
                "web_site"]

    for name in file_names:
        read(name)
        
    logger.info(f"Time taken to convert parquet file into csv : {round(time.time() - start_time, 2)} seconds")
    

if __name__=="__main__":
    main()
    print("Successfullly Done")























