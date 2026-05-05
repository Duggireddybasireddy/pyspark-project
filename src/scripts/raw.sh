#!/bin/bash

file_names=("call_center" "catalog_sales" "customer_demographics" "income_band" "store" \
            "web_returns" "catalog_page" "customer" "date_dim" "reason" "store_returns" "warehouse" "web_sales" \
            "catalog_returns" "customer_address" "household_demographics" "item" "ship_mode" "store_sales" "web_page" \
            "web_site")

docker exec hdfs-namenode hdfs dfs -chmod -R 777 "/files/"
for i in "${file_names[@]}"
do
	path="/files/source_std/$i"
	#echo "Path is ${path}"
	docker exec hdfs-namenode hdfs dfs -mkdir -p "${path}"
done

echo "Executing python script"

if docker exec -e PYTHONPATH=/opt/spark-apps/main spark-master spark-submit /opt/spark-apps/main/src/Raw/convert.py > /mnt/c/pyspark_stack/spark-apps/main/logs/raw/spark_raw.log 2>&1 ; then
	echo "Python script run sucessfully........Moving files from hdfs to local"

else
	echo "Please check python code"
	exit 0
fi

exit 0
