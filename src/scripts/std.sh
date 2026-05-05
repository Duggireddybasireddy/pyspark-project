#!/bin/bash

if [ $# -ne 0 ]; then
	echo "Please Don't pass the Arguments..............."
    echo "Sorry I can't processed with this time pls............ try later"
fi
file_names=("call_center" "catalog_sales" "customer_demographics" "income_band" "store" "reason" \
            "web_returns" "catalog_page" "customer" "date_dim" "store_returns" "warehouse" "web_sales" \
            "catalog_returns" "customer_address" "household_demographics" "item" "ship_mode" "store_sales" "web_page" \
            "web_site")

out_path="/files/source_dist"
source_path="/files/source_std"
docker exec hdfs-namenode hdfs dfs -chmod -R 777 /files/source_dist
for i in "${file_names[@]}"
do
    echo "Creating output directory for $i"
    path="$out_path/$i"
    docker exec hdfs-namenode hdfs dfs -mkdir -p $path
    

    echo "Python Script is running for $i. Pls Wait for result................"
    if docker exec -e PYTHONPATH=/opt/spark-apps/main spark-master spark-submit /opt/spark-apps/main/src/Std/$i.py > /mnt/c/pyspark_stack/spark-apps/main/logs/std/spark_std.log 2>&1 ; then
        echo "Python script run successfully for $i"
        echo "Deleting source_file path is $source_path/$i"
        if docker exec hdfs-namenode hdfs dfs -rm -r $source_path/$i; then
            echo "Source file Deleted"
        fi
    else
        echo "Please check python script for $i............."
        exit 0
    fi
done

echo "All sources run successfully"
