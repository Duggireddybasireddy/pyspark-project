#!/bin/bash

if [ $# -ne 0 ]; then
    echo "Please don't pass arguments.............."
    echo "Better luck Next Time"
    exit 0
fi

sources=("sales" "returns" "sales_web" "returns_web" "returns_catalog" "sales_catalog")
extract="/files/extract"
local="/opt/spark-apps/main/extracts"
local_path="/mnt/c/pyspark_stack/spark-apps/main/extracts"
docker exec hdfs-namenode hdfs dfs -chmod -R 777 /files/extracts

for i in "${sources[@]}"
do 
    echo "creating extract path $extract/$i"
    docker exec hdfs-namenode hdfs dfs -mkdir -p $extract/$i
    echo "Python script running. PLease wait for ..........................result"
    if docker exec -e PYTHONPATH=/opt/spark-apps/main spark-master spark-submit /opt/spark-apps/main/src/Dist/$i.py > /mnt/c/pyspark_stack/spark-apps/main/logs/dist/spark_dist.log 2>&1; then
        echo "python script run successfully"
        echo "Moving the files from $extract/$i to $local_path"
        docker exec hdfs-namenode hdfs dfs -get $extract/$i $local
        docker exec hdfs-namenode hdfs dfs -rm -r $extract/$i
        echo "Successfully moved files from hdfs to local_path"
        
    else    
        echo "Please check python script for $i"
        echo "Try again after updating script"
        exit 0
    fi
done

exit 0