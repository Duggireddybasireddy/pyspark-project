#!/bin/bash

echo "Running Bronze layer..........."
bash /mnt/c/pyspark_stack/spark-apps/main/src/scripts/raw.sh || exit 0

echo "Running Silver layer............."
bash /mnt/c/pyspark_stack/spark-apps/main/src/scripts/std.sh || exit 0

echo "Running Gold layer..............."
bash /mnt/c/pyspark_stack/spark-apps/main/src/scripts/dist.sh || exit 0
exit 0