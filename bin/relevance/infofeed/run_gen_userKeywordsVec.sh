#!/usr/bin/env bash

dayago=0
yesterday=`date --date="1 days ago" +%Y%m%d`

num=`expr $dayago + 1`
echo $num
APP_DAY=$(date --date="${num} days ago" +%Y%m%d)
for i in `seq 2 1 7`; do
    num=`expr $dayago + $i`
    APP_DAY=$APP_DAY","$(date --date="${num} days ago" +%Y%m%d)
done


SPARK_HOME="/home/work/lichangyuan/infra-client"
THRESHOLD=$"10000"
#day="20180418"
day=`date -d "yesterday" +%Y%m%d`
#INPUT_PATH="/user/h_miui_ad/matrix/warehouse/behavior_tags_keyword/std/date=2018041{1,2,3,4,5,6,7}/*"
INPUT_PATH="/user/h_miui_ad/matrix/warehouse/behavior_tags_keyword/std/date={$APP_DAY}*"
#INPUT_PATH="/user/h_data_platform/platform/matrix/matrix_behavior_browser_query/date={$APP_DAY}*"

echo $INPUT_PATH

#INPUT_PATH="/user/h_miui_ad/matrix/warehouse/behavior_tags_keyword/std/date=20180528/part-00093-48cff2bc-61c3-43de-a917-d19bdb7d848c.snappy.parquet"
#OUTPUT_PATH="/user/h_miui_ad/develop/lichangyuan/cosSimilarity/userCateInfo_$day"
OUTPUT_PATH="/user/h_miui_ad/develop/wangzhijun/project/matrix/userKeywordsVec/result_userKeywordsVec_$day"
QUEUE="root.production.miui_group.miui_ad.queue_1"

echo $INPUT_PATH

JAR_FILE="$(ls target/MarchingGiant-*-jar-with-dependencies.jar)"

$SPARK_HOME/bin/spark-submit \
    --cluster c3prc-hadoop-spark2.1 \
    --conf spark.yarn.job.owners=wangzhijun \
    --class com.xiaomi.ad.keyword.marchinggiant.relevance.infofeed.userKeywordsVec \
    --master yarn \
    --deploy-mode cluster \
    --queue "$QUEUE" \
    --driver-memory 8g \
    --executor-memory 8g \
    --executor-cores 1 \
    --conf spark.shuffle.io.preferDirectBufs=false \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.dynamicAllocation.maxExecutors=200 \
    --conf spark.dynamicAllocation.executorIdleTimeout=600s \
    "$JAR_FILE" \
    --input_threshold   $THRESHOLD \
    --input_matrix   $INPUT_PATH \
    --output   $OUTPUT_PATH \
