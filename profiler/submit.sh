#!/bin/bash

[ -z $SPARK_MASTER_URL ] && { export SPARK_MASTER_URL=spark://${SPARK_MASTER_NAME}:${SPARK_MASTER_PORT}; }

echo "Submit application ${SPARK_APPLICATION_JAR_LOCATION} with main class ${SPARK_APPLICATION_MAIN_CLASS} to Spark master ${SPARK_MASTER_URL}"
echo "Additional Spark configuration params: ${SPARK_ADDITIONAL_CONF}"
echo "Passing arguments ${SPARK_APPLICATION_ARGS}"

/spark/bin/spark-submit \
    --jars ${STATS_PROFILER_LOCATION}/statsd-jvm-profiler.jar \
    --conf spark.executor.extraJavaOptions=-javaagent:"${STATS_PROFILER_LOCATION}/statsd-jvm-profiler.jar=server=${INFLUX_HOST},port=${INFLUX_HTTP_PORT},reporter=InfluxDBReporter,database=${INFLUX_DATABASE_NAME},username=${INFLUX_USERNAME},password=${INFLUX_PASSWORD},prefix=sparkapp,tagMapping=spark" \
    --conf spark.driver.extraJavaOptions=-javaagent:"${STATS_PROFILER_LOCATION}/statsd-jvm-profiler.jar=server=${INFLUX_HOST},port=${INFLUX_HTTP_PORT},reporter=InfluxDBReporter,database=${INFLUX_DATABASE_NAME},username=${INFLUX_USERNAME},password=${INFLUX_PASSWORD},prefix=sparkapp,tagMapping=spark" \
    --driver-java-options "-javaagent:${STATS_PROFILER_LOCATION}/statsd-jvm-profiler.jar=server=${INFLUX_HOST},port=${INFLUX_HTTP_PORT},reporter=InfluxDBReporter,database=${INFLUX_DATABASE_NAME},username=${INFLUX_USERNAME},password=${INFLUX_PASSWORD},prefix=sparkapp,tagMapping=spark" \
    --class ${SPARK_APPLICATION_MAIN_CLASS} \
    --master ${SPARK_MASTER_URL} \
    ${SPARK_ADDITIONAL_CONF} \
    ${SPARK_APPLICATION_JAR_LOCATION} ${SPARK_APPLICATION_ARGS}

python2.7 /influxdb_dump.py -o ${INFLUX_HOST} -r ${INFLUX_HTTP_PORT} -u ${INFLUX_USERNAME} -p ${INFLUX_PASSWORD} -d ${INFLUX_DATABASE_NAME} -t spark -e sparkapp -x /graphs/stack_traces
perl /flamegraph.pl --title "$FLAMEGRAPH_TITLE" /graphs/stack_traces/all_*.txt > /graphs/flamegraph.svg
