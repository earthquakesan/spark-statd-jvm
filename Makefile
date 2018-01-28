start-profiler: download build
	docker-compose -f docker-compose.profiler.yml up

start-profiler-bash: download build
	docker run -it --rm --network spark-profiling -e SPARK_MASTER=spark://spark-master:7077 -e INFLUX_HOST=influxdb -e INFLUX_HTTP_PORT=8086 -e INFLUX_DATABASE_NAME=spark -e INFLUX_USERNAME=admin -e INFLUX_PASSWORD=admin -e FLAMEGRAPH_TITLE=spark -v $(shell pwd)/graphs:/data --env-file $(shell pwd)/hadoop-hive.env spark-profiler:latest bash

start-cluster: download
	docker-compose up -d

start-profiler-yarn:
	docker-compose -f docker-compose.profiler.yarn.yml up

start-cluster-yarn:
	docker-compose -f docker-compose.yarn.yml up -d

network:
	docker network create spark-profiling

build:
	docker build -t spark-profiler:latest ./profiler

download: download-compose download-example-app

download-compose:
	if [ ! -f docker-compose.yml ]; then \
	wget -O docker-compose.yml https://raw.githubusercontent.com/big-data-europe/docker-spark/1.6.3-nohadoop-java8/docker-compose.yml ; \
	fi

download-example-app:
	if [ ! -f profiler/SparkWrite.jar ]; then \
	wget -O profiler/SparkWrite.jar https://www.dropbox.com/s/anct7cbd052200a/SparkWrite-1.6.3.jar ; \
	fi
