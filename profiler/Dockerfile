FROM bde2020/spark-submit:1.6.3-nohadoop-java8-scala2.11

RUN apt-get update && apt-get install -y vim perl-modules
RUN apt-get install -y python2.7
RUN apt-get install -y build-essential autoconf libtool pkg-config python-opengl python-imaging python-pyrex python-pyside.qtopengl idle-python2.7 qt4-dev-tools qt4-designer libqtgui4 libqtcore4 libqt4-xml libqt4-test libqt4-script libqt4-network libqt4-dbus python-qt4 python-qt4-gl libgle3 python-dev
RUN apt-get install -y python-setuptools

RUN easy_install influxdb
RUN easy_install blist

RUN mkdir /graphs

COPY statsd-jvm-profiler-2.1.0.jar /statsd-jvm-profiler.jar
COPY influxdb_dump.py /influxdb_dump.py
COPY flamegraph.pl /flamegraph.pl
COPY submit.sh /submit.sh

RUN mkdir -p /app
COPY SparkWrite.jar /app/

ENV SPARK_MASTER_NAME spark-master
ENV SPARK_MASTER_PORT 7077
ENV SPARK_APPLICATION_JAR_LOCATION /app/SparkWrite.jar
ENV SPARK_APPLICATION_MAIN_CLASS org.ermilov.SparkWriteApplication
ENV ENABLE_INIT_DAEMON false

CMD ["/bin/bash", "/submit.sh"]
