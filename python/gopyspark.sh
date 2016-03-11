
#set default_spark_home=/opt/cloudera/parcels/CDH/lib/spark/python
#set spark_tk_jar=/home/blbarker/explore/at/spark-tk/target/spark-tk-1.0-SNAPSHOT.jar

export SPARK_TK_JAR=/home/blbarker/dev/tap-at/spark-tk/target/spark-tk-1.0-SNAPSHOT.jar
echo SPARK_TK_JAR=$SPARK_TK_JAR

#export SPARK_CLASSPATH=$SPARK_CLASSPATH:$SPARK_TK_JAR
#echo SPARK_CLASSPATH=$SPARK_CLASSPATH

export CSV_JAR=/home/blbarker/.m2/repository/org/apache/commons/commons-csv/1.0/commons-csv-1.0.jar
export JSON_JAR=/home/blbarker/.m2/repository/io/spray/spray-json_2.10/1.2.6/spray-json_2.10-1.2.6.jar

export PYSPARK_PYTHON=/usr/bin/python2.7
export PYSPARK_DRIVER_PYTHON=/usr/bin/python2.7


IPYTHON=1 ~/spark-1.5.0/bin/pyspark --master local[4] --jars $SPARK_TK_JAR,$CSV_JAR,$JSON_JAR --driver-class-path "$SPARK_TK_JAR:$CSV_JAR:$JSON_JAR"
#IPYTHON=1 /opt/cloudera/parcels/CDH/bin/pyspark --master local[4] --jars $SPARK_TK_JAR,$CSV_JAR,$JSON_JAR --driver-class-path "$SPARK_TK_JAR:$CSV_JAR:$JSON_JAR"

