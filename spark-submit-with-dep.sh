spark-submit \
  --class ScoreStreamingKafka \
  --master local \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:src/main/resources/log4j.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:src/main/resources/log4j.properties" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  target/scala-2.12/scoreboard-spark_2.12-1.0.jar
