import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

case class Score(
  court: String,
  blue: Int,
  red: Int,
  playtime: Long
)

object ScoreStreamingKafka {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.getActiveSession match {
      case Some(session) => session
      case None => SparkSession.builder().master("local[1]").getOrCreate()
    }
    spark.conf.set("spark.sql.shuffle.partitions", "5")

    val schema = StructType(Array(
      StructField("court", StringType, true),
      StructField("blue", IntegerType, false),
      StructField("red", IntegerType, false),
      StructField("playtime", LongType, false)))

    val scores = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingoffsets", "earliest")
      .option("subscribe", "score")
      .load()
      .select(
        from_json(col("value").cast("string"), schema)
        .alias("v")).select("v.*")

    val scores_add_datetime = scores.withColumn("datetime", from_unixtime(col("playtime") / 1000))
    val scores_add_diffs = scores_add_datetime
      .groupBy(col("court"), window(col("datetime"), "1 day"))
      .agg(
        max("blue").as("blue"),
        max("red").as("red"))
      .selectExpr("court", "blue", "red", "blue - red as blue_diff", "red - blue as red_diff")

    val blue_teams = scores_add_diffs
      .selectExpr(
        "concat(court, 'B') as team", 
        "blue_diff as diff")
    val red_teams = scores_add_diffs
      .selectExpr(
        "concat(court, 'R') as team", 
        "red_diff as diff")

    val standings = blue_teams.union(red_teams)
      .sort(desc("diff"))
   
    val query = standings.writeStream
      .outputMode("complete")
      .format("console")
      .start()
  }
}
