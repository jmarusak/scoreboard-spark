import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object ScoreStreamingJson {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.getActiveSession match {
      case Some(session) => session
      case None => SparkSession.builder().master("local[1]").getOrCreate()
    }
    spark.conf.set("spark.sql.shuffle.partitions", "5")
    import spark.implicits._

    val scores = spark.read.format("json").load("scores.json")
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

    val teams = blue_teams.union(red_teams)
      .sort(desc("diff"))

    teams.show()
  }
}
