import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object ScoreBatchJson {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.getActiveSession match {
      case Some(session) => session
      case None => SparkSession.builder().master("local[1]").getOrCreate()
    }
    spark.conf.set("spark.sql.shuffle.partitions", "5")

    val scores = spark.read.format("json").load("scores.json")

    val partition = Window.partitionBy("court").orderBy(desc("playtime"))
    val scores_ranked = scores.withColumn("rank", rank().over(partition))
    val scores_latest = scores_ranked.filter("rank = 1").drop("rank")
    val scores_stats = scores_latest
      .withColumn("datetime", from_unixtime(col("playtime") / 1000))
      .withColumn("blue_wins", when(col("blue") > col("red"), 1).otherwise(0))
      .withColumn("red_wins", when(col("red") > col("blue"), 1).otherwise(0))
      .withColumn("blue_loss", when(col("blue") < col("red"), 1).otherwise(0))
      .withColumn("red_loss", when(col("red") < col("blue"), 1).otherwise(0))
      .withColumn("blue_ties", when(col("blue") === col("red"), 1).otherwise(0))
      .withColumn("red_ties", when(col("blue") === col("red"), 1).otherwise(0))
      .withColumn("blue_diff", col("blue") - col("red"))
      .withColumn("red_diff", col("red") - col("blue"))

    val blue_teams = scores_stats
      .selectExpr(
        "concat(court, 'B') as team", 
        "blue_wins as wins", 
        "blue_ties as ties", 
        "blue_loss as loss", 
        "blue_diff as diff")
    val red_teams = scores_stats
      .selectExpr(
        "concat(court, 'R') as team", 
        "red_wins as wins", 
        "red_ties as ties", 
        "red_loss as loss", 
        "red_diff as diff")

    val teams = blue_teams.union(red_teams)
    val standing = teams
      .sort(desc("wins"), desc("ties"), col("loss"), desc("diff"))
      .withColumn("pos", monotonically_increasing_id() + 1)

    standing
      .select("pos", "team", "wins", "ties", "loss", "diff")
      .show()
  }
}
