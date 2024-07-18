package scoreboard

import org.apache.spark.sql.SparkSession

object BasicScript {
  val spark: SparkSession = SparkSession.getActiveSession match {
    case Some(session) => session
    case None => SparkSession.builder().master("local[1]").getOrCreate()
  }

  val data = Seq(("peter", 22), ("adam", 34))
  val df = spark.createDataFrame(data)
  df.show()
}