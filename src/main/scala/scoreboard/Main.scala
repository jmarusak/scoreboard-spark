import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.getActiveSession match {
      case Some(session) => session
      case None => SparkSession.builder().master("local[1]").getOrCreate()
    }

    val df = spark.range(100).toDF("number")
    val df2 = df.filter("number = 2")

    df.show(5)
  }
}
