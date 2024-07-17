import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.{
  StructType,
  StructField,
  StringType,
  IntegerType
}

object BasicScript {
  val spark = SparkSession.getActiveSession match {
    case Some(session) => session
    case None => SparkSession.builder().master("local[1]").getOrCreate()
  }
}
