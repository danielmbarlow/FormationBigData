import org.apache.spark.sql.SparkSession

object HiveTraining {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder()
      .appName("My Hive app")
      .master("local[*]")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.shuffle.compress", "true")
      .getOrCreate()
  }
}
