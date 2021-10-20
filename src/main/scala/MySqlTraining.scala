import org.apache.spark.sql.SparkSession

object MySqlTraining {
  def main(args : Array[String]) : Unit = {
    val ss = SparkSession
      .builder()
      .appName("My Hive app")
      .master("local[*]")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.shuffle.compress", "true")
      .getOrCreate()

    val orders = ss.read
      .format("jdbc")
      .option("url", "jdbc:mysql://127.0.0.1:3306/jea_db?zeroDateTimeBehaviour=CONVERT_TO_NULL&serverTimeZone=UTC")
      .option("user", "consultant")
      .option("password", "pwd#86")
      .option("dbtable",
        """
          |(
          |  SELECT state, city, SUM(ROUND(numunits * totalprice)) as totalValue
          |  FROM jea_db.orders
          |  GROUP BY state, city
          |) requete
          |""".stripMargin
      ).load()

    orders.show()
  }
}
