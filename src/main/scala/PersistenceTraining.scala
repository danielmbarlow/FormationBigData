import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object PersistenceTraining {
  val NOT_NULLABLE = false
  val orderLineSchema: StructType = StructType {
    List(
      StructField("OrderLine", IntegerType, NOT_NULLABLE),
      StructField("OrderId", IntegerType, NOT_NULLABLE),
      StructField("ProductId", IntegerType, NOT_NULLABLE),
      StructField("ShipDate", DateType, NOT_NULLABLE),
      StructField("BillDate", DateType, NOT_NULLABLE),
      StructField("UnitPrice", DoubleType, NOT_NULLABLE),
      StructField("NumUnits", IntegerType, NOT_NULLABLE),
      StructField("TotalPrice", DoubleType, NOT_NULLABLE)
    )
  }

  var orderSchema: StructType = StructType {
    List(
      StructField("OrderId", IntegerType, NOT_NULLABLE),
      StructField("CustomerId", IntegerType, NOT_NULLABLE),
      StructField("CampaignId", IntegerType, NOT_NULLABLE),
      StructField("OrderDate", DateType, NOT_NULLABLE),
      StructField("City", StringType, NOT_NULLABLE),
      StructField("State", StringType, NOT_NULLABLE),
      StructField("ZipCode", StringType, NOT_NULLABLE),
      StructField("PaymentType", StringType, NOT_NULLABLE),
      StructField("TotalPrice", DoubleType, NOT_NULLABLE),
      StructField("NumOrderLines", IntegerType, NOT_NULLABLE),
      StructField("NumUnits", IntegerType, NOT_NULLABLE)
    )
  }

//  partition order line by city

  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder()
      .appName("My Hive app")
      .master("local[*]")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.shuffle.compress", "true")
      .enableHiveSupport()
      .getOrCreate()
    import ss.implicits._

    println("== Read from order line")
    val orderLines = ss.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .schema(orderLineSchema)
      .load("C:/tmp/orderline.txt")
    orderLines.createOrReplaceTempView("order_lines")

    val orders = ss.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .schema(orderSchema)
      .load("C:/tmp/orders.txt")
      .select(
        $"OrderId",
        $"City"
      )
    orders.createOrReplaceTempView("orders")

    orderLines.show(5)

    val joined = ss.sql("""
      SELECT City, ROUND(SUM(TotalPrice), 2) as TotalPrice
      FROM orders
      INNER JOIN order_lines on order_lines.OrderId = orders.OrderId
      GROUP BY City
    """)

    println("== Save CSV")
    joined
      .write
      .partitionBy("City")
      .format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv("C:/tmp/output_csv")

    println("== Save ORC")
    orderLines.repartition(2)
      .write
      .mode(SaveMode.Overwrite)
      .orc("C:/tmp/output_orc")

    println("== Save Parquet")
    orderLines.repartition(2)
      .write
      .mode(SaveMode.Overwrite)
      .parquet("C:/tmp/output_parquet")
  }
}
