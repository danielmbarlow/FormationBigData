import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import java.io.File

object SparkSqlTraining {
  def main(args : Array[String]) : Unit = {
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

    val warehouseLocation = new File("/D:system/hive").getAbsolutePath

    val ss = SparkSession
      .builder()
      .appName("My Hive app")
      .master("local[*]")
//      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.shuffle.compress", "true")
      .getOrCreate()
    import ss.implicits._

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

    ss.sql("""
      SELECT City, ROUND(SUM(TotalPrice), 2) as TotalPrice
      FROM orders
      INNER JOIN order_lines on order_lines.OrderId = orders.OrderId
      GROUP BY City
    """).show(10)

  }
}
