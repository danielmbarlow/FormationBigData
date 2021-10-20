import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, dayofmonth, lit, round, when}

object SparkTraining {
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

  def main(args : Array[String]) : Unit = {
    val ss = SparkSession.builder()
      .appName("My Spark applications")
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.shuffle.compress", "true")
      .getOrCreate()
    import ss.implicits._

    println("== Read from sequence")
    val sequence = Seq("Daniel", "Amelia", "Josh", "Saffron", "Amber")
    val sparkContext = ss.sparkContext.parallelize(sequence)

    sparkContext.foreach(e => println(e))

    println("== Read from list")
    val numericRdd = ss.sparkContext.parallelize(List(34, 56, 89, 209, 30))
    numericRdd.map(_*2).foreach(println(_))

    println("== Read from small file")
    val textFileRdd = ss.sparkContext.textFile("C:/tmp/spark.txt")
      .flatMap(_.split(" "))
//      .groupBy(w => w.matches(".*i.*"))

    println("== Show RDD contents")
    textFileRdd.foreach(println(_))
    println("== Show RDD count")
    println(textFileRdd.count())

    // This is used to transform RDDs to dataframes
    println("== Show data frame")
    val dataFrame = textFileRdd.toDF()

    dataFrame.show()

    println("== Read from big CSV file")
    val orderLines = ss.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .schema(orderLineSchema)
      .load("C:/tmp/orderline.txt")

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

//    orders.show(15)
//    orderLines.show(15)
//    orderLines.printSchema()

//    println(orderLines.columns)

    println("== Show transformed data")

    val taxRate = 0.175
    val totalPrice = col("TotalPrice")
    val taxes = totalPrice * taxRate
    val promo = when(totalPrice < 20, 0).otherwise(
      when((lit(200) < totalPrice) && (totalPrice < lit(600)), 0.05)
        .otherwise(0.07)
    )
    val promoAmount = promo * totalPrice
    val totalBill = round(totalPrice - promoAmount + taxes)

    val priceDataFrame = orderLines.select(
      $"OrderId",
      $"UnitPrice".as("Unit price"),
      $"NumUnits",
      round(totalPrice, 2).as("TotalPrice"),
      $"OrderLine".cast(StringType),
      dayofmonth(col("ShipDate")).as("ShipDayOfMonth"),
      promo.as("Promo"),
      promoAmount.as("PromoAmount"),
      totalBill.as("TotalBill")
    )
    priceDataFrame.show(10)

    println("== Show filtered data")
    val filtered = priceDataFrame
      .filter(col("NumUnits") > lit(2))
      .filter(col("ShipDayOfMonth").isin(1, 15, 30))

    println("== Show joined data")
    val joined = filtered.join(orders, orders("OrderId") === filtered("OrderId"), "inner")

    joined.show(15)

    println("== Show aggregated data")
    joined.groupBy("City")
      .sum("TotalPrice").as("TotalPrice")
      .show(15)

    println("== Read from orc file")
    val df_orc = ss.read.orc("C:/tmp/orc/*.orc")
    df_orc.show(5)

    println("== Read from parquet file")
    val df_parquet = ss.read.parquet("C:/tmp/parquet/*.parquet")
    df_parquet.show(5)
  }
}
