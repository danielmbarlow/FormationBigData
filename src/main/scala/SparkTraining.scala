import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}

object SparkTraining {
  val nullable = false
  val csvSchema: StructType = StructType {
    List(
      StructField("OrderLine", IntegerType, nullable),
      StructField("OrderId", IntegerType, nullable),
      StructField("ProductId", IntegerType, nullable),
      StructField("ShipDate", DateType, nullable),
      StructField("BillDate", DateType, nullable),
      StructField("UnitPrice", DoubleType, nullable),
      StructField("NumUnits", IntegerType, nullable),
      StructField("TotalPrice", DoubleType, nullable)
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
//      .groupBy(w => w.matches(".*i.*")) <--- Celui ci m'a fait des problemes

    println("== Show RDD contents")
    textFileRdd.foreach(println(_))
    println("== Show RDD count")
    println(textFileRdd.count())

    // This is used to transform RDDs to dataframes
    import ss.implicits._
    println("== Show data frame")
    val dataFrame = textFileRdd.toDF()

    dataFrame.show()

    println("== Read from big CSV file")
    val df_orders = ss.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .schema(csvSchema)
      .load("C:/tmp/orderline.txt")

    df_orders.show(15)

    println("== Read from orc file")
    val df_orc = ss.read.orc("C:/tmp/orc/*.orc")
    df_orc.show(5)

    println("== Read from parquet file")
    val df_parquet = ss.read.parquet("C:/tmp/parquet/*.parquet")
    df_parquet.show(5)
  }
}
