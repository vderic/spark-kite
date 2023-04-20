import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, StructField, LongType, DecimalType, DoubleType, DateType}
import java.time.LocalDateTime
import java.time.Duration



val schema = StructType(Array(
    StructField("l_orderkey", LongType),
    StructField("l_partkey", LongType),
    StructField("l_suppkey", LongType),
    StructField("l_linenumber", LongType),
    StructField("l_quantity", DoubleType),
    StructField("l_extendedprice", DoubleType),
    StructField("l_discount", DoubleType),
    StructField("l_tax", DoubleType),
    StructField("l_returnflag", StringType),
    StructField("l_linestatus", StringType),
    StructField("l_shipdate", DateType),
    StructField("l_commitdate", DateType),
    StructField("l_receiptdate", DateType),
    StructField("l_shipinstruct", StringType),
    StructField("l_shipmode", StringType),
    StructField("l_comment", StringType)))

val dfr = spark.read.format("kite").schema(schema)
        .option("host", "localhost:7878")
        .option("path", "test_tpch/csv/lineitem*")
        .option("filespec", "csv")
        .option("fragcnt", 4)
        
val df = dfr.load()

df.createOrReplaceTempView("lineitem")

val start = LocalDateTime.now()

spark.sql("select l_linestatus, avg(l_discount) from lineitem group by l_linestatus").show()

val end = LocalDateTime.now()

val duration = Duration.between(start, end)

println("Duration = " + duration.toMillis() + "ms")
