import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ByteType, IntegerType, FloatType, StringType, StructType, StructField, LongType, DecimalType, DoubleType, 
				DateType, ShortType, TimestampType, ArrayType}
import java.time.LocalDateTime
import java.time.Duration



val schema = StructType(Array(
    StructField("i8", ByteType),
    StructField("i16", ShortType),
    StructField("i32", IntegerType),
    StructField("i64", LongType),
    StructField("fp32", FloatType),
    StructField("fp64", DoubleType),
    StructField("string", StringType),
    StructField("date", DateType),
    StructField("timestamp", TimestampType),
    StructField("dec64", DecimalType(10,4)),
    StructField("dec128", DecimalType(25,4)),
    StructField("i8av", ArrayType(ByteType)),
    StructField("i16av", ArrayType(ShortType)),
    StructField("i32av", ArrayType(IntegerType)),
    StructField("i64av", ArrayType(LongType)),
    StructField("strav", ArrayType(StringType)),
    StructField("fp32av", ArrayType(FloatType)),
    StructField("fp64av", ArrayType(DoubleType)),
    StructField("dateav", ArrayType(DateType)),
    StructField("timestampav", ArrayType(TimestampType)),
    StructField("dec64av", ArrayType(DecimalType(16,4))),
    StructField("dec128av", ArrayType(DecimalType(25,4)))
	))

val dfr = spark.read.format("kite").schema(schema)
        .option("host", "localhost:7878")
        .option("path", "test_spark/spark*.parquet")
        .option("filespec", "parquet")
        .option("fragcnt", 4)
        
val df = dfr.load()

df.createOrReplaceTempView("all_ext")

val start = LocalDateTime.now()

spark.sql("select * from all_ext").show()
// spark.sql("select l_linestatus, avg(l_discount) from lineitem group by l_linestatus").show()

val end = LocalDateTime.now()

val duration = Duration.between(start, end)

println("Duration = " + duration.toMillis() + "ms")
sys.exit
