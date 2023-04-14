# Spark Kite Connector

Spark kite connector is READONLY connector that allows querying data stored in Kite.  This connector is compatible with spark 3.3.2 or above.

# Compilation

1. Install Java 11, Maven, Scala and SBT

To install Scala and SBT,

```
wget https://downloads.lightbend.com/scala/2.13.0/scala-2.13.0.deb

sudo dpkg -i scala-2.13.0.deb

sudo apt-get update
sudo apt-get install apt-transport-https curl gnupg -yqq
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo -H gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import
sudo chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg
sudo apt-get update
sudo apt-get install sbt
```

2. Compile kite-client-sdk and install to Maven

```
% git clone git@github.com:vderic/kite-client-sdk.git
% cd kite-client-sdk/java
% mvn clean install
```

3. Compile spark-kite connector

```
% cd spark-kite
% mvn clean package
```

4. Copy `kite-client-sdk/java/target/kite-sdk-1.0.jar` and `spark-kite/target/spark-kite-3.3.2.jar` to `$SPARK_HOME/jars` for all machines in cluster

# Run on a Spark standalone cluster in local mode

```
./bin/spark-submit --class com.vitessedata.spark.driver.KiteDataSourceRunner \
  --master local[2] \
  $HOME/p/spark-kite/target/spark-kite-3.3.2-tests.jar \
  lineitem $HOME/p/spark-kite/src/test/resources/lineitem.schema \
  "kite://localhost:7878/test_tpch/csv/lineitem*" \
  $HOME/p/spark-kite/src/test/resources/aggregate.sql
 ```


To run with `spark-shell --master local[2]`,

```
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, StructField, LongType, DecimalType, DoubleType, DateType}

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
spark.sql("select l_linestatus, count(*) from lineitem group by l_linestatus").show()

```

# Development with spark-kite in Java/Scala

1. Install the jar file to Maven

```
% mvn clean install
```

2. Add dependency of spark-kite to your Maven project file pom.xml

```
        <dependency>
            <groupId>com.vitessedata.spark</groupId>
            <artifactId>spark-kite</artifactId>
            <version>3.3.2</version>
        </dependency>
```

## Code sample in Java

```
SparkSession sparkSession = SparkSession.builder().appName("kite_app").getOrCreate();

Dataset<Row> dataset = sparkSession.read().schema(schema)
                                          .format("kite")
                                          .option("host", "localhost:7878")
                                          .option("path", "test_tpch/csv/lineitem*")
                                          .option("filespec", "csv")
                                          .option("fragcnt", 4).load();

dataset.createOrReplaceTempView(tablename);

/*
 * NOTE:
 *
 * For simple projection, make sure repartition() is called for parallel load. If not, only one fragment (0, N)
 * will be used and only ONE fragment of the data will be received.
 *
 * For Aggregate, repartition() is not required.
*/
sparkSession.sql(sql).repartition(2).show(false);
```

## Spark Options

Specify the format with the value "kite" and the options belows.

| Option name | Description | Mandatory |
|-------------|-------------|----------|
| host        | host1:port1,host2:port2,...,hostN:portN |  True |
| path        | Path in kite. e.g. test_tpch/csv/lineitem\* | True |
| fragcnt     | Number of fragments | True |
| filespec    | Either csv or parquet | True |
| csv_delim   | CSV delimiter (default ',') | False |
| csv_escape  | CSV escape character (default '"') | False |
| csv_quote   | CSV quote character (default '"') | False |
| csv_header  | CSV header boolean (default false) | False |
| csv_nullstr | CSV NULL string (default '') | False |

## Executors Scheduling
The number of cores assigned to each executor is configurable. When spark.executor.cores is explicitly set, multiple executors from the same application may be launched on the same worker if the worker has enough cores and memory. Otherwise, each executor grabs all the cores available on the worker by default, in which case only one executor per application may be launched on each worker during one single schedule iteration.

```
spark.executor.cores=1
```

You can also configure the executor in the code.

```
val conf = new SparkConf()
  .setMaster(...)
  .setAppName(...)
  .set("spark.cores.max", "10")
val sc = new SparkContext(conf)
```

