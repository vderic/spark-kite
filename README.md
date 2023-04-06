# Kite Connector

spark-kite connector is READONLY connector that allows querying data stored in Kite.

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

4. Copy `kite-client-sdk/java/target/kite-sdk-1.0.jar` and `spark-kite/target/spark-kite-1.0-SNAPSHOT.jar` to `$SPARK_HOME/jars` for all machines in cluster

# Run on a Spark standalone cluster in local mode

```
./bin/spark-submit --class com.vitessedata.test.KiteDataSourceRunner \
  --master local[2] \
  target/spark-kite-1.0-SNAPSHOT.jar \
  lineitem $HOME/p/spark-kite/src/test/resources/lineitemdec.schema \
  $HOME/p/spark-kite/src/test/resources/aggregate.sql
 ```


# Code sample in Java

```
SparkSession sparkSession = SparkSession.builder().appName("kite_app").getOrCreate();

Dataset<Row> dataset = sparkSession.read().schema(schema).format("kite").option("host", "localhost:7878")
    .option("path", "test_tpch/csv/lineitem*").option("filespec", "csv").option("fragcnt", 4).load();

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

# Spark Options

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

# Executors Scheduling
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

