
/* SimpleApp.java */
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.memory.RootAllocator;

public class SimpleApp {
    public static void main(String[] args) {
        String logFile = "file:///home/ubuntu/p/hadoop/spark-3.3.2-bin-hadoop3/README.md"; // Should be some file on
                                                                                           // your system
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter((org.apache.spark.api.java.function.FilterFunction<String>) s -> s.contains("a"))
                .count();
        long numBs = logData.filter((org.apache.spark.api.java.function.FilterFunction<String>) s -> s.contains("b"))
                .count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                IntVector intVector = new IntVector("fixed-size", allocator);) {
            intVector.allocateNew(3);
            intVector.set(0, 1);
            intVector.setNull(1);
            intVector.set(2, 2);
            intVector.setValueCount(3);
        }

        spark.stop();
    }
}
