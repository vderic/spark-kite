package com.vitessedata.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.SparkSession;
import java.util.Map;
import java.util.HashMap;

import java.nio.ByteBuffer;
import org.apache.arrow.vector.util.DecimalUtility;
import java.math.BigDecimal;
import org.json.*;
import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.FileInputStream;

public class KiteDataSourceRunner {

    public static void main(String[] args) {

        if (args.length != 3) {
            System.out.println("java class tablename schemafn sqlfn");
            return;
        }

        String tablename = args[0];
        String schemafn = args[1];
        String sqlfn = args[2];

        StructType schema = null;
        String sql = null;

        try {
            schema = getSchema(schemafn);

            sql = getSQL(sqlfn);

        } catch (IOException ex) {
            System.err.println(ex);
            return;
        }

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
        // sparkSession.sql(sql).show(false);

        /* java 11 map */
        Map<String, String> aggr = Map.of("l_discount", "sum", "l_extendedprice", "avg");

        dataset.filter("l_quantity < 2").groupBy("l_linestatus").agg(aggr).explain(true);

        /* required columns. use required.csv */
        dataset.select("l_orderkey", "l_linestatus", "l_discount", "l_extendedprice").filter("l_quantity > 2")
                .explain(true);

    }

    private static StructType getSchema(String schemafn) throws IOException {
        JSONTokener tokener = new JSONTokener(new FileReader(schemafn));
        JSONArray array = new JSONArray(tokener);

        StructType schema = new StructType();

        for (int i = 0; i < array.length(); i++) {
            JSONObject obj = array.getJSONObject(i);
            String name = obj.getString("name");
            String type = obj.getString("type");
            int precision = 0;
            int scale = 0;
            try {
                precision = obj.getInt("precision");
                scale = obj.getInt("scale");
            } catch (JSONException ex) {
                ;
            }

            switch (type) {
            case "int8":
                schema = schema.add(name, DataTypes.ByteType, true);
                break;
            case "int16":
                schema = schema.add(name, DataTypes.ShortType, true);
                break;
            case "int32":
                schema = schema.add(name, DataTypes.IntegerType, true);
                break;
            case "int64":
                schema = schema.add(name, DataTypes.LongType, true);
                break;
            case "fp32":
                schema = schema.add(name, DataTypes.FloatType, true);
                break;
            case "fp64":
                schema = schema.add(name, DataTypes.DoubleType, true);
                break;
            case "date":
                schema = schema.add(name, DataTypes.DateType, true);
                break;
            case "timestamp":
                schema = schema.add(name, DataTypes.TimestampType, true);
                break;
            case "bytea":
                schema = schema.add(name, DataTypes.BinaryType, true);
            case "decimal":
                schema = schema.add(name, DataTypes.createDecimalType(precision, scale), true);
                break;
            case "string":
                schema = schema.add(name, DataTypes.StringType, true);
                break;
            default:
                break;
            }
        }

        return schema;
    }

    private static String getSQL(String sqlfn) throws IOException {
        String sql = null;

        File file = new File(sqlfn);
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] data = new byte[(int) file.length()];
            fis.read(data);
            sql = new String(data);
            System.out.println("SQL: " + sql);
        }
        return sql;
    }
}
