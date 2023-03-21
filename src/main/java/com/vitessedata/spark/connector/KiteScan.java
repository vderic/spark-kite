package com.vitessedata.spark.connector;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;

import java.util.Map;

public class KiteScan implements Scan {
    private StructType schema;
    private StructType requiredSchema;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;
    private Aggregation aggregation = null;

    public KiteScan(StructType schema, Map<String, String> properties, CaseInsensitiveStringMap options,
            Aggregation aggregation, StructType requiredSchema) {

        this.schema = schema;
        this.requiredSchema = requiredSchema;
        this.properties = properties;
        this.options = options;
        this.aggregation = aggregation;
    }

    @Override
    public StructType readSchema() {
        // return schema;

        if (aggregation != null) {
            /* aggregate */
            StructField[] structFields = new StructField[] {
                    new StructField("Item_Type", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("SUM(Total_Cost)", DataTypes.DoubleType, true, Metadata.empty()),
                    new StructField("COUNT(Total_Cost)", DataTypes.IntegerType, true, Metadata.empty()),
                    new StructField("SUM(Unit_Price)", DataTypes.DoubleType, true, Metadata.empty()) };

            schema = new StructType(structFields);
        }

        System.out.println(schema.toString());
        return schema;
    }

    @Override
    public String description() {
        return "kite_scan";
    }

    @Override
    public Batch toBatch() {
        return new KiteBatch(schema, properties, options, aggregation, requiredSchema);
    }
}
