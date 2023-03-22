package com.vitessedata.spark.connector;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.filter.Predicate;

import java.util.Map;

public class KiteScan implements Scan {
    private final StructType schema;
    private final StructType requiredSchema;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;
    private final Aggregation aggregation;
    private final Predicate[] predicates;
    private StructType outSchema;

    public KiteScan(StructType schema, Map<String, String> properties, CaseInsensitiveStringMap options,
            Aggregation aggregation, StructType requiredSchema, Predicate[] predicates) {

        this.schema = schema;
        this.requiredSchema = requiredSchema;
        this.properties = properties;
        this.options = options;
        this.aggregation = aggregation;
        this.predicates = predicates;
        this.outSchema = null;
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

            outSchema = new StructType(structFields);
        } else if (requiredSchema != null) {
            outSchema = requiredSchema;
        } else {
            outSchema = schema;
        }

        System.out.println(outSchema.toString());
        return outSchema;
    }

    @Override
    public String description() {
        return "kite_scan";
    }

    @Override
    public Batch toBatch() {
        return new KiteBatch(schema, properties, options, aggregation, outSchema, predicates);
    }
}
