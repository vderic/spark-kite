package com.vitessedata.spark.connector;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.*;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;

import java.util.Map;
import java.lang.StringBuffer;

public class KiteScanBuilder
        implements SupportsPushDownAggregates, SupportsPushDownV2Filters, SupportsPushDownRequiredColumns {
    private final StructType schema;
    private StructType requiredSchema;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;
    private Aggregation aggregation;
    private Predicate[] pushedPredicates;
    private Predicate[] nonpushedPredicates;

    public KiteScanBuilder(StructType schema, Map<String, String> properties, CaseInsensitiveStringMap options) {

        this.schema = schema;
        this.properties = properties;
        this.options = options;
        this.aggregation = null;
        this.pushedPredicates = null;
        this.nonpushedPredicates = null;
    }

    public boolean pushAggregation(Aggregation aggregation) {

        System.out.println("pushAggregation");
        AggregateFunc[] func = aggregation.aggregateExpressions();
        Expression[] expr = aggregation.groupByExpressions();

        for (int i = 0; i < func.length; i++) {
            System.out.println(func[i].describe());
            Expression[] child = func[i].children();
            if (func[i] instanceof Avg) {
                return false;
            }
        }
        System.out.println("END pushAggregation");

        this.aggregation = aggregation;
        return true;
    }

    /*
     * always return false as kite will send multiple rows with the same key and Spark need to group the data again.
     * Never able to fully complete grouping
     */
    public boolean supportCompletePushDown(Aggregation aggregation) {
        return false;
    }

    /* Pushes down predicates, and returns predicates that need to be evaluated after scanning */
    public Predicate[] pushPredicates(Predicate[] predicates) {
        System.out.println("pushPredicate:");
        for (int i = 0; i < predicates.length; i++) {
            System.out.println("Filter[" + i + "]: " + predicates[i].toString());
        }

        pushedPredicates = predicates;
        nonpushedPredicates = new Predicate[0];
        return nonpushedPredicates;
    }

    /* Returns the predicates that are pushed to the data source via pushPredicates(Predicate[]) */
    public Predicate[] pushedPredicates() {
        System.out.println("pushedPredicates: pushed nothing");
        if (pushedPredicates == null) {
            pushedPredicates = new Predicate[0];
        }
        return pushedPredicates;
    }

    public void pruneColumns(StructType requiredSchema) {
        System.out.println("pruneColumns: " + requiredSchema.toString());
        this.requiredSchema = requiredSchema;
    }

    @Override
    public Scan build() {
        return new KiteScan(schema, properties, options, aggregation, requiredSchema, pushedPredicates);
    }

}
