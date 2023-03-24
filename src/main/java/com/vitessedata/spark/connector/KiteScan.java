package com.vitessedata.spark.connector;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.expressions.aggregate.*;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.NamedReference;

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

    /* TODO: compare the old and new type */
    private DataType getMaxType(DataType oldtyp, DataType newtyp) {
        if (oldtyp == null) {
            return newtyp;
        }

        if (oldtyp instanceof DecimalType) {
            return oldtyp;
        }

        if (oldtyp.equals(DataTypes.DoubleType)) {
            if (newtyp instanceof DecimalType) {
                return newtyp;
            }
            return oldtyp;
        }

        if (oldtyp.equals(DataTypes.FloatType)) {
            if (newtyp instanceof DecimalType || newtyp.equals(DataTypes.DoubleType)) {
                return newtyp;
            } else if (newtyp.equals(DataTypes.LongType)) {
                return DataTypes.DoubleType;
            }
            return oldtyp;
        }

        if (oldtyp.equals(DataTypes.LongType)) {
            if (newtyp instanceof DecimalType || newtyp.equals(DataTypes.DoubleType)) {
                return newtyp;
            } else if (newtyp.equals(DataTypes.FloatType)) {
                return DataTypes.DoubleType;
            }
            return oldtyp;
        }

        if (oldtyp.equals(DataTypes.IntegerType)) {
            if (newtyp instanceof DecimalType || newtyp.equals(DataTypes.DoubleType)
                    || newtyp.equals(DataTypes.FloatType) || newtyp.equals(DataTypes.LongType)) {
                return newtyp;
            }
            return oldtyp;
        }

        if (oldtyp.equals(DataTypes.ShortType)) {
            if (newtyp.equals(DataTypes.ByteType)) {
                return oldtyp;
            }
            return newtyp;
        }

        if (oldtyp.equals(DataTypes.ByteType)) {
            return newtyp;
        }

        throw new IllegalArgumentException("getMaxType type not supported. " + newtyp.toString());
    }

    private DataType getReturnType(StructType schema, Expression[] exprs) {
        DataType ret = null;
        StructField[] fields = schema.fields();

        for (Expression expr : exprs) {
            if (expr instanceof NamedReference) {
                String cname = expr.describe();
                int idx = schema.fieldIndex(cname);
                DataType typ = fields[idx].dataType();
                ret = getMaxType(ret, typ);

            } else {
                DataType typ = getReturnType(schema, expr.children());
                ret = getMaxType(ret, typ);
            }
        }
        ;

        return ret;
    }

    @Override
    public StructType readSchema() {

        if (aggregation != null) {
            /* aggregate */

            AggregateFunc[] funcs = aggregation.aggregateExpressions();
            Expression[] exprs = aggregation.groupByExpressions();
            StructField[] fields = schema.fields();

            outSchema = new StructType();

            for (Expression expr : exprs) {
                String cname = expr.describe();
                int idx = schema.fieldIndex(cname);
                outSchema = outSchema.add(fields[idx]);
            }

            for (AggregateFunc func : funcs) {
                // decimal -> BigDecimal, Long -> BigDecimal, int32 -> Long, Float -> Double, Double -> Double
                if (func instanceof Sum) {
                    DataType typ = getReturnType(schema, func.children());
                    if (typ.equals(DataTypes.ByteType) || typ.equals(DataTypes.ShortType)
                            || typ.equals(DataTypes.IntegerType)) {
                        outSchema = outSchema.add(func.describe(), DataTypes.LongType);
                    } else if (typ.equals(DataTypes.LongType)) {
                        outSchema = outSchema.add(func.describe(), DataTypes.createDecimalType(38, 0));
                    } else if (typ instanceof DecimalType) {
                        DecimalType dectype = (DecimalType) typ;
                        int precision = dectype.precision();
                        int scale = dectype.scale();
                        outSchema = outSchema.add(func.describe(), DataTypes.createDecimalType(precision, scale));
                    } else if (typ.equals(DataTypes.FloatType) || typ.equals(DataTypes.DoubleType)) {
                        outSchema = outSchema.add(func.describe(), DataTypes.DoubleType);
                    } else {
                        throw new RuntimeException("sum() type not supported. " + typ.toString());
                    }

                }

                // same as the input type
                if (func instanceof Min || func instanceof Max) {
                    Expression[] e = func.children();
                    DataType typ = getReturnType(schema, func.children());
                    outSchema = outSchema.add(func.describe(), typ);

                }

                // Long type
                if (func instanceof Count || func instanceof CountStar) {
                    outSchema = outSchema.add(func.describe(), DataTypes.LongType);
                }

            }

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
