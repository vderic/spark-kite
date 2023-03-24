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
import org.apache.spark.sql.connector.expressions.NamedReference;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Vector;
import java.util.Arrays;
import java.lang.StringBuffer;

public class Util {

    public static List<Predicate[]> checkPredicates(Predicate[] predicates) {
        List<Predicate[]> list = new ArrayList<>();
        Vector<Predicate> pushedPredicates = new Vector<>();
        Vector<Predicate> nonpushedPredicates = new Vector<>();

        for (Predicate p : predicates) {
            if (p.name().equals("IS_NULL") || p.name().equals("IS_NOT_NULL")) {
                nonpushedPredicates.add(p);
            } else {
                pushedPredicates.add(p);
            }
        }

        /* TODO: need support IS_NULL and IS_NOT_NULL. partial predicate push down make aggregate not working */
        // pushedPredicates.addAll(Arrays.asList(predicates));

        list.add(nonpushedPredicates.toArray(new Predicate[nonpushedPredicates.size()]));
        list.add(pushedPredicates.toArray(new Predicate[pushedPredicates.size()]));

        return list;
    }

    private static void buildPredicate(StringBuffer sb, Predicate[] predicates) {

        for (int i = 0; i < predicates.length; i++) {
            Predicate p = predicates[i];
            String name = p.name();
            Expression[] exprs = p.children();

            if (i > 0) {
                sb.append(" AND ");
            }
            sb.append('(');
            sb.append(p.describe());
            sb.append(')');
        }
    }

    public static String buildAggregate(String path, Aggregation aggregation, Predicate[] predicates) {

        StringBuffer sb = new StringBuffer();
        AggregateFunc[] funcs = aggregation.aggregateExpressions();
        Expression[] exprs = aggregation.groupByExpressions();

        sb.append("SELECT ");
        for (Expression expr : exprs) {
            sb.append(expr.describe());
            sb.append(", ");
        }

        for (int i = 0; i < funcs.length; i++) {
            AggregateFunc func = funcs[i];
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(func.describe());
        }

        sb.append(" FROM \"");
        sb.append(path);
        sb.append('"');

        if (predicates != null && predicates.length > 0) {
            sb.append(" WHERE ");
            buildPredicate(sb, predicates);
        }

        sb.append(" GROUP BY ");

        for (int i = 0; i < exprs.length; i++) {
            Expression expr = exprs[i];
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(expr.describe());
        }

        return sb.toString();
    }

    public static String buildProjection(String path, StructType schema, Predicate[] predicates) {

        StringBuffer sb = new StringBuffer();
        StructField[] fields = schema.fields();

        sb.append("SELECT ");
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(fields[i].name());
        }
        sb.append(" FROM \"");
        sb.append(path);
        sb.append('"');

        if (predicates != null && predicates.length > 0) {
            sb.append(" WHERE ");
            buildPredicate(sb, predicates);
        }

        return sb.toString();
    }

    public static String buildSchema(StructType schema) {

        StringBuffer sb = new StringBuffer();
        StructField[] fields = schema.fields();

        for (int i = 0; i < fields.length; i++) {
            DataType dtype = fields[i].dataType();

            sb.append(fields[i].name());
            sb.append(':');
            if (dtype.equals(DataTypes.ByteType)) {
                sb.append("int8:0:0\n");
            } else if (dtype.equals(DataTypes.ShortType)) {
                sb.append("int16:0:0\n");
            } else if (dtype.equals(DataTypes.IntegerType)) {
                sb.append("int32:0:0\n");
            } else if (dtype.equals(DataTypes.LongType)) {
                sb.append("int64:0:0\n");
            } else if (dtype.equals(DataTypes.FloatType)) {
                sb.append("fp32:0:0\n");
            } else if (dtype.equals(DataTypes.DoubleType)) {
                sb.append("fp64:0:0\n");
            } else if (dtype.equals(DataTypes.DateType)) {
                sb.append("date:0:0\n");
            } else if (dtype.equals(DataTypes.TimestampType)) {
                sb.append("timestamp:0:0\n");
            } else if (dtype.equals(DataTypes.StringType)) {
                sb.append("string:0:0\n");
            } else if (dtype.equals(DataTypes.BinaryType)) {
                sb.append("bytea:0:0\n");
            } else if (dtype instanceof DecimalType) {
                DecimalType dectype = (DecimalType) dtype;
                sb.append("decimal");
                sb.append(':');
                sb.append(dectype.precision());
                sb.append(':');
                sb.append(dectype.scale());
                sb.append('\n');
            }

        }

        return sb.toString();
    }

    public static boolean isIntegral(DataType typ) {
        if (typ == null) {
            return false;
        }
        if (typ.equals(DataTypes.LongType) || typ.equals(DataTypes.IntegerType) || typ.equals(DataTypes.ByteType)
                || typ.equals(DataTypes.ShortType)) {
            return true;
        }
        return false;
    }

    public static boolean isFloating(DataType typ) {
        if (typ == null) {
            return false;
        }
        if (typ.equals(DataTypes.FloatType) || typ.equals(DataTypes.DoubleType)) {
            return true;
        }
        return false;
    }

}
