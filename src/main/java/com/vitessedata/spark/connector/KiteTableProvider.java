package com.vitessedata.spark.connector;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.sources.DataSourceRegister;

import java.util.Map;

public class KiteTableProvider implements TableProvider, DataSourceRegister {
    public KiteTableProvider() {

    }

    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return null;
    }

    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return new KiteTable(schema, properties);
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }

    public String shortName() {
        return "kite";
    }
}
