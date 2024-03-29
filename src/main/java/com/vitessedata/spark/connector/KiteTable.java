package com.vitessedata.spark.connector;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class KiteTable implements SupportsRead {

    private final StructType schema;
    private final Map<String, String> properties;
    private Set<TableCapability> capabilities;

    public KiteTable(StructType schema, Map<String, String> properties) {
        this.schema = schema;
        this.properties = properties;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new KiteScanBuilder(schema, properties, options);
    }

    @Override
    public String name() {
        return "kite_exttab";
    }

    @Override
    public StructType schema() {
        return schema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        if (capabilities == null) {
            this.capabilities = new HashSet<>();
            capabilities.add(TableCapability.BATCH_READ);
        }
        return capabilities;
    }
}
