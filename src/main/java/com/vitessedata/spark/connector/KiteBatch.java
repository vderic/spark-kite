package com.vitessedata.spark.connector;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;

import java.util.Map;

import com.vitessedata.kite.sdk.*;

public class KiteBatch implements Batch {
    private final StructType schema;
    private final StructType requiredSchema;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;
    private final Aggregation aggregation;
    private final String path;
    private final int fragcnt;
    private final FileSpec filespec;
    private final String[] hosts;

    public KiteBatch(StructType schema, Map<String, String> properties, CaseInsensitiveStringMap options,
            Aggregation aggregation, StructType requiredSchema) {

        this.schema = schema;
        this.requiredSchema = requiredSchema;
        this.aggregation = aggregation;
        this.properties = properties;
        this.options = options;
        this.path = options.get("path");
        this.fragcnt = options.getInt("fragcnt", 4);
        String format = options.get("filespec");
        if (format.equalsIgnoreCase("csv")) {
            this.filespec = new CsvFileSpec();
        } else if (format.equalsIgnoreCase("parquet")) {
            this.filespec = new ParquetFileSpec();
        } else {
            throw new RuntimeException("filespec only supports csv or parquet");
        }

        String host = options.get("host");
        if (host == null) {
            throw new RuntimeException("host not found");
        }
        hosts = host.split(",");

    }

    @Override
    public InputPartition[] planInputPartitions() {
        InputPartition[] partitions = new KiteInputPartition[fragcnt];
        for (int i = 0; i < fragcnt; i++) {
            Integer[] fragment = new Integer[] { i, fragcnt };

            partitions[i] = new KiteInputPartition(fragment, hosts);
        }
        return partitions;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new KitePartitionReaderFactory(schema, path, filespec, aggregation, requiredSchema);
    }
}
