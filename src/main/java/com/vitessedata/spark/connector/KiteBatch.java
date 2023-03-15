package com.vitessedata.spark.connector;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class KiteBatch implements Batch {
    private final StructType schema;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;
    private final String filename;
    public KiteBatch(StructType schema,
                    Map<String, String> properties,
                    CaseInsensitiveStringMap options) {

        this.schema = schema;
        this.properties = properties;
        this.options = options;
        this.filename = options.get("fileName");
    }

    @Override
    public InputPartition[] planInputPartitions() {
	int npart = 3;
	InputPartition[] partitions = new KiteInputPartition[npart];
	for (int i = 0 ; i < npart ; i++) {
		Integer[] fragid = new Integer[]{i, npart};
		partitions[i] = new KiteInputPartition(fragid, "localhost");
	}
        return partitions;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new KitePartitionReaderFactory(schema, filename);
    }
}
