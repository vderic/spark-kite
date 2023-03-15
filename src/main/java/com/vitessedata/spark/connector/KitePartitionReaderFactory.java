package com.vitessedata.spark.connector;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;

public class KitePartitionReaderFactory implements PartitionReaderFactory {
    private final StructType schema;
    private final String filePath;

    public KitePartitionReaderFactory(StructType schema, String fileName) {
        this.schema = schema;
        this.filePath = fileName;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        try {
            return new KitePartitionReader((KiteInputPartition) partition, schema, filePath);
        } catch (FileNotFoundException | URISyntaxException e) {
            e.printStackTrace();
        }
        return null;
    }
}
