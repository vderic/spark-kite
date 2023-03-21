package com.vitessedata.spark.connector;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;

import com.vitessedata.kite.sdk.*;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;

public class KitePartitionReaderFactory implements PartitionReaderFactory {
    private final StructType schema;
    private final String path;
    private final StructType requiredSchema;
    private final Aggregation aggregation;
    private final FileSpec filespec;

    public KitePartitionReaderFactory(StructType schema, String path, FileSpec filespec, Aggregation aggregation,
            StructType requiredSchema) {
        this.schema = schema;
        this.path = path;
        this.aggregation = aggregation;
        this.requiredSchema = requiredSchema;
        this.filespec = filespec;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        try {
            return new KitePartitionReader((KiteInputPartition) partition, schema, path, filespec, aggregation,
                    requiredSchema);
        } catch (FileNotFoundException | URISyntaxException e) {
            e.printStackTrace();
        }
        return null;
    }
}
