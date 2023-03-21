package com.vitessedata.spark.connector;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;

import com.vitessedata.kite.sdk.*;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;

public class KitePartitionReaderFactory implements PartitionReaderFactory {
    private final StructType schema;
    private final String kite_schema;
    private final FileSpec filespec;
    private final String sql;

    public KitePartitionReaderFactory(StructType schema, String kite_schema, String sql, FileSpec filespec) {
        this.schema = schema;
        this.kite_schema = kite_schema;
        this.sql = sql;
        this.filespec = filespec;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        try {
            return new KitePartitionReader((KiteInputPartition) partition, schema, kite_schema, sql, filespec);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
