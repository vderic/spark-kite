package com.vitessedata.spark.connector;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.filter.Predicate;

import java.util.Map;

import com.vitessedata.kite.sdk.*;

public class KiteBatch implements Batch {
    private final StructType schema;
    private final StructType outputSchema;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;
    private final Aggregation aggregation;
    private final String path;
    private final int fragcnt;
    private final FileSpec filespec;
    private final String[] hosts;
    private final String kite_schema;
    private final String sql;
    private final Predicate[] predicates;

    public KiteBatch(StructType schema, Map<String, String> properties, CaseInsensitiveStringMap options,
            Aggregation aggregation, StructType outputSchema, Predicate[] predicates) {

        this.schema = schema;
        this.outputSchema = outputSchema;
        this.aggregation = aggregation;
        this.predicates = predicates;
        this.properties = properties;
        this.options = options;
        this.path = options.get("path");

        if (this.path == null) {
            throw new IllegalArgumentException("path not defined yet");
        }

        this.fragcnt = options.getInt("fragcnt", 4);
        String format = options.get("filespec");
        if (format == null) {
            throw new IllegalArgumentException("filespec not defined yet. csv or parquet");
        }

        if (format.equalsIgnoreCase("csv")) {
            String delim = options.get("csv_delim");
            String quote = options.get("csv_quote");
            String escape = options.get("csv_escape");
            boolean header = options.getBoolean("csv_header", false);
            String nullstr = options.get("csv_nullstr");
            CsvFileSpec csv = new CsvFileSpec();
            if (delim != null) {
                csv.delim(delim.charAt(0));
            }
            if (quote != null) {
                csv.quote(quote.charAt(0));
            }
            if (escape != null) {
                csv.escape(escape.charAt(0));
            }
            if (nullstr != null) {
                csv.nullstr(nullstr);
            }
            csv.header_line(header);

            this.filespec = csv;
        } else if (format.equalsIgnoreCase("parquet")) {
            this.filespec = new ParquetFileSpec();
        } else {
            throw new IllegalArgumentException("filespec only supports csv or parquet");
        }

        String host = options.get("host");
        if (host == null) {
            throw new IllegalArgumentException("host not found");
        }
        hosts = host.split(",");

        /* we should build a SQL and Schema here */
        kite_schema = Util.buildSchema(schema);
        if (aggregation != null) {
            sql = Util.buildAggregate(schema, path, aggregation, predicates);
        } else {
            sql = Util.buildProjection(schema, path, outputSchema, predicates);
        }
        // System.out.println("schema: " + kite_schema);
        // System.out.println("sql: " + sql);
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
        return new KitePartitionReaderFactory(outputSchema, kite_schema, sql, filespec);
    }
}
