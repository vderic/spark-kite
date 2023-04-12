package com.vitessedata.spark.connector;

import com.opencsv.CSVReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.Decimal;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.math.BigInteger;
import java.math.BigDecimal;

import com.vitessedata.kite.sdk.*;
import com.vitessedata.xrg.format.*;

public class KitePartitionReader implements PartitionReader<InternalRow> {

    private final KiteInputPartition kiteInputPartition;
    private final StructType schema;
    private final String kite_schema;
    private final String sql;
    private final FileSpec filespec;
    private KiteConnection kite;
    private InternalRow currentRow;

    public KitePartitionReader(KiteInputPartition kiteInputPartition, StructType schema, String kite_schema, String sql,
            FileSpec filespec) throws IOException {
        this.kiteInputPartition = kiteInputPartition;
        this.schema = schema;
        this.kite_schema = kite_schema;
        this.sql = sql;
        this.filespec = filespec;
        this.currentRow = null;
        createKite();
    }

    private String getHost() {
        Integer[] fragment = kiteInputPartition.getFragment();
        String[] hosts = kiteInputPartition.preferredLocations();

        int idx = fragment[0] % hosts.length;
        return hosts[idx];
    }

    private void createKite() throws IOException {

        String host = getHost();
        Integer[] fragment = kiteInputPartition.getFragment();

        kite = new KiteConnection();
        kite.host(host).schema(kite_schema).sql(sql).fragment(fragment[0], fragment[1]).format(filespec).submit();

    }

    @Override
    public boolean next() throws IOException {
        XrgIterator iter = kite.next();
        if (iter != null) {
            currentRow = getCurrentRow(iter);
            return true;
        }
        currentRow = null;
        return false;
    }

    @Override
    public InternalRow get() {
        return currentRow;
    }

    private InternalRow getCurrentRow(XrgIterator iter) {
        Object[] values = iter.getValues();
        byte[] flags = iter.getFlags();
        Object[] convertedValues = new Object[values.length];

        for (int i = 0; i < values.length; i++) {
            if (flags[i] != 0 || values[i] == null) {
                convertedValues[i] = null;
            } else if (values[i] instanceof BigInteger) {
                convertedValues[i] = Decimal.apply((BigInteger) values[i]);
            } else if (values[i] instanceof BigDecimal) {
                convertedValues[i] = Decimal.apply((BigDecimal) values[i]);
            } else if (values[i] instanceof String) {
                convertedValues[i] = UTF8String.fromString((String) values[i]);
            } else {
                convertedValues[i] = values[i];
            }
        }

        return InternalRow.apply(
                JavaConverters.asScalaIteratorConverter(Arrays.asList(convertedValues).iterator()).asScala().toSeq());
    }

    @Override
    public void close() throws IOException {
        kite.release();
    }
}
