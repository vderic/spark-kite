package com.vitessedata.spark.connector;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.catalyst.util.GenericArrayData;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.ArrayList;
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

    private byte[] swapInt128(byte[] i128) {
        ByteBuffer bb = ByteBuffer.wrap(i128);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        long low = bb.getLong();
        long high = bb.getLong();
        bb.order(ByteOrder.BIG_ENDIAN);
        bb.rewind();
        bb.putLong(high);
        bb.putLong(low);
        return bb.array();
    }

    private InternalRow getCurrentRow(XrgIterator iter) {
        Object[] values = iter.getValues();
        byte[] flags = iter.getFlags();
        Object[] convertedValues = new Object[values.length];
        XrgVectorHeader[] attrs = iter.getAttributes();

        for (int i = 0; i < values.length; i++) {
            short ptyp = attrs[i].getPhysicalType();
            short ltyp = attrs[i].getLogicalType();
            int scale = attrs[i].getScale();
            int precision = attrs[i].getPrecision();

            if (flags[i] != 0 || values[i] == null) {
                convertedValues[i] = null;
            } else if (values[i] instanceof byte[]) {
                if (ptyp == PhysicalTypes.INT128) {
                    byte[] v = swapInt128((byte[]) values[i]);
                    if (ltyp == LogicalTypes.DECIMAL) {
                        convertedValues[i] = Decimal.apply(new BigDecimal(new BigInteger(v), scale));
                    } else {
                        convertedValues[i] = Decimal.apply(new BigInteger(v));
                    }
                } else {
                    convertedValues[i] = values[i];
                }
            } else if (values[i] instanceof BigDecimal) {
                convertedValues[i] = Decimal.apply((BigDecimal) values[i]);
            } else if (values[i] instanceof String) {
                convertedValues[i] = UTF8String.fromString((String) values[i]);
            } else if (values[i] instanceof ArrayType) {
                ArrayType arr = (ArrayType) values[i];
                Object[] objs = arr.toArray();
                short elem_ptyp = arr.getPhysicalType();
                short elem_ltyp = arr.getLogicalType();

                for (int j = 0; j < objs.length; j++) {
                    if (objs[j] instanceof byte[]) {
                        if (elem_ptyp == PhysicalTypes.INT128) {
                            byte[] v = swapInt128((byte[]) objs[j]);
                            if (elem_ltyp == LogicalTypes.DECIMAL) {
                                objs[j] = Decimal.apply(new BigDecimal(new BigInteger(v), scale));
                            } else {
                                objs[j] = Decimal.apply(new BigInteger(v));
                            }
                        }
                    } else if (objs[j] instanceof BigDecimal) {
                        objs[j] = Decimal.apply((BigDecimal) objs[j]);
                    } else if (objs[j] instanceof String) {
                        objs[j] = UTF8String.fromString((String) objs[j]);
                    }
                }
                convertedValues[i] = new GenericArrayData(objs);
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
