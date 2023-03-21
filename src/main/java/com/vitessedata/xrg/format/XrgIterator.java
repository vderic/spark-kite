package com.vitessedata.xrg.format;

import java.lang.Byte;
import java.lang.String;
import java.lang.Integer;
import java.lang.Long;
import java.lang.Double;
import java.lang.Float;
import java.lang.Short;
import java.util.Vector;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.ByteOrder;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;

public class XrgIterator {

    Vector<XrgVector> vector;
    int curr = 0;
    int nitem = 0;
    XrgVectorHeader[] attrs;
    ByteBuffer[] data_array;
    ByteBuffer[] flag_array;

    Object[] values;
    byte[] flags;

    public XrgIterator(Vector<XrgVector> vector) {
        this.vector = vector;

        int nvec = vector.size();
        data_array = new ByteBuffer[nvec];
        flag_array = new ByteBuffer[nvec];

        attrs = new XrgVectorHeader[nvec];
        for (int i = 0; i < nvec; i++) {
            XrgVector v = vector.elementAt(i);
            attrs[i] = v.getHeader();
            nitem = attrs[i].getNItem();
            data_array[i] = v.getData().asReadOnlyBuffer();
            data_array[i].order(ByteOrder.LITTLE_ENDIAN);
            flag_array[i] = v.getFlag().asReadOnlyBuffer();
            flag_array[i].order(ByteOrder.LITTLE_ENDIAN);
        }

        flags = new byte[nvec];
        values = new Object[nvec];
    }

    public boolean next() {
        if (curr >= nitem) {
            return false;
        }

        int nvec = vector.size();
        for (int i = 0; i < nvec; i++) {
            short ptyp = attrs[i].getPhysicalType();
            short ltyp = attrs[i].getLogicalType();
            int scale = attrs[i].getScale();
            int precision = attrs[i].getPrecision();
            int itemsz = attrs[i].getItemSize();

            ByteBuffer data = data_array[i];
            ByteBuffer flag = flag_array[i];

            flags[i] = flag.get();

            switch (attrs[i].getPhysicalType()) {
            case PhysicalTypes.INT8:
                values[i] = new Byte(data.get());
                break;
            case PhysicalTypes.INT16:
                values[i] = new Short(data.getShort());
                break;
            case PhysicalTypes.INT32:
                if (ltyp == LogicalTypes.DATE) {
                    long offset = 24L * 3600L * 1000L;
                    long datetime = data.getInt() * offset;
                    values[i] = new java.sql.Date(datetime);
                } else {
                    values[i] = new Integer(data.getInt());
                }
                break;
            case PhysicalTypes.INT64:
                long int64 = data.getLong();
                if (ltyp == LogicalTypes.DECIMAL) {
                    values[i] = new BigDecimal(int64).movePointLeft(scale);
                } else {
                    values[i] = new Long(int64);
                }
                break;
            case PhysicalTypes.FP32:
                values[i] = new Float(data.getFloat());
                break;
            case PhysicalTypes.FP64:
                values[i] = new Double(data.getDouble());
                break;
            case PhysicalTypes.BYTEA: {
                int basz = data.getInt();
                byte[] ba = new byte[basz];
                data.get(ba);
                if (ltyp == LogicalTypes.STRING) {
                    values[i] = new String(ba);
                } else {
                    values[i] = ba;
                }
            }
                break;
            case PhysicalTypes.INT128: {
                byte[] ba = new byte[itemsz];
                data.get(ba);
                /*
                 * xrg return array of int64 (little endian) [low, high] but BigDecimal requires [high, low] in Big
                 * endian
                 */
                ByteBuffer bb = ByteBuffer.wrap(ba);
                bb.order(ByteOrder.LITTLE_ENDIAN);
                long low = bb.getLong();
                long high = bb.getLong();
                bb.order(ByteOrder.BIG_ENDIAN);
                bb.rewind();
                bb.putLong(high);
                bb.putLong(low);
                if (ltyp == LogicalTypes.DECIMAL) {
                    values[i] = new BigDecimal(new BigInteger(bb.array()), scale);
                } else {
                    // spark only support BigDecimal with scale 0
                    values[i] = new BigDecimal(new BigInteger(bb.array()), 0);
                }
            }
                break;
            }

        }

        curr++;
        return true;
    }

    public Object[] getValues() {
        return values;
    }

    public byte[] getFlags() {
        return flags;
    }

}
