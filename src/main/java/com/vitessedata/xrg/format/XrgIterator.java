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
		for (int i = 0 ; i < nvec ; i++) {
			XrgVector v = vector.elementAt(i);
			attrs[i] = v.getHeader();
			nitem = attrs[i].getNItem();
			data_array[i] = v.getData().asReadOnlyBuffer();
			flag_array[i] = v.getFlag().asReadOnlyBuffer();
		}

		flags = new byte[nvec];
		values = new Object[nvec];
	}

	public boolean next() {
		if (curr >= nitem) {
			return false;
		}

		int nvec = vector.size();
		for (int i = 0 ; i < nvec ; i++) {
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
					values[i] = new Integer(data.getInt());
					break;
				case PhysicalTypes.INT64:
					values[i] = new Long(data.getLong());
					break;
				case PhysicalTypes.FP32:
					values[i] = new Float(data.getFloat());
					break;
				case PhysicalTypes.FP64:
					values[i] = new Double(data.getDouble());
					break;
				case PhysicalTypes.BYTEA:
					{
					int itemsz = data.getInt();
					byte[] ba = new byte[itemsz];
					data.get(ba);
					values[i] = ba;
					}
					break;
				default:
					{
					int itemsz = attrs[i].getItemSize();
					byte[] ba = new byte[itemsz];
					data.get(ba);
					values[i] = ba;
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
