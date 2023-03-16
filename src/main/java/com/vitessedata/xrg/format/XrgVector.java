package com.vitessedata.xrg.format;

import java.nio.ByteBuffer;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.util.zip.GZIPInputStream;

public class XrgVector {

    private byte[] buffer;
    private XrgVectorHeader header;
    private ByteBuffer data;
    private ByteBuffer flag;

    public XrgVector(byte[] buf) throws IOException {
        init(buf);
    }

    public void init(byte[] buf) throws IOException {
        buffer = buf;
        ByteBuffer headerbuf = ByteBuffer.wrap(buf, 0, XrgVectorHeader.HEADER_SIZE);
        header = XrgVectorHeader.read(headerbuf, new XrgVectorHeader());
        int nbyte = header.getNByte();
        int zbyte = header.getZByte();
        int nitem = header.getNItem();

        if (isCompressed()) {
            // decompress the data
            ByteBuffer zbuf = ByteBuffer.wrap(buf, XrgVectorHeader.HEADER_SIZE, zbyte);

            try (GZIPInputStream gzis = new GZIPInputStream(
                    new ByteArrayInputStream(buf, XrgVectorHeader.HEADER_SIZE, zbyte))) {
                byte[] d = new byte[nbyte];
                int len = gzis.read(d, 0, nbyte);
                if (len != nbyte) {
                    throw new IOException("compressed data length not match");
                }

                data = ByteBuffer.wrap(d);
            }
        } else {
            data = ByteBuffer.wrap(buf, XrgVectorHeader.HEADER_SIZE, nbyte);
        }
        flag = ByteBuffer.wrap(buf, XrgVectorHeader.HEADER_SIZE + zbyte, nitem);

    }

    public boolean isCompressed() {
        return (header.getZByte() != header.getNByte());
    }

    public XrgVectorHeader getHeader() {
        return header;
    }

    public ByteBuffer getData() {
        return data;
    }

    public ByteBuffer getFlag() {
        return flag;
    }

}
