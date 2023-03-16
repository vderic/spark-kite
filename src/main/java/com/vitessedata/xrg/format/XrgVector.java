package com.vitessedata.xrg.format;

import java.nio.ByteBuffer;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

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
            LZ4Factory factory = LZ4Factory.fastestInstance();
            ByteBuffer zbuf = ByteBuffer.wrap(buf, XrgVectorHeader.HEADER_SIZE, zbyte);

            LZ4FastDecompressor decompressor = factory.fastDecompressor();
            ByteBuffer data = ByteBuffer.allocate(nbyte);
            decompressor.decompress(zbuf, data);
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
