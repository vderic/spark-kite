package com.vitessedata.kite.sdk;

import java.net.*;
import java.io.*;
import java.util.Arrays;
import java.util.Vector;
import com.vitessedata.kite.client.*;
import com.vitessedata.xrg.format.*;

public class KiteClient {

    private SockStream sockstream;
    private XrgIterator iter;

    public KiteClient() {
        iter = null;

    }

    public void submit() throws IOException {

    }

    public XrgIterator next() throws IOException {

        for (;;) {
            if (iter == null) {
                Vector<XrgVector> pages = readXrgVector();
                if (pages == null || pages.size() == 0) {
                    return null;
                }
                iter = new XrgIterator(pages);
            }
            if (iter.next()) {
                return iter;
            }
            iter = null;
        }
    }

    public void release() throws IOException {
        sockstream.close();
    }

    private Vector<XrgVector> readXrgVector() throws IOException {
        Vector<XrgVector> pages = new Vector<XrgVector>();

        while (true) {
            KiteMessage msg = sockstream.recv();

            byte[] msgty = msg.getMessageType();
            if (Arrays.equals(msgty, KiteMessage.ERROR)) {
                throw new IOException(new String(msg.getMessageBuffer()));

            } else if (Arrays.equals(msgty, KiteMessage.VECTOR)) {

                int len = msg.getMessageLength();
                if (len == 0) {
                    break;
                } else {
                    XrgVector v = new XrgVector(msg.getMessageBuffer());
                    pages.addElement(v);
                }
            } else if (Arrays.equals(msgty, KiteMessage.BYE)) {
                release();
                break;
            } else {
                throw new IOException("Invalid Kite message type");
            }

        }

        return pages;
    }

}
