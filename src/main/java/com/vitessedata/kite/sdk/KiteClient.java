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

        if (iter == null) {
            Vector<XrgVector> pages = readXrgVector();
            if (pages == null) {
                return null;
            }
            iter = new XrgIterator(pages);
        }
        if (iter.next() == false) {
            return null;
        }

        return iter;
    }

    public void release() throws IOException {
        sockstream.close();
    }

    private Vector<XrgVector> readXrgVector() throws IOException {
        int ncol = 0;
        Vector<XrgVector> pages = new Vector<XrgVector>();
        KiteMessage msg = sockstream.recv();

        byte[] msgty = msg.getMessageType();
        if (Arrays.equals(msgty, KiteMessage.ERROR)) {

        } else if (Arrays.equals(msgty, KiteMessage.VECTOR)) {

            int len = msg.getMessageLength();
            if (len == 0) {
                return null;
            } else {
                XrgVector v = new XrgVector(msg.getMessageBuffer());
                pages.addElement(v);
                ncol++;
            }
        } else {

        }

        return pages;
    }

}
