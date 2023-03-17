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
        sockstream = null;
        iter = null;

    }

    /*
     * submit a SQL query to kite. addr: host:port schema: lines of "name:type:precision:scale" fragid: a number between
     * 0 and fragnct-1 fragcnt: max number of fragments
     */
    public void submit(String addr, Request request) throws IOException {

        if (!addr.contains(":")) {
            throw new IllegalArgumentException("addr should be host:port");
        }
        String[] addrport = addr.split(":", 2);

        Socket socket = new Socket(addrport[0], Integer.parseInt(addrport[1]));
        sockstream = new SockStream(socket);

        String json = request.toString();
        sockstream.send(KiteMessage.JSON, json.getBytes());

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
