package com.vitessedata.kite.client;

import java.lang.System;

public class KiteMessage {

    public static final byte[] KIT1 = { 'K', 'I', 'T', '1' };
    public static final byte[] JSON = { 'J', 'S', 'O', 'N' };
    public static final byte[] BYE = { 'B', 'Y', 'E', '_' };
    public static final byte[] ERROR = { 'E', 'R', 'R', '_' };
    public static final byte[] VECTOR = { 'V', 'E', 'C', '_' };

    private byte[] buffer = null;
    private int msglen = 0;
    private byte[] msgty = null;

    public KiteMessage() {
        msgty = new byte[4];
        buffer = new byte[1024];
        msglen = 0;

    }

    public byte[] getMessageType() {
        return msgty;
    }

    public int getMessageLength() {
        return msglen;
    }

    public byte[] getMessage() {
        return buffer;
    }

    /* will set the message length and realloc buffer */
    public byte[] allocateMessage(byte[] msgty, int bufsz) {
        System.arraycopy(msgty, 0, this.msgty, 0, this.msgty.length);
        msglen = bufsz;

        // realloc buffer
        if (buffer.length < bufsz) {
            buffer = new byte[bufsz];
            return buffer;
        }

        return buffer;
    }

}
