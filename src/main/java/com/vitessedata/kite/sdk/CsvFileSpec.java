package com.vitessedata.kite.sdk;

public class CsvFileSpec extends FileSpec {

    public char delim;
    public char quote;
    public char escape;
    public boolean header_line;
    public String nullstr;

    public CsvFileSpec(char delim, char quote, char escape, boolean header_line, String nullstr) {
        super("csv");
        this.delim = delim;
        this.quote = quote;
        this.escape = escape;
        this.header_line = header_line;
        this.nullstr = nullstr;
    }
}
