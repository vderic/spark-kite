package com.vitessedata.kite.sdk;

import org.json.*;

public class Request {

    private String schema;
    private String sql;
    private int fragid;
    private int fragcnt;
    private FileSpec filespec;
    private JSONObject json;

    public Request() {
        json = new JSONObject();
    }

    public Request(String schema, String sql, int fragid, int fragcnt, FileSpec filespec) {
        this.schema = schema;
        this.sql = sql;
        this.fragid = fragid;
        this.fragcnt = fragcnt;
        this.filespec = filespec;

        json = new JSONObject();

        json.put("schema", schema);
        json.put("sql", sql);
        json.put("fragment", new JSONArray().put(fragid).put(fragcnt));

        if (filespec instanceof CsvFileSpec) {
            CsvFileSpec csv = (CsvFileSpec) filespec;
            json.put("fmt", csv.fmt);
            json.put("csvspec",
                    new JSONObject().put("delim", String.valueOf(csv.delim)).put("quote", String.valueOf(csv.quote))
                            .put("escape", String.valueOf(csv.escape))
                            .put("header_line", String.valueOf(csv.header_line)).put("nullstr", csv.nullstr));
        }

    }

    public String toString() {
        return json.toString();
    }

    public static void main(String[] args) {

        Request req = new Request("schema", "sql", 0, 1, new CsvFileSpec(',', '"', '"', false, "NULL"));
        System.out.println(req.toString());
    }

}
