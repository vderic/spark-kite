package com.vitessedata.kite.sdk;

import org.json.*;

public class Request {

    private String schema;
    private String sql;
    private int fragid;
    private int fragcnt;
    private FileSpec filespec;
    private JSONObject json;

    public Request(String schema, String sql, int fragid, int fragcnt, FileSpec filespec) {
        this.schema = schema;
        this.sql = sql;
        this.fragid = fragid;
        this.fragcnt = fragcnt;
        this.filespec = filespec;

        json = new JSONObject();

        json.put("schema", schema2JSON(schema));
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

    private JSONArray schema2JSON(String schema) {

        JSONArray array = new JSONArray();
        String[] lines = schema.split("\n");

        for (int i = 0; i < lines.length; i++) {
            String[] column = lines[i].split(":", 4);
            array.put(new JSONObject().put("name", column[0]).put("type", column[1])
                    .put("precision", Integer.parseInt(column[2])).put("scale", Integer.parseInt(column[3])));
        }

        return array;
    }

    public String toString() {
        return json.toString();
    }

    public static void main(String[] args) {

        String schema = "orderid:int64:0:0\ncost:fp64:0:0\ntotal:decimal:28:3";
        String sql = "select * from lineitem*";
        Request req = new Request(schema, sql, 0, 1, new CsvFileSpec(',', '"', '"', false, "NULL"));
        System.out.println(req.toString());
    }

}
