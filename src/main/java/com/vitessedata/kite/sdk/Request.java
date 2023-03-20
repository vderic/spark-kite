package com.vitessedata.kite.sdk;

import org.json.*;

public class Request {

    private String schema;
    private String sql;
    private int fragid;
    private int fragcnt;
    private FileSpec filespec;

    public Request() {

    }

    public Request(String schema, String sql, int fragid, int fragcnt, FileSpec filespec) {
        this.schema = schema;
        this.sql = sql;
        this.fragid = fragid;
        this.fragcnt = fragcnt;
        this.filespec = filespec;
    }

    public Request schema(String schema) {
        this.schema = schema;
        return this;
    }

    public Request fragment(int fragid, int fragcnt) {
        this.fragid = fragid;
        this.fragcnt = this.fragcnt;
        return this;
    }

    public Request format(FileSpec filespec) {
        this.filespec = filespec;
        return this;
    }

    public Request sql(String sql) {
        this.sql = sql;
        return this;
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

    public JSONObject toJSON() {
        JSONObject json = new JSONObject();

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
        } else if (filespec instanceof ParquetFileSpec) {
            ParquetFileSpec par = (ParquetFileSpec) filespec;
            json.put("fmt", par.fmt);
        }
        return json;
    }

    public static void main(String[] args) {

        String schema = "orderid:int64:0:0\ncost:fp64:0:0\ntotal:decimal:28:3";
        String sql = "select * from lineitem*";
        Request req = new Request().schema(schema).sql(sql).fragment(0, 1)
                .format(new CsvFileSpec(',', '"', '"', false, "NULL"));

        System.out.println(req.toJSON().toString());
    }

}
