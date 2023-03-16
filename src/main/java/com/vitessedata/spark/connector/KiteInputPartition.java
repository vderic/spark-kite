package com.vitessedata.spark.connector;

import org.apache.spark.sql.connector.read.InputPartition;

public class KiteInputPartition implements InputPartition {
    private final Integer[] fragid;
    private final String host;

    public KiteInputPartition(Integer[] fragid, String host) {
        this.fragid = fragid;
        this.host = host;
    }

    @Override
    public String[] preferredLocations() {
        return new String[] { host };
    }

    public Integer[] getFragId() {
        return fragid;
    }
}
