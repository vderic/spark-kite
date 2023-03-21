package com.vitessedata.spark.connector;

import org.apache.spark.sql.connector.read.InputPartition;

public class KiteInputPartition implements InputPartition {
    private final Integer[] fragment;
    private final String[] host;

    public KiteInputPartition(Integer[] fragment, String[] host) {
        this.fragment = fragment;
        this.host = host;
    }

    @Override
    public String[] preferredLocations() {
        return host;
    }

    public Integer[] getFragment() {
        return fragment;
    }
}
