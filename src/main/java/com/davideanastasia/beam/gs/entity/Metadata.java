package com.davideanastasia.beam.gs.entity;

import java.io.Serializable;

public class Metadata implements Serializable {

    private final String sourceName;
    private final long lineId;

    Metadata(String sourceName, long lineId) {
        this.sourceName = sourceName;
        this.lineId = lineId;
    }

    public String getSourceName() {
        return sourceName;
    }

    public long getLineId() {
        return lineId;
    }

    @Override
    public String toString() {
        return "<" + sourceName + ":" + lineId + ">";
    }

    public static Metadata of(String filename, long lineId) {
        return new Metadata(filename, lineId);
    }
}