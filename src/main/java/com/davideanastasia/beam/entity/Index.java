package com.davideanastasia.beam.entity;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

public class Index implements Serializable {
    private final Map<String, Set<Long>> acc = Maps.newTreeMap();

    public void add(Metadata metadata) {
        Set<Long> ids = acc.get(metadata.getSourceName());
        if (ids == null) {
            Set<Long> newIds = Sets.newTreeSet();
            newIds.add(metadata.getLineId());
            acc.put(metadata.getSourceName(), newIds);
        } else {
            ids.add(metadata.getLineId());
        }
    }

    public void add(Index other) {
        for (Map.Entry<String, Set<Long>> entry : other.acc.entrySet()) {
            Set<Long> lineIds = acc.get(entry.getKey());
            if (lineIds == null) {
                Set<Long> ids = Sets.newTreeSet(entry.getValue());
                acc.put(entry.getKey(), ids);
            } else {
                lineIds.addAll(entry.getValue());
            }
        }
    }

    @Override
    public String toString() {
        // <filename, []; filename: []>
        StringBuilder sb = new StringBuilder(2048);
        sb.append("<");

        String filenameDelimiter = "";
        for (Map.Entry<String, Set<Long>> entry : acc.entrySet()) {
            sb.append(filenameDelimiter);
            sb.append(entry.getKey());
            sb.append(": [");
            String lineIdDelimiter = "";
            for (Long lineId : entry.getValue()) {
                sb.append(lineIdDelimiter).append(lineId);
                lineIdDelimiter = ",";
            }
            sb.append("]");

            filenameDelimiter = "; ";
        }

        sb.append(">");
        return sb.toString();
    }
}
