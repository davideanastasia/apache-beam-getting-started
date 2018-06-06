package com.davideanastasia.beam.gs.fn;

import com.davideanastasia.beam.gs.entity.Metadata;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;


public class ExpanderFn extends DoFn<KV<Metadata, String>, KV<Metadata, String>> {

    @ProcessElement
    public void processElement(ProcessContext c) {
        KV<Metadata, String> element = c.element();
        String line = element.getValue().trim();
        if (!line.isEmpty()) {
            String[] tokens = line.split("\\P{L}+", -1);

            for (String token: tokens) {
                if (token.length() > 1) {
                    c.output(KV.of(element.getKey(), token.toLowerCase()));
                }
            }
        }
    }

}