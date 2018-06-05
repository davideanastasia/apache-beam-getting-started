package com.davideanastasia.beam.fn;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class KeyValueInverter <K, V> extends DoFn<KV<K, V>, KV<V, K>> {

    @ProcessElement
    public void processElement(ProcessContext c) {
        KV<K, V> kv = c.element();
        c.output(KV.of(kv.getValue(), kv.getKey()));
    }

}
