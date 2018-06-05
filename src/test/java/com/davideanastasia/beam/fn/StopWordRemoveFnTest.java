package com.davideanastasia.beam.fn;

import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class StopWordRemoveFnTest {
    
    static private class Empty {}
    static private final Empty EMPTY = new Empty();

    @Test
    public void testDoFn() throws Exception {
        StopWordRemoveFn<Empty> doFn = new StopWordRemoveFn<>();
        DoFnTester<KV<Empty, String>, KV<Empty, String>> fnTester = DoFnTester.of(doFn);

        List<KV<Empty, String>> output1 = fnTester.processBundle(KV.of(EMPTY, "dream"));

        assertEquals(1, output1.size());
        assertEquals(KV.of(EMPTY, "dream"), output1.get(0));

        List<KV<Empty, String>> output2 = fnTester.processBundle(
            KV.of(EMPTY, "be"), KV.of(EMPTY, "is"), KV.of(EMPTY, "night"));

        assertEquals(1, output2.size());
        assertEquals(KV.of(EMPTY, "night"), output2.get(0));
    }
}