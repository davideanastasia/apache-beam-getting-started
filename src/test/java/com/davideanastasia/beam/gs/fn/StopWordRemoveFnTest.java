package com.davideanastasia.beam.gs.fn;

import com.davideanastasia.beam.gs.utils.Empty;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class StopWordRemoveFnTest {

    @Test
    public void testDoFn() throws Exception {
        StopWordRemoveFn<Empty> doFn = new StopWordRemoveFn<>();
        DoFnTester<KV<Empty, String>, KV<Empty, String>> fnTester = DoFnTester.of(doFn);

        List<KV<Empty, String>> output1 = fnTester.processBundle(KV.of(Empty.EMPTY, "dream"));

        assertEquals(1, output1.size());
        assertEquals(KV.of(Empty.EMPTY, "dream"), output1.get(0));

        List<KV<Empty, String>> output2 = fnTester.processBundle(
            KV.of(Empty.EMPTY, "be"), KV.of(Empty.EMPTY, "is"), KV.of(Empty.EMPTY, "night"));

        assertEquals(1, output2.size());
        assertEquals(KV.of(Empty.EMPTY, "night"), output2.get(0));
    }

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testDoFn_TestPipeline() throws Exception {
        PCollection<KV<Empty, String>> input = pipeline.apply(Create.of(
            KV.of(Empty.EMPTY, "be"), KV.of(Empty.EMPTY, "is"), KV.of(Empty.EMPTY, "night"), KV.of(Empty.EMPTY, "dream")
        ).withCoder(KvCoder.of(AvroCoder.of(Empty.class), StringUtf8Coder.of())));

        PCollection<KV<Empty, String>> output = input.apply(ParDo.of(new StopWordRemoveFn<>()));

        PAssert.that(output)
            .containsInAnyOrder(
                KV.of(Empty.EMPTY, "night"),
                KV.of(Empty.EMPTY, "dream")
            );

        // run pipeline!
        pipeline.run();
    }
}