package com.davideanastasia.beam.gs.fn;

import com.google.common.collect.Sets;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.util.Set;


public class StopWordRemoveFn<T> extends DoFn<KV<T, String>, KV<T, String>> {

    private static final Set<String> STOP_WORDS = Sets.newHashSet(
        "", "the", "this", "that", "a", "an", "has", "is", "for", "of", "so", "as", "and", "us", "be", "but", "them",
        "or", "not", "to", "what", "with", "by", "i", "you", "he", "she", "they", "its", "are", "had", "was", "which",
        "on", "it", "in", "were", "them", "know", "give", "there", "good", "am"
    );

    @ProcessElement
    public void processElement(ProcessContext c) {
        String word = c.element().getValue();
        if (word.length() > 1 && !STOP_WORDS.contains(word)) {
            c.output(c.element());
        }
    }

}
