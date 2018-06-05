package com.davideanastasia.beam.fn;

import com.davideanastasia.beam.entity.Index;
import com.davideanastasia.beam.entity.Metadata;
import org.apache.beam.sdk.transforms.Combine;


public class ReduceFn extends Combine.CombineFn<Metadata, Index, Index> {

    @Override
    public Index createAccumulator() {
        return new Index();
    }

    @Override
    public Index addInput(Index accumulator, Metadata input) {
        accumulator.add(input);
        return accumulator;
    }

    @Override
    public Index mergeAccumulators(Iterable<Index> accumulators) {
        Index accum = new Index();
        for (Index currentAccum : accumulators) {
            accum.add(currentAccum);
        }

        return accum;
    }

    @Override
    public Index extractOutput(Index accumulator) {
        return accumulator;
    }

}