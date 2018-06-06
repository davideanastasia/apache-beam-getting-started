package com.davideanastasia.beam.gs;

import com.davideanastasia.beam.gs.fn.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TypeDescriptors;


public class Indexer {

    public static interface Options extends PipelineOptions {

        @Description("Path of input files")
        @Default.String("data/input/*")
        String getInputFile();

        void setInputFile(String value);

        @Description("path of output files")
        @Default.String("data/indexer/output")
        String getOutputFile();

        void setOutputFile(String value);

    }

    public static void main(String[] args) throws Exception {

        Options options = PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(Options.class);
        Pipeline p = Pipeline.create(options);

        p.apply(FileIO.match().filepattern(options.getInputFile()))
            .apply(FileIO.readMatches())
            .apply("ReadFile", ParDo.of(new ReadFileFn()))
            .apply("ExpandLines", ParDo.of(new ExpanderFn()))
            .apply("FilterStopWords", ParDo.of(new StopWordRemoveFn<>()))
            .apply("KeyValueInverter", ParDo.of(new KeyValueInverter<>()))
            .apply("BuildIndex", Combine.perKey(new ReduceFn()))
            // OR: .apply(GroupByKey.create()) + aggregation
            .apply("FormatOutput", MapElements.into(TypeDescriptors.strings())
                .via(item -> item.getKey() + ": " + item.getValue()))
            .apply(TextIO.write().to(options.getOutputFile()));

        p.run().waitUntilFinish();
    }

}
