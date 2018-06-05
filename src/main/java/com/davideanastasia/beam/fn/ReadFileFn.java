package com.davideanastasia.beam.fn;

import com.davideanastasia.beam.entity.Metadata;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;


public class ReadFileFn extends DoFn<FileIO.ReadableFile, KV<Metadata, String>> {

    @ProcessElement
    public void processElement(ProcessContext c) {
        try {
            FileIO.ReadableFile f = c.element();
            String filename = f.getMetadata().resourceId().getFilename();

            ReadableByteChannel rbc = f.open();
            try (InputStream stream = Channels.newInputStream(rbc);
                 BufferedReader br = new BufferedReader(new InputStreamReader(stream))) {

                long lineId = 0;
                String line;
                while ((line = br.readLine()) != null) {
                    c.output(KV.of(Metadata.of(filename, lineId), line));

                    lineId++;
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}