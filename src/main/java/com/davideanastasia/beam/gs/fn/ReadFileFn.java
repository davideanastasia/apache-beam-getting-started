package com.davideanastasia.beam.gs.fn;

import com.davideanastasia.beam.gs.entity.Metadata;
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

                // we start counting rows from 1
                long lineId = 1;
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