package com.davideanastasia.beam.gs.entity;

import avro.shaded.com.google.common.collect.Lists;
import avro.shaded.com.google.common.collect.Sets;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.*;


public class IndexTest {
    @Test
    public void testAddMetadata() {
        Index index = new Index();
        index.add(Metadata.of("filename1.txt", 1));

        assertEquals(1, index.numKeys());

        Set<Long> id1 = index.get("filename1.txt");
        assertNotNull(id1);
        assertEquals(id1, Sets.newTreeSet(Lists.newArrayList(1L)));

        index.add(Metadata.of("filename1.txt", 5));

        assertEquals(1, index.numKeys());

        Set<Long> id2 = index.get("filename1.txt");
        assertNotNull(id2);
        assertEquals(id2, Sets.newTreeSet(Lists.newArrayList(1L, 5L)));

        index.add(Metadata.of("filename2.txt", 3));

        assertEquals(2, index.numKeys());

        Set<Long> id3 = index.get("filename2.txt");
        assertNotNull(id3);
        assertEquals(id3, Sets.newTreeSet(Lists.newArrayList(3L)));

        assertNull(index.get("filename3.txt"));
    }

    @Test
    public void testAddIndex() {
        Index index1 = new Index();
        index1.add(Metadata.of("file1.txt", 1L));
        index1.add(Metadata.of("file1.txt", 5L));
        index1.add(Metadata.of("file1.txt", 7L));
        index1.add(Metadata.of("file3.txt", 3L));

        Index index2 = new Index();
        index2.add(Metadata.of("file2.txt", 2L));
        index2.add(Metadata.of("file2.txt", 5L));
        index2.add(Metadata.of("file2.txt", 6L));
        index2.add(Metadata.of("file3.txt", 4L));

        index1.add(index2);

        assertEquals(3, index1.numKeys());
        assertEquals(index1.get("file1.txt"), Sets.newTreeSet(Lists.newArrayList(1L, 5L, 7L)));
        assertEquals(index1.get("file2.txt"), Sets.newTreeSet(Lists.newArrayList(2L, 5L, 6L)));
        assertEquals(index1.get("file3.txt"), Sets.newTreeSet(Lists.newArrayList(3L, 4L)));
        assertNull(index1.get("file4.txt"));
    }
}