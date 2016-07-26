package com.epam.bigdata.utils;

import org.junit.Test;
import static org.hamcrest.CoreMatchers.is;

import java.io.*;

import static org.junit.Assert.*;

public class FileProcessorTest {

    @Test
    public void processFile() throws Exception {
        // Sort of integration test
        try (BufferedReader in = new BufferedReader(new InputStreamReader(FileProcessorTest.class.getResourceAsStream("/sample.txt")))) {
            FileProcessor fp = new FileProcessor();
            fp.processFile(in);
            assertThat(fp.getSortedIds().get(0), is("null"));
            assertThat(fp.getSortedIds().get(1), is("VhnxPx5VPqEyBoC"));

            assertThat(fp.getSortedCounts().get(0), is(133));
            assertThat(fp.getSortedCounts().get(1), is(2));

            System.out.println(fp.getSortedIds());
            System.out.println(fp.getSortedCounts());

        }
    }

}