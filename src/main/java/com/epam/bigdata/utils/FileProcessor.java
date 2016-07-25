package com.epam.bigdata.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileProcessor {
    private BufferedReader reader; // read only
    private boolean processed = false;

    private List<String> sortedIds;
    private List<Integer> sortedCounts;

    public FileProcessor(BufferedReader reader) {
        this.reader = reader;
    }

    public void processFile() throws IOException {
        processed = true;
        Map<String, Integer> countMap = new HashMap<>();
        String line;
        while ((line = reader.readLine()) != null) {
            String[] values = line.split("\\t");
            String id = values[2];
            Integer count = countMap.get(id);
            if (count == null) {
                countMap.put(id, 1);
            } else {
                countMap.put(id, count + 1);
            }
        }

        TopLList<Integer, String> top = new TopLList<>(100);
        for(Map.Entry<String, Integer> e : countMap.entrySet()) {
            top.add(-e.getValue(), e.getKey());
        }

        sortedIds = new ArrayList<>(100);
        sortedCounts = new ArrayList<>(100);

        for (int i = 0; i < top.getLList().size(); i++) {
            sortedIds.add(top.getRList().get(i));
            sortedCounts.add(-top.getLList().get(i));
        }
    }

    public List<String> getSortedIds() {
        if (!processed) {
            throw new RuntimeException("Call processFile first");
        }
        return sortedIds;
    }

    public List<Integer> getSortedCounts() {
        return sortedCounts;
    }


}
