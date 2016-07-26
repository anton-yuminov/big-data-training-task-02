package com.epam.bigdata.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileProcessor {
    private boolean processed = false;
    private Map<String, Integer> countMap = new HashMap<>(20_000_000);
    private int maxMapSize = 0;

    private List<String> sortedIds = null;
    private List<Integer> sortedCounts = null;

    public void processFile(BufferedReader reader) throws IOException {
        processed = true;
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
            if (countMap.size() >= maxMapSize + 50_000) {
                System.out.println("ids count: " + countMap.size());
                maxMapSize = countMap.size();
            }
        }
    }

    public String getStatusInfo() {
        return "Current map size: " + countMap.size();
    }

    public List<String> getSortedIds() {
        if (!processed) {
            throw new RuntimeException("Call processFile first");
        }
        if (!isOutListsFilled()) {
            fillOutLists();
        }
        return sortedIds;
    }

    public List<Integer> getSortedCounts() {
        if (!processed) {
            throw new RuntimeException("Call processFile first");
        }
        if (!isOutListsFilled()) {
            fillOutLists();
        }
        return sortedCounts;
    }

    private boolean isOutListsFilled() {
        return sortedIds != null;
    }

    private void fillOutLists() {
        TopLList<Integer, String> top = new TopLList<>(100);
        for (Map.Entry<String, Integer> e : countMap.entrySet()) {
            top.add(-e.getValue(), e.getKey());
        }
        sortedIds = new ArrayList<>(100);
        sortedCounts = new ArrayList<>(100);

        for (int i = 0; i < top.getLList().size(); i++) {
            sortedIds.add(top.getRList().get(i));
            sortedCounts.add(-top.getLList().get(i));
        }
    }
}
