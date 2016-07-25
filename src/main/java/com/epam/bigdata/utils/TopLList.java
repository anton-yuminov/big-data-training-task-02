package com.epam.bigdata.utils;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TopLList<L extends Comparable<L>, R> {

    private List<L> ls;
    private List<R> rs;
    private int length;

    public TopLList(int length) {
        ls = new ArrayList<>(length + 1);
        rs = new ArrayList<>(length + 1);
        this.length = length;
    }

    public void add(@Nonnull L l, R r) {
        int i = Collections.binarySearch(ls, l) + 1;
        i = Math.abs(i);
        while (i < ls.size() && ls.get(i).compareTo(l) <= 0) {
            i++;
        }

        if (i >= length) {
            return;
        }

        ls.add(i, l);
        rs.add(i, r);
        if (ls.size() > length) {
            ls.remove(length);
            rs.remove(length);
        }
    }

    public List<L> getLList() {
        return ls;
    }

    public List<R> getRList() {
        return rs;
    }
}
