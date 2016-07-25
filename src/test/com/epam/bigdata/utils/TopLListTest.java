package com.epam.bigdata.utils;

import org.hamcrest.CoreMatchers;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class TopLListTest {
    @org.junit.Test
    public void test() throws Exception {
        TopLList<String, String> l = new TopLList<>(5);
        l.add("3", "3");
        l.add("6", "6");
        l.add("4", "4");
        l.add("4", "44");
        l.add("4", "444");
        l.add("5", "5");
        l.add("7", "7");
        l.add("7", "7");
        l.add("8", "8");

        //First
        assertThat(l.getLList().get(0), is("3"));
        assertThat(l.getRList().get(0), is("3"));

        assertThat(l.getLList().get(1), is("4"));
        assertThat(l.getRList().get(1), is("4"));
        assertThat(l.getRList().get(2), is("44"));
        assertThat(l.getRList().get(3), is("444"));

        // Last
        assertThat(l.getLList().get(4), is("5"));
        assertThat(l.getRList().get(4), is("5"));

        System.out.println(l.getLList());
        System.out.println(l.getRList());
    }

}