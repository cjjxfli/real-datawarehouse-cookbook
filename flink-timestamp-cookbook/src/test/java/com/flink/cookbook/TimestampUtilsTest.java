package com.flink.cookbook;

import org.junit.Test;

import java.sql.Timestamp;

public class TimestampUtilsTest {
    @Test
    public void test_transDateToString(){
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        String pattern = "YYYY_MM_DD_HH";
        System.out.println(TimestampUtils.transDateToString(timestamp,pattern));
    }
}
