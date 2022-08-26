package com.flink.cookbook;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

public class DateTransFunction extends ScalarFunction {
    private static final long serialVersionUID = 6987170485362792532L;
    private String pattern;

    public DateTransFunction(String pattern) {
        this.pattern = pattern;
    }

    public String eval(Timestamp date) {
        return TimestampUtils.transDateToString(date, pattern);
    }
}
