package com.flink.cookbook;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.TimeUnit;

public class DateHourUDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        env.setRestartStrategy(
                RestartStrategies.failureRateRestart(
                        6,
                        org.apache.flink.api.common.time.Time.of(10L, TimeUnit.MINUTES),
                        org.apache.flink.api.common.time.Time.of(5L, TimeUnit.SECONDS)));
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //注册 dateHour
        tEnv.registerFunction("dateHour", new DateTransFunction(TimestampUtils.YYYY_MM_DD_HH));
        // SQL query
        String sourceSql =
                "create table tb_orders_by_udf(\n" +
                        "order_no bigint,\n" +
                        "timezone STRING,\n" +
                        "send_time timestamp(3)\n" +
                        " )\n" +
                        "with (\n" +
                        "'connector' = 'jdbc',\n" +
                        "'url' = 'jdbc:mysql://localhost:3306/db_quant_spider',\n" +
                        "'username' = 'root',\n" +
                        "'password' = '',\n" +
                        "'table-name' = 'order')";

        //使用 dateHour
        String sinkSql =
                "select *,dateHour(send_time) as send_time_hour from tb_orders_by_udf";

        tEnv.executeSql(sourceSql);
        TableResult tableResult = tEnv.executeSql(sinkSql);
        tableResult.print();
    }
}
