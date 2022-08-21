package flink.sql;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.TimeUnit;

public class ReuseByView {
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
        env.setParallelism(2);

        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        Configuration configuration = tEnv.getConfig().getConfiguration();

        // SQL query
        String sql1 =  "create table source_table(  data string,  client string) with ( 'connector' = 'kafka', 'topic' = 'tc_src', 'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json','scan.startup.mode'='latest-offset')";

        String sql2 = "create table sink_table1(  data string,  client string) with ( 'connector' = 'kafka', 'topic' = 'tc_sink1', 'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json','scan.startup.mode'='latest-offset')";

        String sql3 = "create table sink_table2(  data string,  client string) with ( 'connector' = 'kafka', 'topic' = 'tc_sink2', 'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json','scan.startup.mode'='earliest-offset')";

        String sql4 = "create view MyView as  select * from (select SUBSTRING(data, 0, 6) data,client from source_table) ";

        String sql5 = "insert into sink_table1 select * from MyView where client='android'";

        String sql6 = "insert into sink_table2 select * from MyView where client='ios'";


        tEnv.executeSql(sql1);
        tEnv.executeSql(sql2);
        tEnv.executeSql(sql3);
        tEnv.executeSql(sql4);
        StreamStatementSet streamStatementSet = tEnv.createStatementSet();
        streamStatementSet.addInsertSql(sql5);
        streamStatementSet.addInsertSql(sql6);
        String explain = streamStatementSet.explain();
        System.out.println(explain);
        streamStatementSet.execute();
        String sql7 = "select * from sink_table1";
        tEnv.explainSql(sql7);


    }
}
