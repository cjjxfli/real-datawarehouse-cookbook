package flink;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.TimeUnit;

public class MiniBatchOptimization {
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

        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        Configuration configuration = tEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.mini-batch.enabled", "true");
        configuration.setString("table.exec.mini-batch.allow-latency", "3 s");
        configuration.setString("table.exec.mini-batch.size", "5000");
        configuration.setString("table.optimizer.agg-phase-strategy", "ONE_PHASE");

        // SQL query
        String sourceSql =
                "create table t_daily_company_info(\n" +
                        "stock_code STRING,--股票代码\n" +
                        "turnover DOUBLE,--流通量\n" +
                        "market_count DOUBLE,--总市值\n" +
                        "current_market_value DOUBLE,--流通市值\n" +
                        "trade_time  TIMESTAMP(3),--交易时间\n" +
                        "trade_date   TIMESTAMP(3),--交易日期\n" +
                        "WATERMARK FOR trade_time AS trade_time - INTERVAL '5' SECONDS\n" +
                        ")WITH (\n" +
                        "'connector' = 'datagen',\n" +
                        "'fields.stock_code.length' = '6',\n" +
                        "'fields.turnover.min' = '1',\n" +
                        "'fields.turnover.max' = '150',\n" +
                        "'fields.market_count.min' = '1',\n" +
                        "'fields.market_count.max' = '150000',\n" +
                        "'fields.current_market_value.min' = '1',\n" +
                        "'fields.current_market_value.max' = '100000',\n" +
                        "'rows-per-second' = '180'\n" +
                        ")";

        String selectWhereSql =
                "select turnover / avg_cnt from t_daily_company_info t1\n" +
                        "left join\n" +
                        "(\n" +
                        "-- 前 5 日平均每分钟交易量\n" +
                        "select stock_code,sum(turnover) / 1200 as avg_cnt from t_daily_company_info\n" +
                        "where SECOND(trade_date) >= (SECOND(LOCALTIMESTAMP) -5*4*60*60)\n" +
                        "group by stock_code\n" +
                        ")t2\n" +
                        "on t1.stock_code=t2.stock_code";

        tEnv.executeSql(sourceSql);
        tEnv.executeSql(selectWhereSql);
    }
}
