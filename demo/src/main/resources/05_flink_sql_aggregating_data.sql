-- 创建股票基本信息表
create table t_daily_company_info(
stock_code STRING,--股票代码
turnover DOUBLE,--流通量
market_count DOUBLE,--总市值
current_market_value DOUBLE,--流通市值
trade_time  TIMESTAMP(3),--交易时间
trade_date   TIMESTAMP(3),--交易日期
WATERMARK FOR trade_time AS trade_time - INTERVAL '5' SECONDS
)WITH (
'connector' = 'faker',
'fields.stock_code.expression' = '#{regexify ''(SZ|SH|BJ|HK)-[0-9]{6}''}',
'fields.turnover.expression' = '#{Number.randomDouble ''2'',''1'',''150''}',
'fields.market_count.expression' = '#{Number.randomDouble ''2'',''1'',''150000''}',
'fields.current_market_value.expression' = '#{Number.randomDouble ''2'',''1'',''15000''}',
'fields.trade_time.expression' = '#{date.past ''30'',''SECONDS''}',
'fields.trade_date.expression' = '#{date.past ''5'',''SECONDS''}',
'rows-per-second' = '180'
);

-- 量比
-- 前 N 分钟平均交易量
select turnover / avg_cnt from t_daily_company_info t1
left join
(
-- 前 5 日平均每分钟交易量
select stock_code,sum(turnover) / 1200 as avg_cnt from t_daily_company_info
where SECOND(trade_date) >= (SECOND(LOCALTIMESTAMP) -5*4*60*60)
group by stock_code
)t2
on t1.stock_code=t2.stock_code;