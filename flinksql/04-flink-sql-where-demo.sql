drop table if exists t_price;
create table t_price(
stock_code STRING,
stock_name STRING,
currency_code STRING,
exchange STRING,
yclose DOUBLE,
topen DOUBLE,
tmax DOUBLE,
tmin DOUBLE,
latest_price DOUBLE,
trading_volume INT,
transaction_amount INT,
trade_time  TIMESTAMP(3),
trade_date   TIMESTAMP(3),
WATERMARK FOR trade_time AS trade_time - INTERVAL '5' SECONDS
)WITH (
'connector' = 'faker',
'fields.stock_code.expression' = '#{regexify ''(SZ|SH|BJ|HK)-[0-9]{6}''}',
'fields.stock_name.expression' = '#{Commerce.productName}',
'fields.currency_code.expression' = '#{regexify ''(EUR|USD|GBP|RMB)''}',
'fields.exchange.expression' = '#{regexify ''(SZ|SH|BJ|HK)''}',
'fields.yclose.expression' = '#{Number.randomDouble ''2'',''1'',''150''}',
'fields.topen.expression' = '#{Number.randomDouble ''2'',''1'',''150''}',
'fields.tmax.expression' = '#{Number.randomDouble ''2'',''1'',''150''}',
'fields.tmin.expression' = '#{Number.randomDouble ''2'',''1'',''150''}',
'fields.latest_price.expression' = '#{Number.randomDouble ''2'',''1'',''150''}',
'fields.trading_volume.expression' = '#{Number.digit}',
'fields.transaction_amount.expression' = '#{Number.digit}',
'fields.trade_time.expression' = '#{date.past ''30'',''SECONDS''}',
'fields.trade_date.expression' = '#{date.past ''5'',''SECONDS''}',
'rows-per-second' = '100'
);

create table t_asset(
stock_code STRING,
asset STRING
)WITH (
'connector' = 'faker',
'fields.stock_code.expression' = '#{regexify ''(SZ|SH|BJ|HK)-[0-9]{6}''}',
'fields.asset.expression' = '#{regexify ''(medical|education|fund|bank)''}');

EXPLAIN PLAN FOR SELECT sum(P.tmax-P.topen) as total
from t_price P
join t_asset T
ON P.stock_code=T.stock_code
where SECOND(P.trade_date) >= (SECOND(LOCALTIMESTAMP) -26*24*60*60)
and T.asset='medical';


