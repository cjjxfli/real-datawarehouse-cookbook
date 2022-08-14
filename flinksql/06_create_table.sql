-- mysql
create table if not exists tb_real_market_data
(
	id bigint null,
	stock_code varchar(64) null,
	stock_name varchar(64) null,
	topen decimal(10,4) null,
	yclose decimal(10,4) null,
	current_price decimal(10,4) null,
	day_high_price decimal(10,4) null,
	day_lowest_price decimal(10,4) null,
	bid_price decimal(10,4) null,
	bidding_price decimal(10,4) null,
	number_of_shares_traded int null,
	transaction_amount decimal(14,4) null,
	buy_one_application int null,
	buy_one_quotation decimal(10,4) null,
	buy_two_application int null,
	buy_three_application int null,
	buy_four_application int null,
	sell_one int null,
	trade_date datetime null,
	trade_time datetime null,
	create_time datetime null,
	update_time datetime null,
	buy_five_application int null,
	sell_two int null,
	sell_three int null,
	sell_four int null,
	sell_five int null,
	buy_two_quotation decimal(10,4) null,
	buy_three_quotation decimal(10,4) null,
	buy_four_quotation decimal(10,4) null,
	buy_five_quotation decimal(10,4) null,
	sell_one_quotation decimal(10,4) null,
	sell_two_quotation decimal(10,4) null,
	sell_three_quotation decimal(10,4) null,
	sell_four_quotation decimal(10,4) null,
	sell_five_quotation decimal(10,4) null
)
comment '实时行情数据表' charset=gb18030;

-- flink sql 使用 MySQL 连接器，流表
drop table if exists tb_real_market_data;
create table tb_real_market_data(
stock_code STRING,
current_price DECIMAL,
trade_time TIMESTAMP(3),
WATERMARK FOR trade_time AS trade_time - INTERVAL '1' minute
)
with(
'connector' = 'jdbc',
'url' = 'jdbc:mysql://localhost:3306/db_quant_spider',
'username' = 'root',
'password' = '181018lxf',
'table-name' = 'tb_real_market_data'
);

-- flink sql 使用 MySQL 连接器，执行模式为批处理时
drop table if exists tb_real_market_data;
create table tb_real_market_data(
stock_code STRING,
current_price DECIMAL,
trade_time TIMESTAMP(3)
)
with(
'connector' = 'jdbc',
'url' = 'jdbc:mysql://localhost:3306/db_quant_spider',
'username' = 'root',
'password' = '181018lxf',
'table-name' = 'tb_real_market_data'
);