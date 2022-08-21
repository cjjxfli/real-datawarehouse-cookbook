--批表操作
select stock_code,current_price,trade_time
from tb_real_market_data
order by trade_time,current_price;

--流表操作
select stock_code,current_price,
TUMBLE_START(trade_time, INTERVAL '1' minute) as window_start,
TUMBLE_END(trade_time, INTERVAL '1' minute) as window_end
from tb_real_market_data
GROUP BY TUMBLE(trade_time, INTERVAL '1' minute),stock_code,current_price
order by window_start,current_price desc limit 1000;
