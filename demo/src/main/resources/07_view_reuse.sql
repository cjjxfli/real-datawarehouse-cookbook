create table source_table(  data string,  client string)
with ( 'connector' = 'kafka', 'topic' = 'tc_src', 'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json','scan.startup.mode'='earliest-offset');

create table sink_table1(  data string,  client string) with ( 'connector' = 'kafka', 'topic' = 'tc_sink1', 'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json','scan.startup.mode'='earliest-offset');

create table sink_table2(  data string,  client string) with ( 'connector' = 'kafka', 'topic' = 'tc_sink2', 'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json','scan.startup.mode'='earliest-offset');

begin statement set; --开启语句集
EXPLAIN
insert into sink_table1 select * from (select SUBSTRING(data, 0, 6) data,client from source_table) where client='android';

insert into sink_table2 select * from (select SUBSTRING(data, 0, 6) data,client from source_table) where client='ios';
end; --结束语句集，执行作业提交

--使用视图
create view MyView as  select * from (select SUBSTRING(data, 0, 6) data,client from source_table) ;

--%flink.ssql(runAsOne=true)
insert into sink_table1 select * from MyView where client='android';

insert into sink_table2 select * from MyView where client='ios';