-- 为了回滚执行清空操作
delete from dim_dm.dm_sa_b_biz_statinfo_d where stat_time >= '${hiveconf:run_period}';

-- 执行insert语句，将dwd的数据统计插入dm表
insert into dim_dm.dm_sa_b_biz_statinfo_d
select

