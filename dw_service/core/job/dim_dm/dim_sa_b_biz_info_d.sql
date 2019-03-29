-- 为了支持重跑,删除dw_beg_date >= iv_etl_time的数据，并将dw_end_date为iv_etl_time改为'99991231'
  DELETE FROM dim_dm.dim_sa_b_biz_info
  WHERE dw_beg_date >= '2019-03-13 00:00:00';

  UPDATE dim_dm.dim_sa_b_biz_info
  SET dw_end_date = '9999-12-31 00:00:00'
  WHERE dw_end_date >= '2019-03-13 00:00:00';

-- 清空临时表
  truncate table dim_dm.dim_sa_b_biz_info_temp;

-- 插入temp表
select
  o.buid,o.sms_id
  ,case when o.edition = 0 then 0 else 1 end version_type
  ,o.area_code,ct.name city_name
  ,case when o.edition = 0 then 'ht' else case when length(o.cooperator) >=20 then '' else lower(o.cooperator) end end partner_code
  ,coalesce(i.industry_id,'Z') industry_type
  ,coalesce(i.industry_name,'【Z】其他') industry_name  -- regexp_extract('A123','^[A-Z]',0)
  ,case when regexp_extract(coalesce(i.industry_id,'Z'),'^[A-Z]',0) = ''
    then 'Z' else regexp_extract(coalesce(i.industry_id,'Z'),'^[A-Z]',0) end fix_industry_type
  , case when o.order_price in (30,58,128,238,518) then  case when o.order_price in (30,58) then concat('0', o.order_price) else concat('',o.order_price) end
         else case when p.sale_item_price is not null then case when p.sale_item_price < 100 then concat('0',p.sale_item_price) else concat('',p.sale_item_price) end
              else '030' end
         end package_type
  , case when o.order_price in (30,58,128,238,518) then o.order_price
         else case when p.sale_item_price is not null then p.sale_item_price else 30 end end order_price
  ,0 intersect_user
  ,saas.attr_value saas_product_type
  ,o.product_access_number
  ,o.bte_custname ,o.bte_telephone ,o.bte_custaddr
  ,o.cust_name ,o.bte_cust_name
  ,o.bus_tel ,o.bill_tel
  ,o.company_addr ,o.cust_addr
  ,o.link_tel,o.link_name
  ,coalesce(s.site_url,'') site_url
  ,o.lng ,o.lat ,o.source ,o.is_sentive
  ,o.cust_id ,o.crmcustid ,o.recept_channel
  ,o.complete ,o.complete_time ,o.create_time
  ,o.status ,o.case_id
  ,o.audit_time ,null delete_time
  ,o.remark
  ,s.create_time,s.audit_time,s.status
  ,h.status typt_hy_status
  ,s.order_type,s.case_id,s.second_status,s.staff_audit_time
  ,s.info_audit_status,s.info_audit_time,s.del_time
  -- ,date(iv_etl_time) dw_beg_date
  ,date('9999-12-31') dw_end_date
  from (
    select j.*,rank() over(partition by j.buid order by coalesce(unix_timestamp(j.complete_time),unix_timestamp('1970-01-01 00:00:01')) desc,j.id desc) as row_number
    from dwd.dwd_sa_t_114sps_analyse_combine_order j
    left join (
      select buid,max(delete_time) delete_time
      from dwd.dwd_sa_t_114sps_analyse_combine_order_his
      where edition != 0 and record_is_delete = 0
      group by buid
    ) d on j.buid = d.buid
    where greatest(j.create_time,j.update_time,j.recept_time,j.complete_time,j.construct_time,j.audit_time) < d.delete_time
    and j.edition != 0 and j.buid != '' and d.buid is null and j.record_is_delete = 0
  ) o  -- 同一个buid获取未拆机的最新一条记录
  left join dim_dm.dim_pub_city_gd ct on ct.area_code = o.area_code
  left join dim_dm.dim_sa_sale_item_price p -- sale_item_code字段扩展成四级，例如ZH0002-044-2或ZH0002-118，则扩展成ZH0002-044-2-1或ZH0002-118-1-1
       on case when length(o.sale_item_code)-length(regexp_replace(o.sale_item_code,'-','')) = 1 then concat(o.sale_item_code, '-1-1')
          else case when length(o.sale_item_code)-length(regexp_replace(o.sale_item_code,'-','')) = 2 then concat(o.sale_item_code, '-1') else o.sale_item_code end end = p.sale_item_code
  left join (
    select *
    from (
      select buid,industry_id,industry_name,rank() over(partition by buid order by modify_time desc,id desc) as row_number
      from dwd.dwd_up_t_qymp_114p_temp_extra
      where industry_id != '' and buid != ''
    ) t where cast(t.row_number as int) = 1
  ) i on o.buid = i.buid   -- 获取行业字段
  left join (
    select *
    from (
      select a.*,rank() over(partition by buid order by modify_time desc,id desc,info_audit_status desc,del_time desc) as row_number
      from (
        select id,buid,sms_id,edition,modify_time,site_url
               ,create_time,audit_time,status
               ,order_type,case_id,second_status,staff_audit_time
               ,info_audit_status,info_audit_time,null del_time
        from dwd.dwd_up_t_qymp_114p_temp
        union all
        select id,buid,sms_id,edition,modify_time,site_url
               ,create_time,audit_time,status
               ,order_type,case_id,second_status,staff_audit_time
               ,null info_audit_status,null info_audit_time,del_time
         from dwd.dwd_up_t_qymp_114p_del
      ) a where edition != 0
    ) t where t.row_number = 1
  ) s on o.buid = s.buid  -- 获取统一平台中相关信息
  left join (
    select *
    from (
      select a.*,rank() over(partition by buid order by modify_time desc,id desc,del_time desc) as row_number
      from (
        select id,buid,sms_id,status,modify_time,null del_time from dwd.dwd_up_t_qymp_jobs
        union all
        select id,buid,sms_id,status,modify_time,del_time from dwd.dwd_up_t_qymp_jobs_del
      ) a where buid != ''
    ) t where t.row_number = 1
  ) h on o.buid = h.buid  -- 获取行业工单状态
  left join (
    select *
    from (
      select *,rank() over(partition by a.order_id order by a.create_time desc,a.id desc) as row_number
      from dwd.dwd_sa_t_workorder_info_attrs a
      where a.attr_key = 'PM_HYSAAS' and a.record_is_delete = 0
    ) a where a.row_number = 1
  ) saas on o.sps_orderid = saas.order_id
  where o.row_number = 1;