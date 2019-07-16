CREATE TABLE `newhouselog` PARTITIONED BY (`data_date`) STORED AS PARQUET AS
SELECT device_id,
       context_id,
       city_x,
       login_account,
       start_time,
       end_time,
       object_id,
       channel,
       context,
       roomid,
       projecttype,
       modelid,
       projectid,
       shaixuan,
       prj_listid,
       city_y,
       city_name,
       prj_itemname,
       prj_loc,
       prj_decorate,
       prj_views,
       b_lng,
       b_lat,
       price_avg,
       price_show,
       pic_id,
       pic_prjid,
       pic_prjname,
       pic_type,
       pic_desc,
       pic_ting,
       pic_wei,
       pic_chu,
       pic_area,
       pic_sell_point,
       pic_hx_totalprice,
       room_id,
       flats,
       price,
       totalprice,
       data_date
FROM `newhouselog_tmp`



CREATE TABLE `newhouselog_tmp` PARTITIONED BY (`data_date`) STORED AS PARQUET AS
SELECT device_id,
       context_id,
       city_x,
       login_account,
       start_time,
       end_time,
       object_id,
       channel,
       context,
       roomid,
       projecttype,
       modelid,
       projectid,
       shaixuan,
       prj_listid,
       city_y,
       city_name,
       prj_itemname,
       prj_loc,
       prj_decorate,
       prj_views,
       b_lng,
       b_lat,
       price_avg,
       price_show,
       pic_id,
       pic_prjid,
       pic_prjname,
       (CASE pic_type
            WHEN 8 THEN 1
            WHEN 9 THEN 2
            WHEN 10 THEN 3
            WHEN 11 THEN 4
            WHEN 21 THEN 5
            WHEN 22 THEN 6
        END) AS pic_type,
       pic_desc,
       cast(pic_ting AS INT) AS pic_ting,
       pic_wei,
       pic_chu,
       pic_area,
       pic_sell_point,
       cast(ltrim(pic_hx_totalprice, "约") as DOUBLE) as pic_hx_totalprice,
       room_id,
       flats,
       price,
       totalprice,
       data_date
FROM `newhouselog`
-- WHERE pic_hx_totalprice LIKE "约%"
-- LIMIT 10



CREATE TABLE `newhouselog_tmp` PARTITIONED BY (`data_date`) STORED AS PARQUET AS
SELECT device_id,
       context_id,
       city_x,
       login_account,
       start_time,
       end_time,
       object_id,
       channel,
       context,
       roomid,
       projecttype,
       modelid,
       projectid,
       shaixuan,
       prj_listid,
       city_y,
       city_name,
       prj_itemname,
       prj_loc,
       prj_decorate,
       prj_views,
       b_lng,
       b_lat,
       price_avg,
       price_show,
       pic_id,
       pic_prjid,
       pic_prjname,
       pic_type,
       pic_desc,
       pic_ting,
       pic_wei,
       pic_chu,
       pic_area,
       pic_sell_point,
       pic_hx_totalprice,
       room_id,
       flats,
       price,
       totalprice,
       data_date
FROM `newhouselog`


CREATE TABLE `newhouselog_login` PARTITIONED BY (`data_date`) STORED AS PARQUET AS
SELECT new.device_id,
       new.context_id,
       new.city_x,
      ( case
       when length(trim(new.login_account)) =0 then phone.phone
       when new.login_account= 'null' then phone.phone
       when new.login_account is null then phone.phone
       else new.login_account end ) as login_account  ,
       new.start_time,
       new.end_time,
       new.object_id,
       new.channel,
       new.context,
       new.roomid,
       new.projecttype,
       new.modelid,
       new.projectid,
       new.shaixuan,
       new.prj_listid,
       new.city_y,
       new.city_name,
       new.prj_itemname,
       new.prj_loc,
       new.prj_decorate,
       new.prj_views,
       new.b_lng,
       new.b_lat,
       new.price_avg,
       new.price_show,
       new.pic_id,
       new.pic_prjid,
       new.pic_prjname,
       new.pic_type,
       new.pic_desc,
       new.pic_ting,
       new.pic_wei,
       new.pic_chu,
       new.pic_area,
       new.pic_sell_point,
       new.pic_hx_totalprice,
       new.room_id,
       new.flats,
       new.price,
       new.totalprice,
       new.data_date
FROM newhouselog as new
left join dwb_account_device_phone as phone on new.device_id=phone.deviceid;




CREATE TABLE `newhouselog_csv_tmp` AS
SELECT new.device_id,
       new.context_id,
       new.city_x,
      ( case
       when  length(trim(new.login_account)) =0 then phone.phone
       when  new.login_account= 'null' then phone.phone
       when  new.login_account is null then phone.phone
       else new.login_account end ) as login_account  ,
       new.start_time,
       new.end_time,
       new.object_id,
       new.channel,
       new.context,
       new.roomid,
       new.projecttype,
       new.modelid,
       new.projectid,
       new.shaixuan,
       new.prj_listid,
       new.city_y,
       new.city_name,
       new.prj_itemname,
       new.prj_loc,
       new.prj_decorate,
       new.prj_views,
       new.b_lng,
       new.b_lat,
       new.price_avg,
       new.price_show,
       new.pic_id,
       new.pic_prjid,
       new.pic_prjname,
       new.pic_type,
       new.pic_desc,
       new.pic_ting,
       new.pic_wei,
       new.pic_chu,
       new.pic_area,
       new.pic_sell_point,
       new.pic_hx_totalprice,
       new.room_id,
       new.flats,
       new.price,
       new.totalprice
FROM newhouselog_csv as new
left join dwb_account_device_phone as phone on new.device_id=phone.deviceid;



CREATE TABLE`newhouselog_csv_tmp` AS SELECT new.device_id,new.context_id,new.city_x,(case when length(trim(new.login_account))=0 then phone.phone when new.login_account='null' then phone.phone when new.login_account is null then phone.phone else new.login_account end) as login_account,new.start_time,new.end_time,new.object_id,new.channel,new.context,new.roomid,new.projecttype,new.modelid,new.projectid,new.shaixuan,new.prj_listid,new.city_y,new.city_name,new.prj_itemname,new.prj_loc,new.prj_decorate,new.prj_views,new.b_lng,new.b_lat,new.price_avg,new.price_show,new.pic_id,new.pic_prjid,new.pic_prjname,new.pic_type,new.pic_desc,new.pic_ting,new.pic_wei,new.pic_chu,new.pic_area,new.pic_sell_point,new.pic_hx_totalprice,new.room_id,new.flats,new.price,new.totalprice FROM newhouselog_csv as new left join dwb_account_device_phone as phone on new.device_id=phone.deviceid;





select count(*) as count,district_x,login_account,round(avg(price),2), round( avg(averprice_x),2), round(avg(buildarea),2) from secondhouselog where data_date between '2019-07-08' and '2019-07-12' and city='南京' group by login_account,district_x order by count desc limit 300;















