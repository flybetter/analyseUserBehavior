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


CREATE TABLE `secondhouselog_login` PARTITIONED BY (`data_date`) STORED AS PARQUET AS
select
  second.device_id ,
  second.context_id ,
  second.city ,
      ( case
       when length(trim(second.login_account)) =0 then phone.phone
       when second.login_account= 'null' then phone.phone
       when second.login_account is null then phone.phone
       else second.login_account end ) as login_account  ,
  second.start_time ,
  second.end_time ,
  second.content ,
  second.object_id ,
  second.context ,
  second.secondhouse_id ,
  second.esta_x ,
  second.district_x ,
  second.address_x ,
  second.streetid_x ,
  second.blockid ,
  second.blockshowname ,
  second.purpose ,
  second.structure ,
  second.buildtype ,
  second.buildyear   ,
  second.buildarea   ,
  second.subfloor    ,
  second.floor       ,
  second.totalfloor  ,
  second.room        ,
  second.hall        ,
  second.toilet      ,
  second.kitchen     ,
  second.balcony     ,
  second.forward ,
  second.price       ,
  second.averprice_x ,
  second.environment ,
  second.traffic ,
  second.fitment ,
  second.serverco ,
  second.contactor ,
  second.telno ,
  second.mobile ,
  second.creattime ,
  second.updatetime ,
  second.expiretime ,
  second.city_name ,
  second.block_id ,
  second.blockname ,
  second.district_y ,
  second.streetid_y ,
  second.area ,
  second.address_y ,
  second.bus ,
  second.averprice_y ,
  second.updateprice ,
  second.forumid ,
  second.newhouseid ,
  second.b_map_x ,
  second.b_map_y ,
  second.accuracy ,
  second.map_test ,
  second.b_property_type ,
  second.b_green ,
  second.b_parking ,
  second.b_developers ,
  second.b_property_company ,
  second.b_property_fees ,
  second.b_bus ,
  second.b_metro ,
  second.b_num ,
  second.bi_s ,
  second.bi_spell ,
  second.subway ,
  second.sitename ,
  second.subwayrange ,
  second.app ,
  second.esta_y ,
  second.property_fees ,
  second.nofee ,
  second.plot_ratio ,
  second.total_room ,
  second.turn_time ,
  second.b_area ,
  second.feature ,
  second.data_date
FROM secondhouselog as second
left join dwb_account_device_phone as phone on second.device_id=phone.deviceid;



create table tmp as SELECT login_account,district_x,count(*) as count,round(avg(price),2), round( avg(averprice_x),2), round(avg(buildarea),2)  from secondhouselog where data_date between '2019-07-08' and '2019-07-12' and length(login_account)>0 GROUP BY login_account,district_x  ORDER BY count desc;


select aa.login_account,aa.district_x,aa.rn from (
select login_account, district_x, row_number() over(partition by login_account order by count desc ) rn from tmp) aa where aa.rn<=1;





