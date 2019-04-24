CREATE  TABLE `newHouseLog`(
  `b_lat` string,
  `b_lng` string,
  `channel` bigint,
  `city_name` string,
  `city_x` string,
  `city_y` string,
  `content` string,
  `context` bigint,
  `context_id` string,
  `data_date` string,
  `device_id` string,
  `end_time` string,
  `flats` double,
  `login_account` double,
  `modelid` string,
  `object_id` double,
  `pic_area` double,
  `pic_chu` double,
  `pic_desc` string,
  `pic_hx_totalprice` string,
  `pic_id` double,
  `pic_prjid` double,
  `pic_prjname` string,
  `pic_sell_point` string,
  `pic_ting` string,
  `pic_type` double,
  `pic_wei` double,
  `price` double,
  `price_avg` bigint,
  `price_show` string,
  `prj_decorate` string,
  `prj_itemname` string,
  `prj_listid` bigint,
  `prj_loc` string,
  `prj_views` string,
  `projectid` bigint,
  `projecttype` bigint,
  `roomid` string,
  `room_id` double,
  `shaixuan` double,
  `start_time` string,
  `totalprice` double
  )COMMENT 'This is new house table'
STORED AS PARQUET;
